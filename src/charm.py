#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm for ZooKeeper on Kubernetes.

Run the developer's favourite distributed configuration service — ZooKeeper!
Charm for ZooKeeper is a fully supported, automated solution from Canonical for
running production-grade ZooKeeper on Kubernetes. It offers simple, secure and
highly available setup with automatic recovery on fail-over. The solution
includes scaling and other capabilities.
"""

import logging
from typing import Dict, List

from charms.zookeeper_libs.v0.helpers import (
    AUTH_CONFIG_PATH,
    CONFIG_PATH,
    DATA_DIR,
    DYN_CONFIG_PATH,
    generate_password,
    get_auth_config,
    get_main_config,
    get_zookeeper_cmd,
)
from charms.zookeeper_libs.v0.zookeeper import (
    ZooKeeperConfiguration,
    ZooKeeperConnection,
)
from kazoo.exceptions import NoNodeError, ZookeeperError
from kazoo.handlers.threading import KazooTimeoutError
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus, Container
from ops.pebble import Layer, PathError, ProtocolError

logger = logging.getLogger(__name__)
PEER = "database-peers"


class ZooKeeperCharm(CharmBase):
    """A Juju Charm to deploy ZooKeeper on Kubernetes."""

    def __init__(self, *args):
        super().__init__(*args)
        self._peer_relation_joined = False

        self.framework.observe(self.on.zookeeper_pebble_ready, self._on_zookeeper_pebble_ready)
        self.framework.observe(self.on[PEER].relation_joined, self._on_peer_relation_joined)

        self.framework.observe(self.on.leader_elected, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_changed, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_departed, self._reconfigure)

    def _on_peer_relation_joined(self, event) -> None:
        self._peer_relation_joined = True

    def _on_zookeeper_pebble_ready(self, event) -> None:
        """Configure pebble layer specification."""
        # Wait for super password, it is needed for configs
        if "super_password" not in self.app_data:
            logger.error("Super password is not ready yet.")
            event.defer()
            return

        # Wait for peer relation joined event
        if self._peer_relation_joined:
            logger.error("Peer relation is not joined yet.")
            event.defer()
            return

        # Prepare configs
        container = self.unit.get_container("zookeeper")
        if not container.can_connect():
            logger.debug("zookeeper container is not ready yet.")
            event.defer()
            return
        try:
            self._put_general_configs(container)
        except (PathError, ProtocolError) as e:
            logger.error("Cannot put configs: %r", e)
            event.defer()
            return

        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
        # Add initial Pebble config layer using the Pebble API
        container.add_layer("zookeeper", self._zookeeper_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()
        # TODO: rework status
        self.unit.status = ActiveStatus()

    def _generate_passwords(self) -> None:
        """Generate passwords and put them into peer relation.

        The same super password on all members needed.
        It means, it is needed to generate them once and share between members.
        NB: only leader should execute this function.
        """
        if "super_password" not in self.app_data:
            self.app_data["super_password"] = generate_password()

    def _reconfigure(self, event) -> None:
        """Reconfigure members.

        The amount members is updated.
        """
        # update members in config file
        # container = self.unit.get_container("zookeeper")
        # if not container.can_connect():
        #     logger.debug("zookeeper container is not ready yet.")
        #     event.defer()
        #     return
        # self._put_dynamic_configs(container)

        if not self.unit.is_leader():
            return
        self.app_data["juju_leader"] = self.unit.name
        self._generate_passwords()

        try:
            with ZooKeeperConnection(self._zookeeper_config, "leader") as zk:
                zookeeper_members, _ = zk.get_members()

                logger.debug("znode_members: %r", zookeeper_members)
                logger.debug("peer_members: %r", self._zookeeper_config.hosts)

                # compare set of zookeeper members and juju hosts
                # to avoid the unnecessary reconfiguration.
                if zookeeper_members == self._zookeeper_config.hosts:
                    return

                # remove members first, it is faster
                logger.info("Reconfigure zookeeper members")
                for member in zookeeper_members - self._zookeeper_config.hosts:
                    logger.debug("Removing %s", member)
                    zk.remove_member(member)
                for member in self._zookeeper_config.hosts - zookeeper_members:
                    with ZooKeeperConnection(self._zookeeper_config, member) as direct_conn:
                        if not direct_conn.is_ready:
                            logger.info("Deferring reconfigure: %s not ready", member)
                            event.defer()
                    logger.debug("Adding %s", member)
                    zk.add_member(member)
        except (KazooTimeoutError, NoNodeError, ZookeeperError) as e:
            logger.info("Deferring reconfigure: error=%r", e)
            event.defer()

    @property
    def _zookeeper_layer(self) -> Layer:
        """Returns a Pebble configuration layer for ZooKeeper."""
        layer_config = {
            "summary": "zookeeper layer",
            "description": "Pebble config layer for zookeeper",
            "services": {
                "zookeeper": {
                    "override": "replace",
                    "summary": "zookeeper",
                    "command": get_zookeeper_cmd(),
                    "startup": "enabled",
                    "user": "zookeeper",
                    "group": "zookeeper",
                }
            },
        }
        return Layer(layer_config)

    @property
    def app_data(self) -> Dict:
        """Peer relation data object."""
        return self.model.get_relation(PEER).data[self.app]

    @property
    def _zookeeper_config(self) -> ZooKeeperConfiguration:
        """Create a configuration object with settings.

        Needed for correct handling interactions with MongoDB.

        Returns:
            A MongoDBConfiguration object
        """
        return ZooKeeperConfiguration(
            username="super",
            password=self.app_data.get("super_password"),
            hosts=set(self._get_peer_units),
        )

    def _put_general_configs(self, container: Container) -> None:
        """Upload the configs to a workload container."""
        myid = self._get_unit_id_by_unit(self.unit.name)
        container.push(
            DATA_DIR + "/myid",
            str(myid),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )
        container.push(
            CONFIG_PATH,
            get_main_config(myid),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )
        container.push(
            AUTH_CONFIG_PATH,
            get_auth_config(self.app_data.get("super_password")),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )
        members = self._get_init_units
        if self._zookeeper_config.leader is not None:
            members = self._get_peer_units
        container.push(
            DYN_CONFIG_PATH,
            "\n".join(members),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )

    def _put_dynamic_configs(self, container: Container) -> None:
        """Upload the configs to a workload container."""
        container.push(
            DYN_CONFIG_PATH,
            "\n".join(self._get_peer_units),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )

    @property
    def _get_init_units(self) -> List:
        result = [self._get_server_str(self.app_data["juju_leader"])]
        if self.unit.name != self.app_data["juju_leader"]:
            result.append(self._get_server_str(self.unit.name, "observer"))
        return result

    def _get_server_str(self, unit_name: str, role="participant") -> str:
        unit_id = self._get_unit_id_by_unit(unit_name)
        unit_hostname = self._get_hostname_by_unit(unit_name)
        return f"server.{unit_id}={unit_hostname}:2888:3888:{role};0.0.0.0:2181"

    @property
    def _get_peer_units(self) -> List:
        result = []
        for unit in [self.unit] + list(self.model.get_relation(PEER).units):
            result.append(self._get_server_str(unit.name))
        return result

    @staticmethod
    def _get_unit_id_by_unit(unit_name: str) -> int:
        return int(unit_name.split("/")[1]) + 10

    def _get_hostname_by_unit(self, unit_name: str) -> str:
        """Create a DNS name for a unit.

        Args:
            unit_name: the juju unit name, e.g. "mongodb/1".

        Returns:
            A string representing the hostname of the unit.
        """
        unit_id = unit_name.split("/")[1]
        return f"{self.app.name}-{unit_id}.{self.app.name}-endpoints"


if __name__ == "__main__":
    main(ZooKeeperCharm)
