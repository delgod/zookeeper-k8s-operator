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
from kazoo.exceptions import (
    BadArgumentsError,
    BadVersionError,
    ConnectionLoss,
    NewConfigNoQuorumError,
    NoNodeError,
    ReconfigInProcessError,
    UnimplementedError,
    ZookeeperError,
)
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
        self.framework.observe(self.on.zookeeper_pebble_ready, self._on_zookeeper_pebble_ready)
        self.framework.observe(self.on.leader_elected, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_changed, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_departed, self._reconfigure)

    def _on_zookeeper_pebble_ready(self, event) -> None:
        """Configure pebble layer specification."""
        # Wait for the password used for synchronisation between members.
        # It should be generated once and be the same on all members.
        if "sync_password" not in self.app_data:
            logger.error("Super password is not ready yet.")
            event.defer()
            return

        if not self._is_my_turn:
            logger.error("The first time ZooKeeper members should start sequentially.")
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

        The same sync and super passwords on all members needed.
        It means, it is needed to generate them once and share between members.
        NB: only leader should execute this function.
        """
        if "super_password" not in self.app_data:
            self.app_data["super_password"] = generate_password()
        if "sync_password" not in self.app_data:
            self.app_data["sync_password"] = generate_password()

    def _reconfigure(self, event) -> None:
        """Reconfigure members.

        The amount members is updated.
        """
        if not self.unit.is_leader():
            return

        # We generate passwords here, because “on_leader_elected“
        # runs earlier than _on_zookeeper_pebble_ready event.
        self._generate_passwords()

        try:
            with ZooKeeperConnection(self._zookeeper_config, "leader") as zk:
                zookeeper_members, _ = zk.get_members()

                # compare (sets) members from /zookeeper/config and juju peer list
                # to avoid the unnecessary reconfiguration.
                if zookeeper_members == self._zookeeper_config.hosts:
                    return

                # remove members first, it is faster (no startup/initial sync delay)
                logger.info("Reconfigure zookeeper members")
                for member in zookeeper_members - self._zookeeper_config.hosts:
                    logger.debug("Removing %s", member)
                    zk.remove_member(member)
                    self.app_data[member.split("=")[0]] = "removed"

                # It is impossible to add a member with ID lower than the biggest ID between current members.
                # Use “sorted” function to avoid issues.
                for member in sorted(self._zookeeper_config.hosts - zookeeper_members):
                    with ZooKeeperConnection(self._zookeeper_config, member) as direct_conn:
                        if not direct_conn.is_ready:
                            logger.info("Deferring reconfigure: %s not ready", member)
                            event.defer()
                    logger.debug("Adding %s", member)
                    zk.add_member(member)
                    self.app_data[member.split("=")[0]] = "started"
        except (
            KazooTimeoutError,
            NoNodeError,
            ZookeeperError,
            ConnectionLoss,
            UnimplementedError,
            NewConfigNoQuorumError,
            ReconfigInProcessError,
            BadVersionError,
            BadArgumentsError,
        ) as e:
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
            get_main_config(),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )
        container.push(
            AUTH_CONFIG_PATH,
            get_auth_config(
                self.app_data.get("sync_password"), self.app_data.get("super_password")
            ),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )

        members = self._get_peer_units
        # On just created the new/fresh cluster, new units should join as the “observer” before initial sync.
        # Especially case when unit/0 adds unit/1 it is not clear for ZooKeeper, which member has the latest data.
        if not self._is_init_finished:
            members = self._get_init_units

        container.push(
            DYN_CONFIG_PATH,
            "\n".join(members),
            make_dirs=True,
            permissions=0o400,
            user="zookeeper",
            group="zookeeper",
        )

    @property
    def _is_my_turn(self) -> bool:
        """Function guarantee start units started in order unit/0->unit/1->unit/2.

        Unfortunately, if ZooKeeper Server-Server auth configured, during the first
        cluster start (and horizontal scaling) it is needed to add members in order.
        """
        myid = self._get_unit_id_by_unit(self.unit.name)
        for unit_id in range(1, myid):
            if self._get_unit_status(unit_id) != "started":
                return False
        return True

    @property
    def _is_init_finished(self) -> bool:
        """Function detect the unit restart.

        During initial start we should add members one by one, but during a single
        unit restart or failure, we should put all members into the config.
        """
        myid = self._get_unit_id_by_unit(self.unit.name)
        for unit in list(self.model.get_relation(PEER).units):
            unit_id = self._get_unit_id_by_unit(unit.name)
            if myid < unit_id:
                continue
            logger.debug("%s status: %s", unit.name, unit.status)

        myid = self._get_unit_id_by_unit(self.unit.name)
        for unit_id in range(1, myid + 1):
            if self._get_unit_status(unit_id) != "started":
                return False
        return True

    def _get_unit_status(self, unit_id: int) -> str:
        server_str = f"server.{unit_id}"
        if server_str not in self.app_data:
            return "unknown"
        return self.app_data[server_str]

    @property
    def _get_peer_units(self) -> List:
        """Prepare ZooKeeper config with all cluster members."""
        result = []
        for unit in [self.unit] + list(self.model.get_relation(PEER).units):
            result.append(self._get_server_str(unit.name))
        return result

    @property
    def _get_init_units(self) -> List:
        """Prepare *initial* ZooKeeper config with some members.

        On just created the new/fresh cluster, new units should join as the “observer” before initial sync.
        Especially case when unit/0 adds unit/1 it is not clear for ZooKeeper, which member has the latest data.
        """
        quorum_leader = self.unit.name.split("/")[0] + "/0"
        result = [self._get_server_str(quorum_leader)]
        if self.unit.name != quorum_leader:
            result.append(self._get_server_str(self.unit.name, "observer"))
        return result

    def _get_server_str(self, unit_name: str, role="participant") -> str:
        """Prepare member line for ZooKeeper config."""
        unit_id = self._get_unit_id_by_unit(unit_name)
        unit_hostname = self._get_hostname_by_unit(unit_name)
        return f"server.{unit_id}={unit_hostname}:2888:3888:{role};0.0.0.0:2181"

    @staticmethod
    def _get_unit_id_by_unit(unit_name: str) -> int:
        """Cut number from the unit name."""
        return int(unit_name.split("/")[1])

    def _get_hostname_by_unit(self, unit_name: str) -> str:
        """Create a DNS name for a unit.

        Args:
            unit_name: the juju unit name, e.g. "mongodb/1".

        Returns:
            A string representing the hostname of the unit.
        """
        unit_id = self._get_unit_id_by_unit(unit_name)
        return f"{self.app.name}-{unit_id}.{self.app.name}-endpoints"


if __name__ == "__main__":
    main(ZooKeeperCharm)
