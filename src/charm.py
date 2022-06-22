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
from typing import Dict, Set

from charms.rolling_ops.v0.rollingops import RollingOpsManager
from charms.zookeeper_libs.v0.helpers import (
    AUTH_CONFIG_PATH,
    CONFIG_PATH,
    DATA_DIR,
    DYN_CONFIG_PATH,
    LOGS_DIR,
    UNIX_GROUP,
    UNIX_USER,
    generate_password,
    get_auth_config,
    get_main_config,
    get_zookeeper_cmd,
)
from charms.zookeeper_libs.v0.zookeeper import (
    NotLeaderError,
    NotReadyError,
    ZooKeeperConfiguration,
    ZooKeeperConnection,
)
from charms.zookeeper_libs.v0.zookeeper_provider import ZooKeeperProvider
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
from ops.charm import CharmBase, HookEvent, PebbleReadyEvent
from ops.main import main
from ops.model import ActiveStatus
from ops.pebble import Layer, PathError, ProtocolError
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)
PEER = "database-peers"


class ZooKeeperCharm(CharmBase):
    """A Juju Charm to deploy ZooKeeper on Kubernetes."""

    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.framework.observe(self.on.zookeeper_pebble_ready, self._on_zookeeper_pebble_ready)
        self.framework.observe(self.on.leader_elected, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_changed, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_departed, self._reconfigure)
        self.client_relations = ZooKeeperProvider(self)
        self.restart_manager = RollingOpsManager(
            charm=self, relation="restart", callback=self._put_auth_configs
        )

    def _on_zookeeper_pebble_ready(self, event: PebbleReadyEvent) -> None:
        """Configure pebble layer specification."""
        # Wait for the password used for synchronisation between members.
        # It should be generated once and be the same on all members.
        if "sync_password" not in self.app_data:
            logger.error("Super password is not ready yet.")
            event.defer()
            return

        # Prepare configs
        self._put_general_configs(event)
        self._put_auth_configs(event)

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

    # flake8: noqa: C901
    def _reconfigure(self, event: HookEvent) -> None:
        """Reconfigure members.

        The amount members is updated.
        """
        if not self.unit.is_leader():
            return

        # During initial startup we need to provide passwords as soon as possible.
        # Unfortunately password become available for other units only when the function finished.
        if "sync_password" not in self.app_data:
            self._generate_passwords()
            return

        try:
            with ZooKeeperConnection(self.zookeeper_config, "leader") as zk:
                zookeeper_members, _ = zk.get_members()

                # remove members first, it is faster (no startup/initial sync delay)
                for member in zookeeper_members - self.zookeeper_config.hosts:
                    logger.debug("Removing %s", member)
                    zk.remove_member(member)
                    # remove member from peer data to make `is_init_finished` works later.
                    self.app_data[member.split("=")[0]] = "removed"

                # It is impossible to add a member with ID lower than the biggest ID
                # between current members. Use “sorted” function to avoid issues.
                for member in sorted(self.zookeeper_config.hosts - zookeeper_members):
                    logger.debug("Adding %s", member)
                    if not self._is_my_turn(member):
                        logger.info("Deferring adding: members should start sequentially.")
                        event.defer()
                        return
                    with ZooKeeperConnection(self.zookeeper_config, member) as direct_conn:
                        if not direct_conn.is_ready:
                            logger.info("Deferring reconfigure: %s not ready", member)
                            event.defer()
                            return
                    zk.add_member(member)
                    self.app_data[member.split("=")[0]] = "started"
        except NotLeaderError:
            logger.info("Deferring reconfigure: connection established to non-leader unit.")
        except NotReadyError:
            logger.info("Deferring reconfigure: sync in progress.")
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
                    "user": UNIX_USER,
                    "group": UNIX_GROUP,
                }
            },
        }
        return Layer(layer_config)

    @property
    def app_data(self) -> Dict[str, str]:
        """Peer relation data object."""
        return self.model.get_relation(PEER).data[self.app]

    @property
    def zookeeper_config(self) -> ZooKeeperConfiguration:
        """Create a configuration object with settings.

        Needed for correct handling interactions with MongoDB.

        Returns:
            A MongoDBConfiguration object
        """
        return ZooKeeperConfiguration(
            username="super",
            password=self.app_data.get("super_password"),
            hosts=self._get_peer_units,
            chroot="/",
            roles={"cdrwa"},
        )

    def _put_auth_configs(self, event: HookEvent) -> None:
        """Upload the Auth config to a workload container."""
        container = self.unit.get_container("zookeeper")
        if not container.can_connect():
            logger.debug("zookeeper container is not ready yet.")
            event.defer()
            return

        new_content = get_auth_config(
            self.app_data.get("sync_password"),
            self.app_data.get("super_password"),
            self.client_relations.get_configs_from_relations(),
        )
        old_content = None
        try:
            old_content = container.pull(AUTH_CONFIG_PATH).read()
        except (PathError, ProtocolError):
            pass

        if new_content == old_content:
            logger.debug("Restart not needed")
            return

        try:
            container.push(
                AUTH_CONFIG_PATH,
                new_content,
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
        except (PathError, ProtocolError) as e:
            logger.error("Cannot put configs: %r", e)
            event.defer()
            return

        if not container.get_services("zookeeper"):
            return

        # Make sure ZooKeeper is up and joined quorum
        leader = self.zookeeper_config.fetch_leader(self._get_hostname_by_unit(self.unit.name))
        if leader is None:
            logger.debug("Deferring restart: ZooKeeper not joined quorum.")
            event.defer()
            return

        # Apply auth changes
        logger.debug("Restarting ZooKeeper.")
        container.restart("zookeeper")

        # Let's try to wait successful restart
        try:
            for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
                with attempt:
                    leader = self.zookeeper_config.fetch_leader(
                        self._get_hostname_by_unit(self.app.name)
                    )
                    if leader is None:
                        raise Exception("Retrying: ZooKeeper not joined quorum.")
        except RetryError:
            logger.warning("ZooKeeper haven't joined quorum.")

    def _put_general_configs(self, event: HookEvent) -> None:
        """Upload the configs to a workload container."""
        container = self.unit.get_container("zookeeper")
        if not container.can_connect():
            logger.debug("zookeeper container is not ready yet.")
            event.defer()
            return
        try:
            if not container.exists(DATA_DIR):
                container.make_dir(
                    DATA_DIR,
                    make_parents=True,
                    permissions=0o755,
                    user=UNIX_USER,
                    group=UNIX_GROUP,
                )
            if not container.exists(LOGS_DIR):
                container.make_dir(
                    LOGS_DIR,
                    make_parents=True,
                    permissions=0o755,
                    user=UNIX_USER,
                    group=UNIX_GROUP,
                )
            myid = self._get_unit_id_by_unit(self.unit.name)
            container.push(
                DATA_DIR + "/myid",
                str(myid),
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
            container.push(
                CONFIG_PATH,
                get_main_config(),
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
            members = self._get_peer_units
            # On just created the new/fresh cluster, new units should join as the “observer”
            # before initial sync. Especially case when unit/0 adds unit/1 it is not clear
            # for ZooKeeper, which member has the latest data.
            if not self.is_init_finished:
                members = self._get_init_units
            container.push(
                DYN_CONFIG_PATH,
                "\n".join(members),
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
        except (PathError, ProtocolError) as e:
            logger.error("Cannot put configs: %r", e)
            event.defer()
            return

    def _is_my_turn(self, member) -> bool:
        """Function guarantee start units started in order unit/0->unit/1->unit/2.

        Unfortunately, if ZooKeeper Server-Server auth configured, during the first
        cluster start (and horizontal scaling) it is needed to add members in order.
        """
        member_id = self._get_unit_id_by_server_str(member)
        for unit_id in range(1, member_id):
            if self._get_unit_status(unit_id) != "started":
                return False
        return True

    @property
    def is_init_finished(self) -> bool:
        """Function detect the unit restart.

        During initial start we should add members one by one, but during a single
        unit restart or failure, we should put all members into the config.
        """
        for unit_id in range(1, self.app.planned_units()):
            if self._get_unit_status(unit_id) != "started":
                return False
        return True

    def _get_unit_status(self, unit_id: int) -> str:
        server_str = f"server.{unit_id}"
        if server_str not in self.app_data:
            return "unknown"
        return self.app_data[server_str]

    @property
    def _get_peer_units(self) -> Set[str]:
        """Prepare ZooKeeper config with all cluster members."""
        result = []
        for unit in [self.unit] + list(self.model.get_relation(PEER).units):
            result.append(self._get_server_str(unit.name))
        return set(result)

    @property
    def _get_init_units(self) -> Set[str]:
        """Prepare *initial* ZooKeeper config with some members.

        On just created the new/fresh cluster, new units should join as the “observer”
        before initial sync. Especially case when unit/0 adds unit/1 it is not clear
        for ZooKeeper, which member has the latest data.
        """
        quorum_leader = self.unit.name.split("/")[0] + "/0"
        result = [self._get_server_str(quorum_leader)]
        if self.unit.name != quorum_leader:
            result.append(self._get_server_str(self.unit.name, "observer"))
        return set(result)

    def _get_server_str(self, unit_name: str, role="participant") -> str:
        """Prepare member line for ZooKeeper config."""
        unit_id = self._get_unit_id_by_unit(unit_name)
        unit_hostname = self._get_hostname_by_unit(unit_name)
        return f"server.{unit_id}={unit_hostname}:2888:3888:{role};0.0.0.0:2181"

    @staticmethod
    def _get_unit_id_by_unit(unit_name: str) -> int:
        """Cut number from the unit name."""
        return int(unit_name.split("/")[1])

    @staticmethod
    def _get_unit_id_by_server_str(server_str: str) -> int:
        """Cut number from the unit name."""
        return int(server_str.split(".")[1].split("=")[0])

    def _get_hostname_by_unit(self, unit_name: str) -> str:
        """Create a DNS name for a unit.

        Args:
            unit_name: the juju unit name, e.g. "zookeeper-k8s/1".

        Returns:
            A string representing the hostname of the unit.
        """
        unit_id = self._get_unit_id_by_unit(unit_name)
        return f"{self.app.name}-{unit_id}.{self.app.name}-endpoints"


if __name__ == "__main__":
    main(ZooKeeperCharm)
