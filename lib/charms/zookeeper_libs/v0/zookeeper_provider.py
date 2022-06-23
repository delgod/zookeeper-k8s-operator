# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class we manage client database relations.

This class creates user and chroot for each application relation
and expose needed information for client connection via fields in
external relation.
"""

import re
import logging
from typing import Optional, Set, Dict, List
from charms.zookeeper_libs.v0.helpers import generate_password
from charms.zookeeper_libs.v0.zookeeper import (
    ZooKeeperConfiguration,
    ZooKeeperConnection,
)
from ops.framework import Object
from ops.model import Relation
from ops.charm import RelationBrokenEvent, RelationEvent
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
from kazoo.security import ACL, make_acl

# The unique Charmhub library identifier, never change it
LIBID = "1057f353503741a98ed79309b5be7e32"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version.
LIBPATCH = 0

logger = logging.getLogger(__name__)
REL_NAME = "database"


class ZooKeeperProvider(Object):
    """In this class we manage client database relations."""

    def __init__(self, charm) -> None:
        """Manager of ZooKeeper client relations."""
        super().__init__(charm, "client-relations")
        self.charm = charm
        self.framework.observe(self.charm.on[REL_NAME].relation_joined, self._on_relation_event)
        self.framework.observe(self.charm.on[REL_NAME].relation_changed, self._on_relation_event)
        self.framework.observe(self.charm.on[REL_NAME].relation_broken, self._on_relation_event)

    def _on_relation_event(self, event: RelationEvent) -> None:
        """Handle relation joined events.

        When relations join, change, or depart, the :class:`ZooKeeperClientRelation`
        creates or drops ZooKeeper users and sets credentials into relation
        data. As result, related charm gets credentials for accessing the
        ZooKeeper.
        """
        departed_relation_id = None
        if type(event) is RelationBrokenEvent:
            departed_relation_id = event.relation.id
            if self.charm.unit.is_leader():
                username = self._get_username_from_relation_id(departed_relation_id)
                if username in self.charm.app_data:
                    del self.charm.app_data[username]

        # We shouldn't try to create or update users if the database is not
        # initialised. We will create users as part of initialisation.
        if not self.charm.is_init_finished:
            logger.info("Deferring _on_relation_event since the cluster is not initiated.")
            event.defer()
            return

        # Only leader should run setAcl
        if self.charm.unit.is_leader():
            try:
                self.oversee_acls(departed_relation_id)
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
                logger.error("Deferring _on_relation_event since: error=%r", e)
                event.defer()
                return

            # set passwords to relation only when acls are set
            for config in self.get_configs_from_relations(departed_relation_id):
                self._set_relation(config)

        # all units should fetch passwords from relations and put into jaas config.
        configs = self.get_configs_from_relations(departed_relation_id)
        if self._are_passwords_not_set(configs):
            logger.error("Deferring _on_relation_event since: passwords are not set by the leader.")
            event.defer()
            return

        self.charm.on[self.charm.restart_manager.name].acquire_lock.emit()

    def oversee_acls(self, departed_relation_id: Optional[int] = None) -> None:
        """Oversees the users of the application.

        Function manages user relations by removing, updated, and creating
        users; and dropping databases when necessary.

        Args:
            departed_relation_id: When specified execution of functions
                makes sure to exclude the users and databases and remove
                them if necessary.

        When the function is executed in relation departed event, the departed
        relation is still in the list of all relations. Therefore, for proper
        work of the function, we need to exclude departed relation from the list.
        """
        with ZooKeeperConnection(self.charm.zookeeper_config, "leader") as zk:
            database_chroots = zk.get_paths("/")
            relation_chroots = self._get_chroots_from_relations(departed_relation_id)
            logger.debug("database_chroots: %r", database_chroots)
            logger.debug("relation_chroots: %r", relation_chroots)
            acls = self._get_acls(departed_relation_id)
            for chroot in relation_chroots - database_chroots:
                logger.info("Create chroot: %s", chroot)
                zk.create_path(chroot, acls[chroot])

            for chroot in relation_chroots.intersection(database_chroots):
                logger.info("Update %s chroot acls", chroot)
                zk.set_acls(chroot, acls[chroot])

            if not self.charm.model.config["auto-delete"]:
                return

            for chroot in database_chroots - relation_chroots:
                if not self._is_child_of(chroot, relation_chroots):
                    logger.info("Drop chroot: %s", chroot)
                    zk.drop_path(chroot)

    @staticmethod
    def _is_child_of(path: str, chroots: Set[str]) -> bool:
        for chroot in chroots:
            if path.startswith(chroot.rstrip("/") + "/"):
                return True
        return False

    def _get_config_from_relation(self, relation: Relation) -> ZooKeeperConfiguration:
        """Construct config object for future user creation."""
        return ZooKeeperConfiguration(
            chroot=self._get_chroot_from_relation(relation),
            username=self._get_username_from_relation_id(relation.id),
            password=self._get_password_from_relation_id(relation.id),
            hosts=self.charm.zookeeper_config.hosts,
            roles=self._get_roles_from_relation(relation),
        )

    def _set_relation(self, config: ZooKeeperConfiguration) -> None:
        """Save all output fields into application relation."""
        relation = self._get_relation_from_username(config.username)
        if relation is None:
            return None
        if config.password is None:
            config.password = generate_password()
        self.charm.app_data[config.username] = config.password

        relation.data[self.charm.app]["username"] = config.username
        relation.data[self.charm.app]["password"] = config.password
        relation.data[self.charm.app]["chroot"] = config.chroot
        relation.data[self.charm.app]["uris"] = config.uri
        relation.data[self.charm.app]["endpoints"] = ",".join([
            config.parse_host_port(host)
            for host in sorted(config.hosts)
        ])

    @staticmethod
    def _get_username_from_relation_id(relation_id: int) -> str:
        """Construct username."""
        return f"relation-{relation_id}"

    def get_configs_from_relations(self, departed_relation_id: Optional[int] = None) -> Set[ZooKeeperConfiguration]:
        """Return user configs for all relations except departed relation."""
        relations = self.model.relations[REL_NAME]
        result = set()
        for relation in relations:
            if relation.id == departed_relation_id:
                continue
            config = self._get_config_from_relation(relation)
            if config.chroot is None:
                continue
            result.add(config)
        return result

    def _get_chroots_from_relations(self, departed_relation_id: Optional[int] = None) -> Set[str]:
        """Return chroot names from all relations.

        Args:
            departed_relation_id: when specified return all chroot
                paths except the chroot that belong to the departing
                relation specified.
        """
        relations = self.model.relations[REL_NAME]
        chroots = set()
        for relation in relations:
            if relation.id == departed_relation_id:
                continue
            chroot = self._get_chroot_from_relation(relation)
            if chroot is not None:
                chroots.add(chroot)
        return chroots

    def _get_relation_from_username(self, username: str) -> Relation:
        """Parse relation ID from a username and return Relation object."""
        match = re.match(r"^relation-(\d+)$", username)
        # We generated username in `_get_users_from_relations`
        # func and passed it into this function later.
        # It means the username here MUST match to regex.
        assert match is not None, "No relation match"
        relation_id = int(match.group(1))
        return self.model.get_relation(REL_NAME, relation_id)

    @staticmethod
    def _get_chroot_from_relation(relation: Relation) -> Optional[str]:
        """Return chroot name from relation."""
        chroot = relation.data[relation.app].get("chroot", None)
        if chroot is None:
            chroot = relation.data[relation.app].get("database", None)
        if chroot is not None:
            if not str(chroot).startswith("/"):
                chroot = f"/{chroot}"
            return chroot
        return None

    @staticmethod
    def _get_roles_from_relation(relation: Relation) -> Set[str]:
        """Return user roles from relation if specified or return default."""
        roles = relation.data[relation.app].get("user-role", None)
        if roles is not None:
            return set(roles.split(","))
        roles = relation.data[relation.app].get("extra-user-roles", None)
        if roles is not None:
            return set(roles.split(","))
        return {"cdrwa"}

    def _get_password_from_relation_id(self, relation_id: int) -> Optional[str]:
        """Return password from relation if specified or return None."""
        username = self._get_username_from_relation_id(relation_id)
        return self.charm.app_data.get(username, None)

    def _get_acls(self, departed_relation_id: Optional[int]) -> Dict[str, List[ACL]]:
        """Return dictionary with all ACLs per each chroot from all relations."""
        configs = self.get_configs_from_relations(departed_relation_id)
        result: Dict[str, List[ACL]] = dict()
        for config in configs:
            acl = self._get_acl_from_config(config)
            if config.chroot not in result:
                result[config.chroot] = [acl]
            else:
                result[config.chroot].append(acl)
        return result

    @staticmethod
    def _get_acl_from_config(config: ZooKeeperConfiguration) -> ACL:
        return make_acl(
            "sasl",
            config.username,
            read='r' in config.roles,
            write='w' in config.roles,
            create='c' in config.roles,
            delete='d' in config.roles,
            admin='a' in config.roles,
        )

    @staticmethod
    def _are_passwords_not_set(configs: Set[ZooKeeperConfiguration]) -> bool:
        return any(
            config.chroot is not None and config.password is None
            for config in configs
        )
