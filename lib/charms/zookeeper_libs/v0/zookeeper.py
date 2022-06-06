"""Code for interactions with ZooKeeper."""
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import json
from collections import defaultdict
from urllib.request import urlopen
from urllib.error import URLError
from dataclasses import dataclass
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from typing import Set, Dict, Optional

# The unique Charmhub library identifier, never change it
LIBID = "1057f353503741a98ed79309b5be7f30"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version.
LIBPATCH = 0

# path to store mongodb ketFile
logger = logging.getLogger(__name__)


@dataclass
class ZooKeeperConfiguration:
    """
    Class for MongoDB configuration:
    — username: username.
    — password: password.
    — hosts: full list of hosts to connect to, needed for the URI.
    """

    username: str
    password: str
    hosts: Set[str]

    @property
    def uri(self) -> str:
        """Return URI concatenated from hosts."""
        uri = ""
        for host in self.hosts:
            uri += self.parse_host_port(host) + ","
        return uri[:-1]

    @property
    def quorum_leader(self) -> Optional[str]:
        """Connect to all members and find the leader."""
        votes = defaultdict(int)
        for member in self.hosts:
            host = self.parse_host_port(member).split(":")[0]
            try:
                with urlopen(f"http://{host}:8080/commands/leader") as response:
                    body = response.read()
                    answer = json.loads(body)
                    if "leader_ip" in answer:
                        votes[answer["leader_ip"]] += 1
            except (URLError, json.JSONDecodeError):
                pass
        for leader in votes:
            if votes[leader] > len(self.hosts) / 2:
                return leader
        return None

    @staticmethod
    def parse_host_port(member: Optional[str]) -> Optional[str]:
        """Parse hostport from ZooKeeper config entry."""
        if member and "=" in member and ":" in member:
            return member.split("=")[1].split(":")[0] + ":" + member.split(":")[-1]
        return member


class NotReadyError(KazooException):
    """Raised when not all members healthy or finished initial sync."""


class ZooKeeperConnection:
    """
    In this class we create connection object to ZooKeeper.

    Connection is automatically closed when object destroyed.
    Automatic close allows to have more clean code.

    Note that connection when used may lead to the multiple different kazoo exceptions.
    Please carefully check list of possible exceptions in doc string of each method.
    It is suggested that the following pattern be adopted when using ZooKeeperConnection:

    try:
        with ZooKeeperConnection(self._zookeeper_config) as zk:
            zk.<some operation from this class>
    except (KazooTimeoutError, NoNodeError, ZookeeperError) as e:
        <error handling as needed>
    """

    def __init__(self, config: ZooKeeperConfiguration, uri=None):
        """A ZooKeeper client interface.

        Raises:
            KazooTimeoutError
        """
        self.config = config

        if uri == "leader":
            uri = config.quorum_leader
            logger.debug("Connection to leader: %s", uri)

        if uri is None:
            uri = config.uri
        elif "=" in uri:
            uri = self.config.parse_host_port(uri)

        # auth_str = "%s:%s" % (config.username, config.password)
        self.client = KazooClient(
                hosts=uri,
                read_only=False,
                sasl_options={
                    'mechanism': 'DIGEST-MD5',
                    'username': config.username,
                    'password': config.password,
                },
        )
        self.client.start()

    def __enter__(self):
        return self

    def __exit__(self, object_type, value, traceback):
        self.client.stop()
        self.client = None
        self.config = None

    @property
    def is_ready(self) -> bool:
        """Is the MongoDB server ready for services requests.

        Returns:
            True if services is ready False otherwise. Retries over a period of 60 seconds times to
            allow server time to start up.

        Raises:
            ConnectionLoss
        """
        if self.client.connected:
            mntr = self._run_command("mntr")
            return "broadcast" in mntr["zk_peer_state"]
        return False

    def get_members(self) -> (Set[str], int):
        """Get a members configured inside ZooKeeper.

        Returns:
            A set of the members as reported by ZooKeeper.
            A configuration version applied.

        Raises:
            NoNodeError, ZookeeperError
        """
        data, _ = self.client.get("/zookeeper/config")
        lines = data.decode("utf-8").split("\n")
        version = int(lines[-1].split("=")[1], base=16)
        return set(lines[:-1]), version

    def add_member(self, member: str) -> None:
        """Add a new member to config inside ZooKeeper.

        Raises:
            NoNodeError, ZookeeperError — from `get_members`.
            ConnectionLoss — from `_is_any_sync`.
            UnimplementedError, NewConfigNoQuorumError, ReconfigInProcessError,
            BadVersionError, BadArgumentsError, ZookeeperError — from `reconfig`.
        """
        _, version = self.get_members()

        # When we add a new member, ZooKeeper transfer data from existing member to new.
        # Such operation reduce performance of the cluster. To avoid huge performance
        # degradation, before adding new members, it is needed to check that all other
        # members finished sync.
        if self._is_any_sync():
            # it can take a while, we should defer
            raise NotReadyError

        data, stat = self.client.reconfig(joining=member, leaving=None, new_members=None, from_config=version)
        logger.debug("zookeeper reconfig response: %r, %r", stat, data)

    def remove_member(self, member: str) -> None:
        """Remove member from the config inside ZooKeeper.

        Raises:
            NoNodeError, ZookeeperError — from `get_members`.
            ConnectionLoss — from `_is_any_sync`.
            UnimplementedError, NewConfigNoQuorumError, ReconfigInProcessError,
            BadVersionError, BadArgumentsError, ZookeeperError — from `reconfig`.
        """
        _, version = self.get_members()
        member_id = member.split(".")[1].split("=")[0]

        # When we remove member, to avoid issues when majority members is removed, we need to
        # remove next member only when MongoDB forget the previous removed member.
        if self._is_any_sync():
            # removing from replicaset is fast operation, lets @retry(3 times with a 5sec timeout) before giving up.
            raise NotReadyError

        data, stat = self.client.reconfig(joining=None, leaving=member_id, new_members=None, from_config=version)
        logger.debug("zookeeper reconfig response: %r, %r", stat, data)

    def _is_any_sync(self) -> bool:
        """Returns true if any members are syncing data.

        Checks if any members are syncing data. Note it is recommended to run
        only one sync in the cluster to avoid forming quorum by new members.

        Raises:
            ConnectionLoss
        """
        state = self._run_command("mntr")
        if state["zk_peer_state"] == "leading - broadcast" and state["zk_pending_syncs"] == "0":
            return False
        return True

    def _run_command(self, command: str) -> Dict:
        """Run and parse any ZooKeeper command.

        Raises:
            ConnectionLoss
        """
        zk_response = self.client.command(command.encode())
        return dict(
            (element.split("\t", 2)[0], element.split("\t", 2)[1])
            for element in zk_response.split("\n")
            if "\t" in element
        )
