"""Simple functions, which can be used in both K8s and VM charms."""
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import string
import secrets
import logging

from typing import Set
from charms.zookeeper_libs.v0.zookeeper import ZooKeeperConfiguration

# The unique Charmhub library identifier, never change it
LIBID = "1057f353503741a98ed79309b5be7f31"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version.
LIBPATCH = 0

CONFIG_PATH = "/conf/zookeeper.cfg"
DYN_CONFIG_PATH = "/conf/zookeeper-dynamic.cfg"
AUTH_CONFIG_PATH = "/conf/zookeeper-jaas.cfg"
DATA_DIR = "/var/lib/zookeeper"
LOGS_DIR = "/var/log/zookeeper"
UNIX_USER = "zookeeper"
UNIX_GROUP = "zookeeper"

logger = logging.getLogger(__name__)


def get_zookeeper_cmd() -> str:
    """Construct the ZooKeeper startup command line.

    Returns:
        A string representing the command used to start MongoDB.
    """
    cmd = [
        "/usr/local/openjdk-11/bin/java",
        "-server -Djava.awt.headless=true",

        "-Xmx512M -Xms512M",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:OnOutOfMemoryError='kill -9 %p'",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=20",
        "-XX:InitiatingHeapOccupancyPercent=35",
        "-XX:+ExplicitGCInvokesConcurrent",
        "-XX:MaxInlineLevel=15",
        f"'-Xlog:gc*:file={LOGS_DIR}/zookeeper-gc.log:time,tags:filecount=10,filesize=100M'",

        "-Dcom.sun.management.jmxremote",
        "-Dcom.sun.management.jmxremote.local.only=true",

        f"-Dzookeeper.log.dir={LOGS_DIR}",
        "-Dzookeeper.log.file=zookeeper.log",

        f"-Djava.security.auth.login.config={AUTH_CONFIG_PATH}",
        "-Dzookeeper.requireClientAuthScheme=sasl",
        "-Dzookeeper.superUser=super",

        "-cp './lib/*:/conf:'",
        "org.apache.zookeeper.server.quorum.QuorumPeerMain",
        CONFIG_PATH,
    ]
    return " ".join(cmd)


def generate_password() -> str:
    """Generate a random password string.

    Returns:
       A random password string.
    """
    choices = string.ascii_letters + string.digits
    return "".join([secrets.choice(choices) for _ in range(32)])


def get_main_config() -> str:
    """Generate content of the main ZooKeeper config file"""
    return f"""
        standaloneEnabled=false
        dataDir={DATA_DIR}
        4lw.commands.whitelist=mntr
        tickTime=1000
        initLimit=30
        quorumListenOnAllIPs=true
        syncLimit=3
        reconfigEnabled=true
        dynamicConfigFile={DYN_CONFIG_PATH}
        DigestAuthenticationProvider.digestAlg=SHA3-256

        quorum.auth.enableSasl=true
        quorum.auth.learnerRequireSasl=true
        quorum.auth.serverRequireSasl=true
        authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
        audit.enable=true
    """


def get_auth_config(sync_password, super_password: str, configs: Set[ZooKeeperConfiguration]) -> str:
    """Generate content of the auth ZooKeeper config file"""
    users = "\n".join([
        f"user_{config.username}=\"{config.password}\""
        for config in configs
        if config.password is not None
    ])
    result = f"""
        QuorumServer {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            user_sync="{sync_password}";
        }};

        QuorumLearner {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            username="sync"
            password="{sync_password}";
        }};
        
        Server {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            {users}
            user_super="{super_password}";
        }};
    """
    return result
