"""Simple functions, which can be used in both K8s and VM charms."""
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import string
import secrets
import logging

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

logger = logging.getLogger(__name__)


def get_zookeeper_cmd() -> str:
    """Construct the ZooKeeper startup command line.

    Returns:
        A string representing the command used to start MongoDB.
    """
    cmd = [
        "/usr/local/openjdk-11/bin/java",
        "-cp '/apache-zookeeper-3.8.0-bin/lib/*:/conf:'",
        "-Xmx1000m",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:OnOutOfMemoryError='kill -9 %p'",
        "-Dcom.sun.management.jmxremote",
        "-Dcom.sun.management.jmxremote.local.only=true",
        "-Dzookeeper.log.dir=/var/log",
        "-Dzookeeper.log.file=zookeeper.log",
        "-Dzookeeper.requireClientAuthScheme=sasl",
        "-Dzookeeper.superUser.1=super",
        f"-Djava.security.auth.login.config={AUTH_CONFIG_PATH}",
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
        4lw.commands.whitelist=*
        tickTime=1000
        initLimit=30
        quorumListenOnAllIPs=true
        syncLimit=3
        reconfigEnabled=true
        dynamicConfigFile={DYN_CONFIG_PATH}

        quorum.auth.enableSasl=true
        quorum.auth.learnerRequireSasl=true
        quorum.auth.serverRequireSasl=true
        quorum.cnxn.threads.size=20
    """


def get_auth_config(sync_password, super_password: str) -> str:
    """Generate content of the auth ZooKeeper config file"""
    return f"""
        Server {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            user_super="{super_password}";
        }};

        Client {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            username="super"
            password="{super_password}";
        }};

        QuorumServer {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            user_cluster="{sync_password}";
        }};

        QuorumLearner {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            username="cluster"
            password="{sync_password}";
        }};
    """
