"""Copyright 2019 - 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import time
import dse.cluster
from dse.cqlengine import connection
from dse.cqlengine.management import (
    create_keyspace_network_topology,
    drop_keyspace,
    sync_table,
    create_keyspace_simple,
)
from dse.policies import (
    RoundRobinPolicy,
    DCAwareRoundRobinPolicy,
    TokenAwarePolicy,
    WhiteListRoundRobinPolicy,
)


from radon import cfg
from radon.log import logger
from radon.models.group import Group
from radon.models.user import User
from radon.models.tree_entry import TreeEntry
from radon.models.collection import Collection
from radon.models.data_object import DataObject
from radon.models.resource import Resource
from radon.models.search import SearchIndex
from radon.models.id_search import IDSearch
from radon.models.acl import Ace
from radon.models.notification import Notification



def connect():
    """Connect to a Cassandra cluster"""
    num_retries = 5
    retry_timeout = 2

    keyspace = cfg.dse_keyspace
    hosts = cfg.dse_host
    strategy = (cfg.dse_strategy,)

    for _ in range(num_retries):
        try:
            logger.info(
                'Connecting to Cassandra keyspace "{2}" '
                'on "{0}" with strategy "{1}"'.format(hosts, strategy, keyspace)
            )
            connection.setup(
                hosts,
                keyspace,
                protocol_version=3
            )

            return True
        except dse.cluster.NoHostAvailable:
            logger.warning(
                "Unable to connect to Cassandra on {0}. Retrying in {1} seconds...".format(
                    hosts,
                    retry_timeout
                )
            )
            time.sleep(retry_timeout)
    return False

def initialise():
    """Initialise Cassandra connection"""
    if not connect():
        return False

    strategy = (cfg.dse_strategy,)
    repl_factor = cfg.dse_repl_factor
    dc_replication_map = cfg.dse_dc_replication_map
    keyspace = cfg.dse_keyspace
    
    cluster = connection.get_cluster()
    if keyspace in cluster.metadata.keyspaces:
        # If the keyspace already exists we do not create it. Should we raise
        # an error
        return True

    if cfg.dse_strategy is "NetworkTopologyStrategy":
        create_keyspace_network_topology(keyspace, dc_replication_map, True)
    else:
        create_keyspace_simple(keyspace, repl_factor, True)

    return True

def sync():
    """Create tables for the different models"""
    tables = (
        DataObject,
        Group,
        IDSearch,
        Notification,
        SearchIndex,
        TreeEntry,
        User,
    )
    for table in tables:
        logger.info('Syncing table "{0}"'.format(table.__name__))
        sync_table(table)


def destroy(keyspace):
    """Destroy Cassandra keyspaces"""
    logger.warning('Dropping keyspace "{0}"'.format(keyspace))
    drop_keyspace(keyspace)
