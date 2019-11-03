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
    #    drop_keyspace,
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
from radon.log import init_log
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


logger = init_log("models")


def initialise():
    """Initialise Cassandra connection"""
    num_retries = 6
    retry_timeout = 1

    keyspace = cfg.dse_keyspace
    strategy = (cfg.dse_strategy,)
    repl_factor = cfg.dse_repl_factor
    hosts = cfg.dse_host

    for retry in range(num_retries):
        try:
            logger.info(
                'Connecting to Cassandra keyspace "{2}" '
                'on "{0}" with strategy "{1}"'.format(hosts, strategy, keyspace)
            )
            connection.setup(
                hosts,
                keyspace,
                protocol_version=3,
                load_balancing_policy=DCAwareRoundRobinPolicy(),
            )

            if strategy is "SimpleStrategy":
                create_keyspace_simple(keyspace, repl_factor, True)
            else:
                create_keyspace_network_topology(keyspace, {}, True)

            break
        except dse.cluster.NoHostAvailable:
            logger.warning(
                "Unable to connect to Cassandra. Retrying in {0} seconds...".format(
                    retry_timeout
                )
            )
            time.sleep(retry_timeout)
            retry_timeout *= 2


def sync():
    """Create tablesfor the different models"""
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
