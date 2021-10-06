# Copyright 2021
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dse.cqlengine import connection
from dse.cqlengine.management import (
    create_keyspace_network_topology,
    drop_keyspace,
     sync_table,
    create_keyspace_simple,
)
from dse.cluster import (
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
    NoHostAvailable
)
from dse.policies import WhiteListRoundRobinPolicy
import time
#
from radon import cfg
from radon.model import (
    Collection,
    DataObject,
    Group,
    Notification,
    TreeNode,
    User
)
from radon.model.errors import (
    GroupConflictError,
    UserConflictError,
)


def connect():
    """Connect to a Cassandra cluster.
    
    keyspace, hosts, strategy variables are used to configure the connection.
    See the cfg object in the :mod:`radon.model.config` module, .
    
    :return: A boolean which indicates if the connection is successful
    :rtype: bool"""
    num_retries = 5
    retry_timeout = 2
 
    keyspace = cfg.dse_keyspace
    hosts = cfg.dse_host
    strategy = (cfg.dse_strategy,)

    for _ in range(num_retries):
        try:
            cfg.logger.info(
                'Connecting to Cassandra keyspace "{2}" '
                'on "{0}" with strategy "{1}"'.format(hosts, strategy, keyspace)
            )
            profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(hosts))
            profiles = {EXEC_PROFILE_DEFAULT: profile}
            connection.setup(
                hosts,
                keyspace,
                protocol_version=3,
                execution_profiles=profiles,
            )
            return True
        except NoHostAvailable:
            cfg.logger.warning(
                "Unable to connect to Cassandra on {0}. Retrying in {1} seconds...".format(
                    hosts,
                    retry_timeout
                )
            )
            time.sleep(retry_timeout)
    return False


def create_default_users():
    """Create some users and groups
    
    Users and groups are defined in DEFAULT_GROUPS and DEFAULT_USERS in the 
    :mod:`radon.model.config` module, .
    
    """
    for name in cfg.default_groups:
        try:
            Group.create(name=name)
        except GroupConflictError:
            pass
    for name, email, pwd, is_admin, groups in cfg.default_users:
        try:
            User.create(name=name,
                        email=email,
                        password=pwd,
                        administrator=is_admin,
                        groups=groups)
        except UserConflictError:
            pass


def create_root():
    """Create the root container
    
    :return: The root collection object
    :rtype: :class:`radon.model.Collection`"""
    # get_root will create the root if it doesn't exist yet
    return Collection.get_root()


def create_tables():
    """Create Cassandra tables for the different models"""
    tables = (
        DataObject,
        Group,
#         IDSearch,
        Notification,
#         SearchIndex,
        User,
        TreeNode,
    )
    for table in tables:
        cfg.logger.info('Syncing table "{0}"'.format(table.__name__))
        sync_table(table)


def destroy():
    """Destroy Cassandra keyspace. The keyspace contains all the tables."""
    keyspace = cfg.dse_keyspace
    cfg.logger.warning('Dropping keyspace "{0}"'.format(keyspace))
    drop_keyspace(keyspace)


def initialise():
    """Initialise the Cassandra connection
    
    repl_factor, dc_replication_map, keyspace variables are used to configure 
    the connection. See the cfg object in the :mod:`radon.model.config` 
    module, .
    
    :return: A boolean which indicates if the connection is successful
    :rtype: bool
    """
    if not connect():
        return False
    repl_factor = cfg.dse_repl_factor
    dc_replication_map = cfg.dse_dc_replication_map
    keyspace = cfg.dse_keyspace
     
    cluster = connection.get_cluster()
    if keyspace in cluster.metadata.keyspaces:
        # If the keyspace already exists we do not create it. Should we raise
        # an error
        return True
    if cfg.dse_strategy == "NetworkTopologyStrategy":
        create_keyspace_network_topology(keyspace, dc_replication_map, True)
    else:
        create_keyspace_simple(keyspace, repl_factor, True)
 
    return True


