# Radon Copyright 2021, University of Oxford
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

from cassandra.cqlengine import connection
from cassandra.cqlengine.management import (
    create_keyspace_network_topology,
    drop_keyspace,
    sync_table,
    create_keyspace_simple,
)
from cassandra.cluster import (
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
    NoHostAvailable
)
from cassandra import (
    AlreadyExists,
    InvalidRequest
)
from cassandra.policies import WhiteListRoundRobinPolicy
import time

from radon.model.collection import Collection
from radon.model.data_object import DataObject
from radon.model.group import Group
from radon.model.resource import Resource
from radon.model.tree_node import TreeNode
from radon.model.user import User
from radon.model.errors import (
    GroupConflictError,
    UserConflictError,
)
from radon.model.config import (
    MODULE_SEARCH,
    FIELD_TYPE_TEXT,
    OPTION_FIELD_META,
    cfg,
    Config
)
from radon.model.notification import (
    create_group_request,
    create_user_request,
    Notification
)
from radon.model.payload import (
    PayloadCreateGroupRequest,
    PayloadCreateUserRequest,
    PayloadDeleteGroupRequest,
    PayloadDeleteUserRequest,
)
from radon.model.microservices import Microservices

TABLES_LIST = (
    DataObject,
    Group,
    Notification,
    User,
    TreeNode,
    Config,
)


def add_search_field(name, type):
    """
    Add a search field for DSE Search
        
    :param name: The name of the field
    :type name: str
    :param type: The type of the field 
    :type type: str
    
    :return: True if the field has been added
    :rtype: bool
    """
    Config.create(module = MODULE_SEARCH,
                  option = OPTION_FIELD_META,
                  key = name,
                  value = type)
    cluster = connection.get_cluster()
    session = cluster.connect(cfg.dse_keyspace)
    
    query = """ALTER SEARCH INDEX SCHEMA ON {0}.tree_node ADD fields.field[@indexed='true', @name='{1}', @type='{2}'];""".format(
        cfg.dse_keyspace, name, type)
    try:
        session.execute(query)
    except InvalidRequest:
        return False
    
    rebuild_index()
    return True
    

def connect(num_retries=5):
    """Connect to a Cassandra cluster.
    
    keyspace, hosts, strategy variables are used to configure the connection.
    See the cfg object in the :mod:`radon.model.config` module, .
    If we don't want to wait too long we can limit the number of retries.
    
    :param num_retries: Number of retries before failing
    :type num_retries: int
    
    :return: A boolean which indicates if the connection is successful
    :rtype: bool
    """
    print("connect")
    retry_timeout = 2
 
    keyspace = cfg.dse_keyspace
    hosts = cfg.dse_host
    strategy = (cfg.dse_strategy,)

    for i in range(num_retries):
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
            if i < num_retries - 1:
                cfg.logger.warning(
                    "Unable to connect to Cassandra on {0}. Retrying in {1} seconds...".format(
                        hosts,
                        retry_timeout
                    )
                )
                time.sleep(retry_timeout)
            else:
                cfg.logger.warning(
                    "Unable to connect to Cassandra on {0}".format(
                        hosts
                    )
                )
    return False


def create_default_fields():
    """Create default fields for Solr search"""
    for name, field_type in cfg.default_fields:
        add_search_field(name, field_type)



def create_default_groups():
    """Create some groups.
    
    Groups are defined in DEFAULT_GROUPS in the :mod:`radon.model.config` module.
    """
    for name in cfg.default_groups:
        create_group(name)
        # Do we want to use the listener/web microservices loop for that ?
        # create_group_request(PayloadCreateGroupRequest(payload_json))


def create_group(name):
    """Create a group without using the listener"""
    payload_json = {
        "obj": { 
            "name": name
        },
        "meta": {
            "sender": "radon-lib"
        }
    }
    Microservices.create_group(PayloadCreateGroupRequest(payload_json))


def create_default_users():
    """Create some users.
    
    Users are defined in DEFAULT_USERS in the :mod:`radon.model.config` module.
    """
    for login, fullname, email, pwd, is_admin, groups in cfg.default_users:
        payload_json = {
                "obj": { 
                    "login": login,
                    "password": pwd,
                    "fullname": fullname,
                    "email": email,
                    "administrator": is_admin,
                    "groups": groups
                },
                "meta": {
                    "sender": "radon-lib"
                }
            }
        Microservices.create_user(PayloadCreateUserRequest(payload_json))
        # Do we want to use the listener/web microservices loop for that ?
        # create_user_request(PayloadCreateUserRequest(payload_json))


def create_user(login, pwd):
    """Create a user without using the listener"""
    payload_json = {
        "obj": {
            "login": login,
            "password": pwd,
        },
        "meta": {
            "sender": "radon-lib"
        }
    }
    
    Microservices.create_user(PayloadCreateUserRequest(payload_json))


def create_root():
    """Create the root container
    
    :return: The root collection object
    :rtype: :class:`radon.model.collection.Collection`"""
    # get_root will create the root if it doesn't exist yet
    return Collection.get_root()


def create_tables():
    """Create Cassandra tables for the different models"""
    
        
    for table in TABLES_LIST:
        cfg.logger.info('Syncing table "{0}"'.format(table.__name__))
        sync_table(table)
    
    cluster = connection.get_cluster()
    
    session = cluster.connect(cfg.dse_keyspace)
    # Create default search indexes
    query = """CREATE SEARCH INDEX ON {0}.tree_node WITH COLUMNS container, name, user_meta;""".format(cfg.dse_keyspace)
    try:
        session.execute(query)
    except InvalidRequest:  # Search Index already exists
        pass
    
    
    # # Add a field type for the path index (tokenize, lowercase and stem)
    # # query = """ALTER SEARCH INDEX SCHEMA ON radon.tree_node ADD types.fieldType[@name='pathTextField', @class='org.apache.solr.schema.TextField']  
    # #            WITH $$ { "analyzer": {"tokenizer": {"class": "solr.StandardTokenizerFactory"},
    # #                      "filter": [ {"class": "solr.LowerCaseFilterFactory"}, {"class": "solr.PorterStemFilterFactory"} ] }} $$;"""
    # Not using the stem filter finally
    # It will create the indexes /, /test, /test/test2, ...
    query = """ALTER SEARCH INDEX SCHEMA ON {0}.tree_node ADD types.fieldType[@name='pathTextField', @class='org.apache.solr.schema.TextField']  
               WITH $$ {{ "analyzer": {{"tokenizer": {{"class": "solr.PathHierarchyTokenizerFactory"}},
                         "filter": [ {{"class": "solr.LowerCaseFilterFactory"}} ] }}}} $$;""".format(cfg.dse_keyspace)
    try:
        session.execute(query)
    except InvalidRequest:  # Field Type already exists
        pass
    
    query = """ALTER SEARCH INDEX SCHEMA ON {0}.tree_node ADD types.fieldType[@name='TextLine', @class='org.apache.solr.schema.TextField']  
               WITH $$ {{ "analyzer": {{"tokenizer": {{"class": "solr.StandardTokenizerFactory"}},
                         "filter": [ {{"class": "solr.LowerCaseFilterFactory"}} ] }}}} $$;""".format(cfg.dse_keyspace)
    try:
        session.execute(query)
    except InvalidRequest:  # Field Type already exists
        pass
    
    
    # Create a new field for path (concatenate container + name)
    query = """ALTER SEARCH INDEX SCHEMA ON {0}.tree_node ADD fields.field[@name='path', @type='pathTextField'];""".format(cfg.dse_keyspace)
    try:
        session.execute(query)
    except InvalidRequest:  # Field already exists
        pass
    
    query = """ALTER SEARCH INDEX SCHEMA ON {0}.tree_node ADD copyField[@source='container', @dest='path'];""".format(cfg.dse_keyspace)
    try:
        session.execute(query)
    except InvalidRequest:  # Resource Element already exists
        pass
    
    query = """ALTER SEARCH INDEX SCHEMA ON {0}.tree_node ADD copyField[@source='name', @dest='path'];""".format(cfg.dse_keyspace)
    try:
        session.execute(query)
    except InvalidRequest:  # Resource Element already exists
        pass
    
    rebuild_index()

    # Create materialized views
    query = """CREATE MATERIALIZED VIEW notification_by_req_id
               AS SELECT req_id, date, when, op_name, op_type, obj_type, obj_key
               FROM notification
               WHERE req_id IS NOT NULL AND date IS NOT NULL AND when IS NOT NULL AND 
                     op_name IS NOT NULL AND op_type IS NOT NULL AND 
                     obj_type IS NOT NULL AND obj_key IS NOT NULL
               PRIMARY KEY (req_id, op_type, date, when, op_name, obj_type, obj_key);"""
    try:
        session.execute(query)
    except AlreadyExists:   # Materialized view already exists
        pass


def check_keyspace():
    """Check that the keyspace is created"""
    cluster = connection.get_cluster()
    ks_name = cfg.dse_keyspace
    
    return ks_name in cluster.metadata.keyspaces


def check_tables():
    """Check that the tables are created in the keyspace"""
    cluster = connection.get_cluster()
    ks_name = cfg.dse_keyspace

    if ks_name not in cluster.metadata.keyspaces:
        return False
    
    keyspace = cluster.metadata.keyspaces.get(ks_name)
    
    s_existing_tables = set(keyspace.tables.keys())
    s_tables_to_create = set([el.__db_table_name__ for el in TABLES_LIST])
    
    if s_existing_tables != s_tables_to_create:
        return False
    
    return True


def create_keyspace():
    """Create the keyspace and the tables if they don't exist"""
    cluster = connection.get_cluster()
    ks_name = cfg.dse_keyspace
    print(ks_name)

    if ks_name not in cluster.metadata.keyspaces:
        repl_factor = cfg.dse_repl_factor
        create_keyspace_simple(ks_name, repl_factor, True)
    
    try:
        keyspace = cluster.metadata.keyspaces[ks_name]
    except KeyError:
        return False

    s_existing_tables = set(keyspace.tables.keys())
    s_tables_to_create = set([el.__db_table_name__ for el in TABLES_LIST])

    if s_existing_tables != s_tables_to_create:
        print("Creating Tables")
        create_tables()
        create_root()

    return True


def rebuild_index(): 
    """Reload the search index schema and rebuild the search index"""
    cluster = connection.get_cluster()
    session = cluster.connect(cfg.dse_keyspace)
    query = """RELOAD SEARCH INDEX ON {0}.tree_node;""".format(cfg.dse_keyspace)
    session.execute(query)
    
    query = """REBUILD SEARCH INDEX ON {0}.tree_node;""".format(cfg.dse_keyspace)
    session.execute(query)


def rm_search_field(name):
    """
    Add a search field for DSE Search
        
    :param name: The name of the field
    :type name: str
    """ 
    c = Config.objects.filter(module = MODULE_SEARCH,
                              option = OPTION_FIELD_META,
                              key = name)
    c.delete()
    cluster = connection.get_cluster()
    session = cluster.connect(cfg.dse_keyspace)
    
    query = """ALTER SEARCH INDEX SCHEMA ON {0}.tree_node DROP field "{1}";""".format(
                    cfg.dse_keyspace,
                    name)
    try:
        session.execute(query)
    except InvalidRequest:
        return
    
    rebuild_index()


def delete_group(name):
    """Delete a group without using the listener"""
    payload_json = {
        "obj": { 
            "name": name
        },
        "meta": {
            "sender": "radon-lib"
        }
    }
    Microservices.delete_group(PayloadDeleteGroupRequest(payload_json))


def delete_user(login):
    """Delete a user without using the listener"""
    payload_json = {
        "obj": { 
            "login": login
        },
        "meta": {
            "sender": "radon-lib"
        }
    }
    Microservices.delete_user(PayloadDeleteUserRequest(payload_json))


def destroy():
    """Destroy Cassandra keyspace. The keyspace contains all the tables."""
    keyspace = cfg.dse_keyspace
    cfg.logger.warning('Dropping keyspace "{0}"'.format(keyspace))
    drop_keyspace(keyspace)


def init_keyspace():
    if not connect():
        return False
    
    cluster = connection.get_cluster()
    keyspace = cfg.dse_keyspace
    
    if keyspace not in cluster.metadata.keyspaces:
        if cfg.dse_strategy == "NetworkTopologyStrategy":
            create_keyspace_network_topology(keyspace, dc_replication_map, True)
        else:
            create_keyspace_simple(keyspace, repl_factor, True)
    create_tables()
    create_root()
    


def initialise():
    """Initialise the Cassandra database
    
    repl_factor, dc_replication_map, keyspace variables are used to configure 
    the connection. See the cfg object in the :mod:`radon.model.config` 
    module, .
    
    :return: A boolean which indicates if the connection is successful
    :rtype: bool
    """
    print("initialise 1")
    if not connect():
        return False
    repl_factor = cfg.dse_repl_factor
    dc_replication_map = cfg.dse_dc_replication_map
    keyspace = cfg.dse_keyspace

    cluster = connection.get_cluster()
    if keyspace not in cluster.metadata.keyspaces:
        if cfg.dse_strategy == "NetworkTopologyStrategy":
            create_keyspace_network_topology(keyspace, dc_replication_map, True)
        else:
            create_keyspace_simple(keyspace, repl_factor, True)
    
    create_tables()
    create_root()
    
 
    return True


