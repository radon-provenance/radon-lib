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
)
from radon.model.microservices import Microservices


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
    

def connect():
    """Connect to a Cassandra cluster.
    
    keyspace, hosts, strategy variables are used to configure the connection.
    See the cfg object in the :mod:`radon.model.config` module, .
    
    :return: A boolean which indicates if the connection is successful
    :rtype: bool
    """
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


def create_default_fields():
    """Create default fields for Solr search"""
    for name, field_type in cfg.default_fields:
        add_search_field(name, field_type)


def create_default_users():
    """Create some users and groups.
    
    Users and groups are defined in DEFAULT_GROUPS and DEFAULT_USERS in the 
    :mod:`radon.model.config` module, .
    
    """
    for name in cfg.default_groups:
        payload_json = {
            "obj": { 
                "name": name
            },
            "meta": {
                "sender": "radon-lib"
            }
        }
        Microservices.create_group(PayloadCreateGroupRequest(payload_json))
        # Do we want to use the listener/web microservices loop for that ?
        # create_group_request(PayloadCreateGroupRequest(payload_json))

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


def create_root():
    """Create the root container
    
    :return: The root collection object
    :rtype: :class:`radon.model.collection.Collection`"""
    # get_root will create the root if it doesn't exist yet
    return Collection.get_root()


def create_tables():
    """Create Cassandra tables for the different models"""
    tables = (
        DataObject,
        Group,
        Notification,
        User,
        TreeNode,
        Config
    )
        
    for table in tables:
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


