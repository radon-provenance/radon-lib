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


import os

from dotenv import load_dotenv

from dse.cqlengine import columns
from dse.cqlengine.models import connection, Model
from dse.query import SimpleStatement

from radon.log import init_logger



ENV_DSE_HOST_VAR = "DSE_HOST"
ENV_MQTT_HOST_VAR = "MQTT_HOST"

DEFAULT_DSE_HOST = "127.0.0.1"
DEFAULT_DSE_KEYSPACE = "radon"
DEFAULT_DSE_STRATEGY = "SimpleStrategy"
DEFAULT_DSE_REPL_FACTOR = 1
DEFAULT_MQTT_HOST = "127.0.0.1"

SYS_LIB_USER = "radon_lib"
SYS_META_CREATE_TS = "radon_create_ts"
SYS_META_MODIFY_TS = "radon_modify_ts"
SYS_META_MIMETYPE = "radon_mimetype"
SYS_META_SIZE = "radon_size"
SYS_META_CREATE_TS_INTERFACE = "Creation date"
SYS_META_MODIFY_TS_INTERFACE = "Modification date"
SYS_META_MIMETYPE_INTERFACE = "Mimetype"
SYS_META_SIZE_INTERFACE = "Size"
# Keep a list of metadata which are serialized datetime instances
SYS_META_DATETIMES = [
    SYS_META_CREATE_TS, SYS_META_MODIFY_TS
]


DEFAULT_GROUPS = [
    "admins",
    "users"
]

DEFAULT_USERS = [
    ("admin", "Default Administrator", "admin@radon.com", "radon", True, ["admins", "users"]),
    ("test", "Test User", "test@radon.com", "radon", False, ["users"])
]


# Max size for Data Object blobs
CHUNK_SIZE = 1048576
# Compress Data Object data in Cassandra
COMPRESS_DO = False
PROTOCOL_CASSANDRA = "cassandra://"


# LDAP server configuration.
# ldap://ldap.example.com
AUTH_LDAP_SERVER_URI = os.getenv("AUTH_LDAP_SERVER_URI", None)
# "uid=%(user)s,ou=users,dc=example,dc=com"
AUTH_LDAP_USER_DN_TEMPLATE = os.getenv("AUTH_LDAP_USER_DN_TEMPLATE", None)


# For Config stored in Cassandra

MODULE_DSE = "dse_module"
MODULE_SEARCH = "search_module"
MODULE_META = "meta_module"

OPTION_FIELD_META = "field_meta"

FIELD_TYPE_TEXT = "TextLine"
FIELD_TYPE_STR = "StrField"
FIELD_TYPE_INT = "TrieIntField"

FIELD_TYPE_INTERFACE = [
    (FIELD_TYPE_TEXT, FIELD_TYPE_TEXT),
    (FIELD_TYPE_STR, FIELD_TYPE_STR),
    (FIELD_TYPE_INT, FIELD_TYPE_INT),
]


DEFAULT_FIELDS = [
    ("dc_contributor", FIELD_TYPE_TEXT),
    ("dc_coverage", FIELD_TYPE_TEXT),
    ("dc_creator", FIELD_TYPE_TEXT),
    ("dc_date", FIELD_TYPE_TEXT),
    ("dc_description", FIELD_TYPE_TEXT),
    ("dc_format", FIELD_TYPE_TEXT), 
    ("dc_identifier", FIELD_TYPE_TEXT),
    ("dc_language", FIELD_TYPE_TEXT),
    ("dc_publisher", FIELD_TYPE_TEXT),
    ("dc_relation", FIELD_TYPE_TEXT),
    ("dc_rights", FIELD_TYPE_TEXT), 
    ("dc_source", FIELD_TYPE_TEXT),
    ("dc_subject", FIELD_TYPE_TEXT),  
    ("dc_title", FIELD_TYPE_TEXT),   
    ("dc_type", FIELD_TYPE_TEXT),
]


class LocalConfig(object):
    """Store the configuration options for radon
     - Environment variables to set:
        - DSE_HOST: space separated list of IP/Host address (default: ('127.0.0.1',))
        - MQTT_HOST: IP/Host address of the MQTT server (default: '127.0.0.1')
        
    :param dse_host: A list of IP/Host address for the DSE nodes
    :type dse_host: List[str]
    :param dse_keyspace: Name of the keyspace where the tables will be created
    :type dse_keyspace: str
    :param dse_dc_replication_map: The replication map determines how many 
      copies of the data are kept in a given data center. We don't use it yet.
    :type dse_dc_replication_map: dict
    :param dse_strategy: Replication strategy. To be improved for production,
      for the moment we only use the Simple Strategy
    :type dse_strategy: str
    :param dse_repl_factor: Number of copies of each row. 1 for the moment but 
      should be higher when we use a real cluster
    :type dse_repl_factor: int
    :param mqtt_host: IP/host address of the MQTT server
    :type mqtt_host: str
    :param debug: Debug mode
    :type debug: bool
    :param meta_create_ts: Name of the metadata which stores the creation
      timestamp
    :type meta_create_ts: str
    :param meta_modify_ts: Name of the metadata which stores the modification
      timestamp
    :type meta_modify_ts: str
    :param meta_mimetype: Name of the metadata which stores the mimetype
    :type meta_mimetype: str
    :param meta_size: Name of the metadata which stores the size
    :type meta_size: str
    :param vocab_dict: Dictionary which maps the internal metadata name to a
      meaningful name which can be used in the UI.
    :type vocab_dict: dict
    :param meta_datetimes: List of metadata which store datetimes object (they
      need a specific encoding/decoding procedure.
    :type meta_datetimes: List[str]
    :param sys_lib_user: The username used in notification for an operation 
      managed by the library
    :type sys_lib_user: str
    :param default_groups: List of groups to be created when the keyspace is
      created
    :type default_groups: List[str]
    :param default_users: List of users to be created when the keyspace is
      created
    :type default_users: List[str]
    :param chunk_size: Max size for the chunks of Data Objects blobs stored in
      Cassandra 
    :type chunk_size: int
    :param compress_do: Compress the blobs.
    :type compress_do: bool
    :param protocol_cassandra: Prefix used for the URL of the data objects 
      stored in Radon
    :type protocol_cassandra: str
    :param auth_ldap_server_uri: IP/host address of the LDAP server
    :type auth_ldap_server_uri: str
    :param auth_ldap_user_dn_template: LDAP User DN template
    :type auth_ldap_user_dn_template: str
    :param logger: The default logger
    :type logger: :class:`logging.logger`
    """

    def __init__(self):
        # List of host address for the DSE cluster
        dse_host_var = os.environ.get(ENV_DSE_HOST_VAR)
        if dse_host_var:
            self.dse_host = dse_host_var.split(" ")
        else:
            self.dse_host = [DEFAULT_DSE_HOST,]
        # Cassandra keyspace
        self.dse_keyspace = DEFAULT_DSE_KEYSPACE
        #  ("SimpleStrategy" or "NetworkTopologyStrategy")
        self.dse_strategy = DEFAULT_DSE_STRATEGY
        # Not used for Simple Strategy
        # map of dc_names: replication_factor for NetworkTopologyStrategy
        self.dse_dc_replication_map = {}
        self.dse_repl_factor = DEFAULT_DSE_REPL_FACTOR

        # IP address of the MQTT server
        self.mqtt_host = os.environ.get(ENV_MQTT_HOST_VAR, DEFAULT_MQTT_HOST)
        
        # Debug mode
        self.debug = False

        self.meta_create_ts = SYS_META_CREATE_TS
        self.meta_modify_ts = SYS_META_MODIFY_TS
        self.meta_mimetype = SYS_META_MIMETYPE
        self.meta_size = SYS_META_SIZE
        self.vocab_dict = {
            SYS_META_CREATE_TS: SYS_META_CREATE_TS_INTERFACE,
            SYS_META_MODIFY_TS: SYS_META_MODIFY_TS_INTERFACE,
            SYS_META_MIMETYPE: SYS_META_MIMETYPE_INTERFACE,
            SYS_META_SIZE: SYS_META_SIZE_INTERFACE,
        }
        self.meta_datetimes = SYS_META_DATETIMES
        self.sys_lib_user = SYS_LIB_USER

        self.default_groups = DEFAULT_GROUPS

        self.default_users = DEFAULT_USERS
        self.default_fields = DEFAULT_FIELDS
        self.field_type_interface = FIELD_TYPE_INTERFACE

        self.chunk_size = CHUNK_SIZE
        self.compress_do = COMPRESS_DO
        self.protocol_cassandra = PROTOCOL_CASSANDRA

        self.auth_ldap_server_uri = AUTH_LDAP_SERVER_URI
        self.auth_ldap_user_dn_template = AUTH_LDAP_USER_DN_TEMPLATE
        
        self.logger = init_logger("radon-lib", self)


    def to_dict(self):
        """
        Return a dictionary with some options
        
        :return: The dictionary with names/values pairs
        :rtype: dict
        """
        return {
            "dse_host" : self.dse_host,
            "dse_keyspace" : self.dse_keyspace,
            "dse_dc_replication_map" : self.dse_dc_replication_map,
            "dse_strategy" : self.dse_strategy,
            "dse_repl_factor": self.dse_repl_factor,
            "mqtt_host" : self.mqtt_host,
            "debug" : self.debug
        }


    def __repr__(self):
        return str(self.to_dict())





class Config(Model):
    """Config Model
    
    This is used to store dynamic configuration, options are lists of key/value 
    pairs. One row in the table is one option value so lists will be split in
    multiple rows.
    This is more general as it can deal with simple or complex options. For
    simple options we can store the value directly in the key or value column
    (as soon as we are consistent), there will be only one row returned when we
    look for the option.
    
    Example:
    
    |--------|----------|----------|----------|
    | module |  option  |   key    |   value  |
    |--------|----------|----------|----------|
    | search |   field  |  desc    | TextLine |
    | search |   field  |  author  | TextLine |
    |   dse  | keyspace | keyspace |  radon   |
    | ...                                     |
    |--------|----------|----------|----------|
    
    :param module: The name of the radon module that has to be configured
    :type module: :class:`columns.Text`
    :param option: The name of the option
    :type option: :class:`columns.Text`
    :param key: A specific key for the option 
    :type key: :class:`columns.Text`
    :param value: The value associated to the specific key of the option 
    :type value: :class:`columns.Text`    
    
    """

    module = columns.Text(partition_key=True)
    option = columns.Text(primary_key=True)
    key = columns.Text(primary_key=True)
    value = columns.Text()
    
    
    @classmethod
    def get_search_indexes(cls):  
        cfg_fields = Config.objects.filter(module=MODULE_SEARCH,
                                           option=OPTION_FIELD_META)
        list_index = []
        for field in cfg_fields:
            list_index.append((field.key, field.value))
        return list_index


# Load environment variables to initialize config with the user-defined value
# if they exist
load_dotenv()
cfg = LocalConfig()



