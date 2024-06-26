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


import cassandra.cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.connection import get_cluster
from cassandra.cqlengine.management import (
    create_keyspace_simple,
    drop_keyspace
)
import uuid

from radon.model.config import cfg
from radon.database import (
    add_search_field,
    connect,
    create_default_fields,
    create_default_users,
    destroy,
    initialise,
    create_root,
    create_tables,
    rm_search_field
)

TEST_KEYSPACE = "test_keyspace"



def test_creation():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    create_tables()
    create_default_users()
    create_root()
    # Test creation if they already exist
    create_default_users()
    destroy()


def test_destroy():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    create_keyspace_simple(TEST_KEYSPACE, 1, True)
    cluster = connection.get_cluster()
    assert TEST_KEYSPACE in cluster.metadata.keyspaces
    destroy()
    assert TEST_KEYSPACE not in cluster.metadata.keyspaces


def test_initialise():
    cfg.dse_keyspace = TEST_KEYSPACE
    assert initialise() == True
    cluster = connection.get_cluster()
    # Check keyspace has been created
    assert cfg.dse_keyspace in cluster.metadata.keyspaces
    # Keyspace should already exist
    assert initialise() == True
    destroy()


def test_fail_initialise(mocker):
    mocker.patch('radon.database.connect', return_value=False)
    assert initialise() == False


def test_fail_connection_setup(mocker):
    # Raise a fake exception to test all parts of the connection code
    mocker.patch('cassandra.cqlengine.connection.setup', 
                 side_effect=cassandra.cluster.NoHostAvailable(
                     "My Fake Error", 
                     { "127.0.0.1": Exception() }))
    # Deactivate the sleep method to sped up tests
    mocker.patch("time.sleep", return_value=True)
    assert initialise() == False


def test_keyspace_network_topology():
    """We would need a correct setup with multiple data centers to test this
    correctly""" 
    cfg.dse_keyspace = TEST_KEYSPACE
    cfg.dse_strategy = "NetworkTopologyStrategy"
    cfg.dse_dc_replication_map = {"dc1": 1}
    assert initialise() == True
    destroy()


def test_keyspace_simple():
    cfg.dse_keyspace = TEST_KEYSPACE
    cfg.dse_strategy = "SimpleStrategy"

    assert initialise() == True
    cluster = connection.get_cluster()
    # Check keyspace has been created
    assert TEST_KEYSPACE in cluster.metadata.keyspaces
    destroy()
    # Check keyspace has been deleted
    assert TEST_KEYSPACE not in cluster.metadata.keyspaces


def test_tables():
    cfg.dse_keyspace = TEST_KEYSPACE
    
    # list of tables that has to be created
    ls_tables = {'data_object', 'group', 'notification', 
                 'tree_node', 'user', 'config'}
    initialise()
    create_tables()
    cluster = connection.get_cluster()
    created_tables = set(cluster.metadata.keyspaces[TEST_KEYSPACE].tables.keys())
    assert created_tables.difference(ls_tables)==set()
    
    # Already existing tables
    create_tables()
    destroy()


def test_search_field():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    create_tables()
    create_default_users()
    create_root()
    
    # Add a new field
    field_name = uuid.uuid4().hex
    add_search_field(field_name, "StrField")
    
    # Add a field with a wrong field type
    add_search_field(uuid.uuid4().hex, "WrongField")
    
    # create default fields (from Config)
    create_default_fields()
    
    # Remove an existing field
    rm_search_field(field_name)
    
    # Remove a non existing field
    rm_search_field(uuid.uuid4().hex)

    destroy()
    

