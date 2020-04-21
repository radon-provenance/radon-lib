"""Copyright 2020 - 

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


from dse.cqlengine import connection
from dse.cqlengine.connection import get_cluster
from dse.cqlengine.management import (
    create_keyspace_simple,
    drop_keyspace
)

from radon import cfg
from radon.models import (
    destroy,
    initialise,
    sync
)

TEST_KEYSPACE = "test_keyspace"

def test_initialise():
    assert initialise() == True
    cluster = connection.get_cluster()
    # Check keyspace has been created
    assert cfg.dse_keyspace in cluster.metadata.keyspaces
    


def test_keyspace_simple():
    cfg.dse_keyspace = TEST_KEYSPACE
    cfg.dse_strategy = "SimpleStrategy"

    assert initialise() == True
    cluster = connection.get_cluster()
    # Check keyspace has been created
    assert TEST_KEYSPACE in cluster.metadata.keyspaces
    drop_keyspace(TEST_KEYSPACE)
    # Check keyspace has been deleted
    assert TEST_KEYSPACE not in cluster.metadata.keyspaces


def test_keyspace_network_topology():
    """We would need a correct setup with multiple data centers to test this
    correctly""" 
    cfg.dse_keyspace = TEST_KEYSPACE
    cfg.dse_strategy = "NetworkTopologyStrategy"
    cfg.dse_dc_replication_map = {"dc1": 1}

    assert initialise() == True
    cluster = connection.get_cluster()
    # Check keyspace has been created
    assert TEST_KEYSPACE in cluster.metadata.keyspaces
    drop_keyspace(TEST_KEYSPACE)
    # Check keyspace has been deleted
    assert TEST_KEYSPACE not in cluster.metadata.keyspaces


def test_destroy():
    cfg.dse_keyspace = TEST_KEYSPACE
    create_keyspace_simple(TEST_KEYSPACE, 1, True)
    cluster = connection.get_cluster()
    destroy(TEST_KEYSPACE)
    assert TEST_KEYSPACE not in cluster.metadata.keyspaces


def test_tables():
    cfg.dse_keyspace = TEST_KEYSPACE
    
    # list of tables that has to be created
    ls_tables = {'data_object', 'group', 'idsearch', 'notification', 
                 'search_index', 'tree_entry', 'user'}
    initialise()
    sync()
    cluster = connection.get_cluster()
    created_tables = set(cluster.metadata.keyspaces[TEST_KEYSPACE].tables.keys())
    assert created_tables.difference(ls_tables)==set()
    
    drop_keyspace(TEST_KEYSPACE)



