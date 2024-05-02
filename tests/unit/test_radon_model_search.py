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


import pytest
import uuid
import time

from radon.model.config import cfg
from radon.database import (
    connect,
    create_root,
    create_default_fields,
    create_default_users,
    create_tables,
    destroy,
    initialise,
)
from radon.model.collection import Collection
from radon.model.resource import Resource
from radon.model.search import Search
from radon.model.user import User


TEST_KEYSPACE = "test_keyspace"


def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_root()
    create_default_users()
    create_default_fields()
    
    Collection.create("/", "test")
    resc = Resource.create("/test", "resc.txt")
    if not resc:
        resc = Resource.find("/test/resc.txt")
    resc.update(metadata={"dc_description" : "A metadata to search"})
    # We need to wait a bit ti be sure that indexes has been computed
    time.sleep(1)
    


def teardown_module(module):
    destroy()


def test_search():
    user = User.find("admin")
    
    solr_query = """solr_query='path:unknown'"""
    results = Search.search(solr_query, user)
    assert len(results) == 0
    
    solr_query = """solr_query='path:test'"""
    results = Search.search(solr_query, user)
    assert len(results) == 1
    
    solr_query = """solr_query='path:resc.txt'"""
    results = Search.search(solr_query, user)
    assert len(results) == 1
    
    solr_query = """solr_query='path:*test*'"""
    results = Search.search(solr_query, user)
    assert len(results) == 2
    
    solr_query = """solr_query='dc_description:metadata'"""
    results = Search.search(solr_query, user)
    assert len(results) == 1
    
    solr_query = """solr_query='dc_description:strawberry'"""
    results = Search.search(solr_query, user)
    assert len(results) == 0

    solr_query = """wrong_solr_query='dc_description:strawberry'"""
    results = Search.search(solr_query, user)
    assert results == []
    


