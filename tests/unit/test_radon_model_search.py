# Copyright 2022
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
from faker import Faker
import uuid
import hashlib
import json
import io

from radon import cfg
from radon.database import (
    connect,
    create_default_users,
    create_root,
    create_tables,
    destroy,
    initialise,
)
from radon.model import (
    Collection,
    DataObject,
    Group,
    Resource,
    Search,
    TreeNode,
    User
)
from radon.model.resource import NoUrlResource
from radon.model.errors import(
    CollectionConflictError,
    NoSuchCollectionError,
    ResourceConflictError,
)


TEST_KEYSPACE = "test_keyspace"




def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_default_users()
    create_root()    
    
    try:
        u1 = User.create(name="user1", password="pwd", email="email", administrator=True)
    except:
        pass
    try:
        u2 = User.create(name="user2", password="pwd", email="email", administrator=False)
    except:
        pass
    
    try:
        coll = Collection.create("/", "1")
        r = Resource.create("/1", "a")
    except: # If collections or resources already exist
        pass
  



def teardown_module(module):
    # Due to the time needed to index we may have to add waiting time before
    # destroying things
    #destroy()
    pass



def test_search():
    # Search a collection
    solr_query = """solr_query='path:"1"'"""
    res = Search.search(solr_query, User.find("user1"))
    assert len(res) == 1
    
    # Search a data object
    solr_query = """solr_query='path:"a"'"""
    res = Search.search(solr_query, User.find("user1"))
    assert len(res) == 1
    
    # Search a collection
    solr_query = """solr_query='path:"unknown"'"""
    res = Search.search(solr_query, User.find("user1"))
    assert len(res) == 0
    
    # Invalid query
    solr_query = """solr_query='unknown:"unknown"'"""
    res = Search.search(solr_query, User.find("user1"))
    assert len(res) == 0


