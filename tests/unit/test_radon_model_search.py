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

GRP1_NAME = uuid.uuid4().hex
GRP2_NAME = uuid.uuid4().hex
GRP3_NAME = uuid.uuid4().hex

USR1_NAME = uuid.uuid4().hex
USR2_NAME = uuid.uuid4().hex

def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_default_users()
    create_root()

    grp1 = Group.create(name=GRP1_NAME)
    grp2 = Group.create(name=GRP2_NAME)
    grp3 = Group.create(name=GRP3_NAME)
    
    user_name = USR1_NAME
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = [GRP1_NAME]
    user1 = User.create(name=user_name,
                        email=email,
                        password=password, 
                        administrator=administrator,
                        groups=groups)
    
    user_name = USR2_NAME
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = False
    groups = [GRP1_NAME, GRP2_NAME]
    user2 = User.create(name=user_name,
                        email=email,
                        password=password, 
                        administrator=administrator,
                        groups=groups)



def teardown_module(module):
    destroy()


def create_data_object():
    myFactory = Faker()
    content = myFactory.text()
    do = DataObject.create(content.encode())
    return do



def test_put():
    pass



