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

import zipfile
from io import (
    BytesIO,
    StringIO
)
from datetime import datetime
import json


from radon.models.data_object import DataObject
from radon import cfg
from radon.models import connect
from radon.models.acl import (
    Ace,
    acl_list_to_cql
)

# Populated by create_test_env
TEST_KEYSPACE = "radon_pytest"
TEST_CONTENT = "Test Data".encode()
TEST_CONTENT1 = "This ".encode()
TEST_CONTENT2 = "is ".encode()
TEST_CONTENT3 = "a ".encode()
TEST_CONTENT4 = "test.".encode()


def test_create():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    do = DataObject.create(TEST_CONTENT)
    data = []
    for chk in do.chunk_content():
        data.append(chk)
    res = b"".join([s for s in data])
    assert res == TEST_CONTENT
    do.delete()

    do = DataObject.create(TEST_CONTENT, compressed=True)
    data = BytesIO(do.blob)
    z = zipfile.ZipFile(data, "r")
    content = z.read("data")
    data.close()
    z.close()
    assert content == TEST_CONTENT
    do.delete()

    meta = {"test": "value"}
    now = datetime.now()
    do = DataObject.create(TEST_CONTENT,
                           metadata=meta,
                           create_ts=now)
    assert do.create_ts == now
    assert do.metadata == meta
    do.delete()

    list_read = ['grp1']
    list_write = ['grp1']
    acl_dict = {"grp1": Ace(acetype="ALLOW", identifier="grp1", 
                               aceflags=0, acemask=95)
               }
    do = DataObject.create(TEST_CONTENT,
                           acl=acl_dict)
    assert do.acl == acl_dict
    do.delete()


def test_create_acl():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    list_read = ['grp1']
    list_write = ['grp1']
    acl_cql_string = acl_list_to_cql(list_read, list_write)
    do = DataObject.create(TEST_CONTENT)
    do.create_acl(acl_cql_string)
    
    do = DataObject.find(do.uuid)
    assert do.acl['grp1']['acemask'] == 95 # read/write
    do.delete()
    
    do = DataObject.create(TEST_CONTENT)
    do.create_acl_list(list_read, list_write)
    
    do = DataObject.find(do.uuid)
    assert do.acl['grp1']['acemask'] == 95 # read/write
    do.delete()


def test_delete_id():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    do = DataObject.create(TEST_CONTENT)
    
    DataObject.delete_id(do.uuid)
    do = DataObject.find(do.uuid)
    assert do == None


def test_update():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    list_read = ['grp1']
    list_write = ['grp1']

    do = DataObject.create(TEST_CONTENT)
    do.update(type="text", blob=TEST_CONTENT)
    do = DataObject.find(do.uuid)
    assert do.type == "text"
    assert do.blob == TEST_CONTENT
    do.delete()

    do = DataObject.create(TEST_CONTENT)
    acl_cql_string = acl_list_to_cql(list_read, list_write)
    do.update_acl(acl_cql_string)
    do = DataObject.find(do.uuid)
    assert do.acl['grp1']['acemask'] == 95 # read/write
    do.delete()

    do = DataObject.create(TEST_CONTENT)
    do.update_acl_list(list_read, list_write)
    do = DataObject.find(do.uuid)
    assert do.acl['grp1']['acemask'] == 95 # read/write
    do.delete()


def test_append():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    do = DataObject.create(TEST_CONTENT1)
    DataObject.append_chunk(do.uuid, TEST_CONTENT2, 1)
    DataObject.append_chunk(do.uuid, TEST_CONTENT3, 2)
    DataObject.append_chunk(do.uuid, TEST_CONTENT4, 3)
    do = DataObject.find(do.uuid)
    data = []
    for chk in do.chunk_content():
        data.append(chk)
    assert b"".join(data) == b'This is a test.'
    DataObject.delete_id(do.uuid)
    
    do = DataObject.create(TEST_CONTENT1, compressed=True)
    DataObject.append_chunk(do.uuid, TEST_CONTENT2, 1, True)
    DataObject.append_chunk(do.uuid, TEST_CONTENT3, 2, True)
    DataObject.append_chunk(do.uuid, TEST_CONTENT4, 3, True)
    do = DataObject.find(do.uuid)
    data = []
    for chk in do.chunk_content():
        data.append(chk)
    assert b"".join(data) == b'This is a test.'
    DataObject.delete_id(do.uuid)



