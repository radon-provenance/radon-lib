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


from radon.model import (
    DataObject
)
from radon.database import (
    connect,
    create_default_users,
    create_root,
    create_tables,
    destroy,
    initialise,
)
from radon import cfg
from radon.model.acl import (
    Ace,
    acl_list_to_cql
)

TEST_CONTENT = "Test Data".encode()
TEST_CONTENT1 = "This ".encode()
TEST_CONTENT2 = "is ".encode()
TEST_CONTENT3 = "a ".encode()
TEST_CONTENT4 = "test.".encode()


TEST_KEYSPACE = "test_keyspace"


def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_default_users()
    create_root()


def teardown_module(module):
    destroy()

def test_create():
    do = DataObject.create(TEST_CONTENT)
    data = []
    for chk in do.chunk_content():
        data.append(chk)
    res = b"".join([s for s in data])
    assert res == TEST_CONTENT
    assert do.get_url() == cfg.protocol_cassandra + do.uuid
    do.delete()

    do = DataObject.create(TEST_CONTENT, compressed=True)
    data = BytesIO(do.blob)
    z = zipfile.ZipFile(data, "r")
    content = z.read("data")
    data.close()
    z.close()
    assert content == TEST_CONTENT
    do.delete()



def test_delete_id():
    do = DataObject.create(TEST_CONTENT)
    
    DataObject.delete_id(do.uuid)
    do = DataObject.find(do.uuid)
    assert do == None


def test_update():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    do = DataObject.create(TEST_CONTENT)
    do.update(blob=TEST_CONTENT)
    do = DataObject.find(do.uuid)
    assert do.blob == TEST_CONTENT
    do.delete()


def test_append():
    do = DataObject.create(TEST_CONTENT1)
    DataObject.append_chunk(do.uuid, 1, TEST_CONTENT2)
    DataObject.append_chunk(do.uuid, 2, TEST_CONTENT3)
    DataObject.append_chunk(do.uuid, 3, TEST_CONTENT4)
    do = DataObject.find(do.uuid)
    data = []
    for chk in do.chunk_content():
        data.append(chk)
    assert b"".join(data) == b'This is a test.'
    DataObject.delete_id(do.uuid)
    
    do = DataObject.create(TEST_CONTENT1, compressed=True)
    DataObject.append_chunk(do.uuid, 1, TEST_CONTENT2, True)
    DataObject.append_chunk(do.uuid, 2, TEST_CONTENT3, True)
    DataObject.append_chunk(do.uuid, 3, TEST_CONTENT4, True)
    do = DataObject.find(do.uuid)
    data = []
    for chk in do.chunk_content():
        data.append(chk)
    assert b"".join(data) == b'This is a test.'
    DataObject.delete_id(do.uuid)





