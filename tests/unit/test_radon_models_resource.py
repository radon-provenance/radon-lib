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

import pytest
from faker import Faker
import uuid
import hashlib
import json

from radon.models.collection import Collection
from radon.models.resource import (
    PROTOCOL_CASSANDRA,
    Resource
)
from radon.models.user import User
from radon.models.data_object import DataObject

from radon import cfg
from radon.models import connect
from radon.models.errors import(
    CollectionConflictError,
    NoSuchCollectionError,
    ResourceConflictError,
)


# Populated by create_test_env
TEST_KEYSPACE = "radon_pytest"


def create_data_object():
    myFactory = Faker()
    content = myFactory.text()
    do = DataObject.create(content.encode())
    return do


def test_resource():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    coll_name = uuid.uuid4().hex
    meta_dict = {"meta": "val"}
 
    with pytest.raises(NoSuchCollectionError):
        resc = Resource.create("/{}".format(coll_name), uuid.uuid4().hex)

    coll = Collection.create(coll_name)
    do = create_data_object()
    
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid),
                           metadata=meta_dict)

    with pytest.raises(ResourceConflictError):
        resc = Resource.create(coll.path, resc_name)

    resc.delete()
    # Check resource is gone
    resc = Resource.find(resc.path)
    assert resc == None

    do = create_data_object()
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid),
                           mimetype="text/plain",
                           size=do.size)

    assert resc.get_size() == do.size
    assert resc.get_mimetype() == "text/plain"
    resc.delete()
    
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr",
                           metadata=meta_dict)
    resc.delete()
    
    coll.delete()
    # Check deleting resource also deleted data object
    do = DataObject.find(do.uuid)
    assert do == None


def test_chunk_content():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    do = DataObject.create(content.encode())
    
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    data = []
    for chk in resc.chunk_content():
        data.append(chk)
    res = b"".join([s for s in data])
    assert res == content.encode()
    
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr")
    assert resc.chunk_content() == None
    
    coll.delete()


def test_acl():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    list_read = ['grp1']
    list_write = ['grp1']

    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    
    # Read/Write resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    assert resc.get_acl()['grp1'].acemask == 95     # read/write
    assert resc.get_acl_metadata()['cdmi_acl'][0].get('acemask') == "DELETE_OBJECT, WRITE_METADATA, READ_METADATA, APPEND_DATA, WRITE_OBJECT, READ_OBJECT"
    assert resc.get_authorized_actions(User.find("user1")) == {'delete', 'read', 'edit', 'write'}
    read_access, write_access = resc.get_acl_list()
    assert read_access == ['grp1']
    assert write_access == ['grp1']
    resc.delete()

    # Read/Write resource with a loss of the DataObject for unknown reason
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    # Simulate a loss of object (should never happens)
    resc.obj = None
    assert resc.get_acl()['grp1'].acemask == 95     # read/write
    resc.delete()

    # Read/Write resource with a non existing DataObject 
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, uuid))
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    # No acl as they are stored in DataObject
    assert resc.get_acl() == {}
    resc.delete()

    # Read resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.create_acl_list(list_read, [])
    resc = Resource.find(resc.path)
    assert resc.get_acl()['grp1'].acemask == 9      # read
    assert resc.get_acl_metadata()['cdmi_acl'][0].get('acemask') == "READ_METADATA, READ_OBJECT"
    assert resc.get_authorized_actions(User.find("user1")) == {'read'}
    read_access, write_access = resc.get_acl_list()
    assert read_access == ['grp1']
    assert write_access == []
    resc.delete()

    # Write resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.create_acl_list([], list_write)
    resc = Resource.find(resc.path)
    assert resc.get_acl()['grp1'].acemask == 86      # write
    assert resc.get_acl_metadata()['cdmi_acl'][0].get('acemask') == "DELETE_OBJECT, WRITE_METADATA, APPEND_DATA, WRITE_OBJECT"
    assert resc.get_authorized_actions(User.find("user1")) == {'edit', 'delete', 'write'}
    read_access, write_access = resc.get_acl_list()
    assert read_access == []
    assert write_access == ['grp1']
    
    
    resc.delete()

    # Resource stored in Cassandra, acl inherited from root
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    assert resc.get_authorized_actions(User.find("user1")) == {"read"}
    resc.delete()

    # Read/Write resource stored as a reference
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr")
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    assert resc.get_acl()['grp1'].acemask == 95
    assert resc.get_acl_metadata()['cdmi_acl'][0].get('acemask') == "DELETE_OBJECT, WRITE_METADATA, READ_METADATA, APPEND_DATA, WRITE_OBJECT, READ_OBJECT"
    assert resc.get_authorized_actions(User.find("user1")) == {'delete', 'read', 'edit', 'write'}
    resc.delete()

    # Resource stored in Cassandra, update acl
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.update_acl_list([], list_write)
    resc = Resource.find(resc.path)
    acl_dict = resc.get_acl()
    assert resc.get_acl()['grp1'].acemask == 86
    list_read = ['grp2', 'grp3']
    list_write = ['grp2']
    resc.update_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    acl_dict = resc.get_acl()
    assert resc.get_acl()['grp1'].acemask == 86
    assert resc.get_acl()['grp2'].acemask == 95
    assert resc.get_acl()['grp3'].acemask == 9
    
    assert resc.user_can(User.find("user1"), "delete")       # admin
    assert resc.user_can(User.find("user2"), "write")        # grp2
    assert resc.user_can(User.find("user4"), "read")         # grp3
    assert not resc.user_can(User.find("user4"), "write")    # grp3
    resc.delete()

    # Update acl resource stored as a reference
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr")
    resc.update_acl_list([], ['grp1'])
    resc = Resource.find(resc.path)
    acl_dict = resc.get_acl()
    assert resc.get_acl()['grp1'].acemask == 86
    list_read = ['grp2', 'grp3']
    list_write = ['grp2']
    resc.update_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    acl_dict = resc.get_acl()
    assert resc.get_acl()['grp1'].acemask == 86
    assert resc.get_acl()['grp2'].acemask == 95
    assert resc.get_acl()['grp3'].acemask == 9

    assert resc.user_can(User.find("user1"), "delete")       # admin
    assert resc.user_can(User.find("user2"), "write")        # grp2
    assert resc.user_can(User.find("user4"), "read")         # grp3
    assert not resc.user_can(User.find("user4"), "write")    # grp3
    
    assert str(resc.get_acl()['grp1']).__class__ == str
    resc.delete()
    
    coll.delete()

def test_dict():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    list_read = ['grp1']
    list_write = ['grp1']
    
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    
    # Read/Write resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    
    resc_dict = resc.full_dict(User.find("user1"))
    assert resc_dict['size'] == len(content)
    assert 'checksum' in resc_dict
    assert resc_dict['can_read']
    assert resc_dict['can_write']
    assert resc_dict['uuid'] == resc.uuid
    
    resc_dict = resc.simple_dict(User.find("user1"))
    assert resc_dict['name'] == resc_name
    assert resc_dict['is_reference'] == False
    assert not 'checksum' in resc_dict
    assert resc_dict['can_read']
    assert resc_dict['can_write']
    assert resc_dict['id'] == resc.uuid
    
    assert resc.simple_dict() == resc.to_dict()
    
    resc.delete()
    coll.delete()


def test_checksum():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()

    # Checksum passed at creation 
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid),
                           checksum=chk)
    resc = Resource.find(resc.path)
    assert resc.get_checksum() == chk
    resc.delete()

    # Checksum passed with set_checksum()
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.set_checksum(chk)
    resc = Resource.find(resc.path)
    assert resc.get_checksum() == chk
    resc.delete()

    # Checksum with a loss of DO
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid),
                           checksum=chk)
    resc = Resource.find(resc.path)
    # Simulate a loss of object (should never happens)
    resc.obj = None
    assert resc.get_checksum() == chk
    # Simulate a complete loss of object (should never happens)
    resc.obj = None
    DataObject.delete_id(do.uuid)
    assert resc.get_checksum() == None
    resc.delete()

    # Checksum for a reference
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr",
                           checksum=chk)
    resc = Resource.find(resc.path)
    # Checksum not stored for references
    assert resc.get_checksum() == None
    resc.delete()

    # Checksum for a reference with set_checksum()
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr")
    resc.set_checksum(chk)
    resc = Resource.find(resc.path)
    # Checksum not stored for references
    assert resc.get_checksum() == None
    resc.delete()

    # Checksum passed with set_checksum() with loss of data object
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    # Simulate a loss of object (should never happens)
    resc.obj = None
    resc.set_checksum(chk)
    resc = Resource.find(resc.path)
    assert resc.get_checksum() == chk
    resc.delete()

    # Checksum passed with set_checksum() with full loss of data object
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    # Simulate a loss of object (should never happens)
    resc.obj = None
    DataObject.delete_id(do.uuid)
    resc.set_checksum(chk)
    resc = Resource.find(resc.path)
    assert resc.get_checksum() == None
    resc.delete()

    coll.delete()


def test_metadata():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()
    
    metadata = {
        "test" : "val",
        "test_json" : '["t", "e", "s", "t"]'
    }
    
    # Checksum passed at creation 
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid),
                           metadata=metadata)
    resc = Resource.find(resc.path)
    
    meta_dict = resc.get_metadata()
    assert json.loads(meta_dict['test'])['json'] == metadata['test']
    assert json.loads(meta_dict['test_json'])['json'] == metadata['test_json']
    
    assert resc.get_metadata_key('test') == metadata['test']
    resc.delete()
    
    # Checksum passed at creation 
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid),
                           mimetype="text/plain")
    resc = Resource.find(resc.path)
    
    assert resc.get_mimetype() == "text/plain"
    # Simulate a loss of object (should never happens)
    resc.obj = None
    assert resc.get_mimetype() == "text/plain"
    # Simulate a complete loss of object (should never happens)
    resc.obj = None
    DataObject.delete_id(do.uuid)
    assert resc.get_mimetype() == None
    resc.delete()
    
    # Mimetype for a reference
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr",
                           mimetype="text/plain")
    resc = Resource.find(resc.path)
    # Mimetype stored in the tree entry
    assert resc.get_mimetype() == "text/plain"
    
    resc.delete()
    coll.delete()


def test_path():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()
    
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc = Resource.find(resc.path)
    
    assert resc.get_path() == "{}/{}".format(coll.path, resc_name)
    
    resc.delete()
    coll.delete()

def test_size():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()
    
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc = Resource.find(resc.path)
    
    assert resc.get_size() == len(content)
    # Simulate a loss of object (should never happens)
    resc.obj = None
    assert resc.get_size() == len(content)
    # Simulate a complete loss of object (should never happens)
    resc.obj = None
    DataObject.delete_id(do.uuid)
    assert resc.get_size() == 0
    resc.delete()
    
    # Size for a reference
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr",
                           mimetype="text/plain")
    resc = Resource.find(resc.path)
    # Size stored in the tree entry
    assert resc.get_size() == 0
    resc.delete()

    coll.delete()


def test_update():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()
    
    metadata = {
        "test" : "val",
        "test_json" : '["t", "e", "s", "t"]'
    }
    
    # Simple update
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.update(mimetype="text/plain")
    resc = Resource.find(resc.path)
    assert resc.get_mimetype() == "text/plain"
    resc.delete()
    
    # update with metadata and a username for notification
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc.update(username="test1", metadata=metadata)
    resc = Resource.find(resc.path)
    assert resc.get_metadata_key('test') == metadata['test']
    resc.delete()
    
    # Update with a change of url (new dataObject)
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    DataObject.delete_id(do.uuid)
    do = DataObject.create(content.encode())
    resc.update(url = "{}{}".format(PROTOCOL_CASSANDRA, do.uuid))
    resc = Resource.find(resc.path)
    assert resc.get_size() == len(content)
    resc.delete()
    
    # Update for a reference
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr")
    resc.update(mimetype="text/plain")
    resc = Resource.find(resc.path)
    # Mimetype stored in the tree entry
    assert resc.get_mimetype() == "text/plain"
    resc.delete()
    
    
    coll.delete()



