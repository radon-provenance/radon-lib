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


def test_resource():
    coll_name = uuid.uuid4().hex
 
    with pytest.raises(NoSuchCollectionError):
        resc = Resource.create("/{}".format(coll_name), uuid.uuid4().hex)

    coll = Collection.create("/", coll_name)
    do = create_data_object()
    
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    assert str(resc) == resc.path
    assert resc.get_name() == resc_name
    
    with pytest.raises(ResourceConflictError):
        resc = Resource.create(coll.path, resc_name)

    resc.delete()
    # Check resource is gone
    resc = Resource.find(resc.path)
    assert resc == None

    do = create_data_object()
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc.delete()
    
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr")
    assert resc.get_name() == resc_name + '?'
    resc.delete()
    
    # Check deleting resource also deleted data object
    do = DataObject.find(do.uuid)
    assert do == None
    
    do = create_data_object()
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid),
                           size=do.size)
    assert resc.get_size() == do.size
    
    resc.delete()
    coll.delete()


def test_acl():
    list_read = [GRP1_NAME]
    list_write = [GRP1_NAME]

    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    myFactory = Faker()
    content = myFactory.text()
    
    # Read/Write resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    
    assert resc.get_acl_dict()[GRP1_NAME].acemask == 95     # read/write
    assert resc.get_authorized_actions(User.find(USR2_NAME)) == {'delete', 'read', 'edit', 'write'}
    read_access, write_access = resc.get_acl_list()
    assert read_access == [GRP1_NAME]
    assert write_access == [GRP1_NAME]
    cdmi_acl = resc.get_acl_metadata()
    assert 'cdmi_acl' in cdmi_acl
    
    cdmi_acl = [
        {'identifier': GRP1_NAME,
         'acetype': 'ALLOW',
         'aceflags': "INHERITED",
         'acemask': "READ"
        }
    ]
    resc.update_acl_cdmi(cdmi_acl)
    
    resc.delete()


    # Read resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc.create_acl_list(list_read, [])
    resc = Resource.find(resc.path)
    assert resc.get_acl_dict()[GRP1_NAME].acemask == 9      # read
    assert resc.get_authorized_actions(User.find(USR2_NAME)) == {'read'}
    read_access, write_access = resc.get_acl_list()
    assert read_access == [GRP1_NAME]
    assert write_access == []
    resc.delete()

    # Write resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc.create_acl_list([], list_write)
    resc = Resource.find(resc.path)
    assert resc.get_acl_dict()[GRP1_NAME].acemask == 86      # write
    assert resc.get_authorized_actions(User.find(USR2_NAME)) == {'edit', 'delete', 'write'}
    read_access, write_access = resc.get_acl_list()
    assert read_access == []
    assert write_access == [GRP1_NAME]
    
    
    resc.delete()

    # Resource stored in Cassandra, acl inherited from root
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    assert resc.get_authorized_actions(User.find(USR2_NAME)) == {"read"}
    resc.delete()

    # Read/Write resource stored as a reference
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, url = "http://www.google.fr")
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find(resc.path)
    assert resc.get_acl_dict()[GRP1_NAME].acemask == 95
    assert resc.get_authorized_actions(User.find(USR2_NAME)) == {'delete', 'read', 'edit', 'write'}
    resc.delete()

   
    
    coll.delete()


def test_chunk_content():
    coll_name = uuid.uuid4().hex
    coll = Collection.create("/", coll_name)
    myFactory = Faker()
    content = myFactory.text()
    do = DataObject.create(content.encode())
    
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    data = []
    for chk in resc.chunk_content():
        data.append(chk)
    res = b"".join([s for s in data])
    assert res == content.encode()
    
    resc.obj = None
    assert resc.chunk_content() == None
    
    TEST_URL = "http://www.google.fr"
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, url = TEST_URL)
    data = []
    for chk in resc.chunk_content():
        data.append(chk)
    res = b"".join(data)
    
    assert res
    
    coll.delete()


def test_dict():
    coll_name = uuid.uuid4().hex
    coll = Collection.create("/", coll_name)
    myFactory = Faker()
    content = myFactory.text()

    # Read/Write resource stored in Cassandra
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc = Resource.find(resc.path)
    
    resc_dict = resc.full_dict(User.find(USR1_NAME))
    assert resc_dict['size'] == len(content)
    assert resc_dict['can_read']
    assert resc_dict['can_write']
    assert resc_dict['uuid'] == resc.uuid
    
    resc_dict = resc.simple_dict(User.find(USR1_NAME))
    
    assert resc_dict['name'] == resc_name
    assert resc_dict['is_reference'] == False
    assert resc_dict['can_read']
    assert resc_dict['can_write']
    assert resc_dict['id'] == resc.uuid
    
    assert resc.simple_dict() == resc.to_dict()
    
    resc.delete()
    coll.delete()



def test_metadata():
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()
    
    metadata = {
        "test" : "val",
        "test_list" : ['t', 'e', 's', 't']
    }
    
    # Checksum passed at creation 
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid),
                           metadata=metadata)
    resc = Resource.find(resc.path)
    
    meta_dict = resc.get_cdmi_user_meta()

    assert meta_dict['test'] == metadata['test']
    assert meta_dict['test_list'] == metadata['test_list']
    
    sys_meta = resc.get_cdmi_sys_meta()
    assert "radon_create_ts" in sys_meta
    assert "radon_modify_ts" in sys_meta
     
    resc.delete()
     
    # Checksum passed at creation 
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid),
                           mimetype="text/plain")
    resc = Resource.find(resc.path)
     
    assert resc.get_mimetype() == "text/plain"
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
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()
    
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc = Resource.find(resc.path)
    
    assert resc.path == "{}{}".format(coll.path, resc_name)
    assert resc.get_path() == "{}{}".format(coll.path, resc_name)
    
    resc.delete()
    coll.delete()


def test_size():
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    myFactory = Faker()
    content = myFactory.text()
    chk = hashlib.sha224(content.encode()).hexdigest()
    
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc = Resource.find(resc.path)
    assert resc.get_size() == len(content)
    
    resc.obj = None
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
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
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
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc.update(mimetype="text/plain")
    resc = Resource.find(resc.path)
    assert resc.get_mimetype() == "text/plain"
    resc.delete()
    
    # update with metadata and a username for notification
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    resc.update(username="test1", metadata=metadata)
    resc = Resource.find(resc.path)
    assert resc.get_cdmi_user_meta()['test'] == metadata['test']
    resc.delete()
    
    # Update with a change of url (new dataObject)
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create(coll.path, resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    DataObject.delete_id(do.uuid)
    do = DataObject.create(content.encode())
    resc.update(object_url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
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


def test_find_fail():
    resc = Resource.find("/{}".format(uuid.uuid4().hex), "0")
    assert resc == None


def test_create_acl_fail(mocker):
    list_read = [GRP1_NAME]
    list_write = [GRP1_NAME]
    myFactory = Faker()
    content = myFactory.text()

    # Create a new resource with a random name
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create('/', resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    
    mocker.patch('radon.model.resource.acemask_to_str', return_value="wrong_oper")
    resc.create_acl_list(list_read, list_write)
    resc = Resource.find('/{}'.format(resc_name))
    # Test get_acl_list wrong operation name
    acl_list = resc.get_acl_list()
    assert acl_list == ([], [])

    resc.delete()


def test_user_can():
    myFactory = Faker()
    content = myFactory.text()

    # Create a new resource with a random name
    do = DataObject.create(content.encode())
    resc_name = uuid.uuid4().hex
    resc = Resource.create('/', resc_name, 
                           url = "{}{}".format(cfg.protocol_cassandra, do.uuid))
    
    usr1 = User.find(USR1_NAME)
    usr2 = User.find(USR2_NAME)
    
    # usr1 should be admin
    assert resc.user_can(usr1, "read")
    # usr2 should not be admin, on root collection, only read
    assert resc.user_can(usr2, "read")
    assert not resc.user_can(usr2, "write")
    
    resc.delete()


def test_nourlresource():
    name = uuid.uuid4().hex
    node = TreeNode.create(
        container='/',
        name=name
    )
    resc = NoUrlResource(node)
    
    assert resc.get_name() == name + '#'
    assert resc.get_size() == 0
    assert resc.chunk_content() == ""
    with pytest.raises(NotImplementedError):
        resc.put("test")


def test_put():
    myFactory = Faker()
    content = myFactory.text()

    # Create a new resource with a random name
    resc_name = uuid.uuid4().hex
    resc = Resource.create('/', resc_name)
    resc.put(content.encode())
    
    assert resc.get_size() == len(content)
    
    # Force small chunk size for testing
    cfg.chunk_size = 4
    fh = io.BytesIO(content.encode())
    resc.put(fh)
    
    assert resc.get_size() == len(content)
    
    resc.delete()
    
    resc = Resource.create('/', resc_name, url = "http://www.google.fr",
                           mimetype="text/plain")
    with pytest.raises(NotImplementedError):
        resc.put("test")
    
    resc.delete()



