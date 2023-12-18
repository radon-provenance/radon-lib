"""Copyright 2021

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
import uuid

from radon.model.config import cfg
from radon.model.collection import Collection
from radon.model.group import Group
from radon.model.resource import Resource
from radon.model.user import User
from radon.database import (
    connect,
    create_default_users,
    create_root,
    create_tables,
    destroy,
    initialise,
)

from radon.model.errors import(
    CollectionConflictError,
    NoSuchCollectionError,
    ResourceConflictError,
)


TEST_KEYSPACE = "test_keyspace"
TEST_URL = "http://www.google.fr"


def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_default_users()
    create_root()
    pwd = uuid.uuid4().hex
    email = uuid.uuid4().hex
    grp1 = Group.create(name="grp1")
    u1 = User.create(login="user1", password=pwd, email=email, administrator=True)
    u2 = User.create(login="user2", password=pwd, email=email, administrator=False)
    
    grp1.add_users(["user2"])
    
    try:
        coll = Collection.create("/", "1")
        coll = Collection.create("/1", "11")
        coll = Collection.create("/1", "12")
        coll = Collection.create("/1", "13")
        coll = Collection.create("/", "2")
        coll = Collection.create("/2", "21")
        coll = Collection.create("/2/21", "211")
        coll = Collection.create("/2/21", "212")
        r = Resource.create("/1/11", "a")
        r = Resource.create("/1/11", "b", url=TEST_URL)
        r = Resource.create("/1/12", "c")
        r = Resource.create("/1/13", "d")
        r = Resource.create("/2/21/211", "e")
        r = Resource.create("/2/21/212", "f")
        r = Resource.create("/", "g")
    except: # If collections or resources already exist
        pass


def teardown_module(module):
    destroy()


def test_collection():
    coll1 = Collection.create("/", uuid.uuid4().hex)
    coll2 = Collection.create(coll1.path, uuid.uuid4().hex)
    meta_dict = {"meta": "val"}
    coll3 = Collection.create("/", uuid.uuid4().hex, metadata=meta_dict)
    assert coll3.get_cdmi_user_meta() == meta_dict
    coll4 = Collection.create("/", uuid.uuid4().hex, creator="test")
    # test if name ends with '/'
    coll5 = Collection.create("/", uuid.uuid4().hex + "/")
   
    with pytest.raises(NoSuchCollectionError):
        Collection.create("unknown", uuid.uuid4().hex)
 
    r = Resource.find("/g")
  
    with pytest.raises(ResourceConflictError):
        coll = Collection.create("/", "g")
  
    with pytest.raises(CollectionConflictError):
        Collection.create("/", coll1.name)
 
    coll1.delete()
    coll2.delete()
    coll3.delete()
    coll4.delete()
    coll5.delete()


def test_delete_all():
    coll1_name = uuid.uuid4().hex
    coll1 = Collection.create("/", coll1_name)
    coll2 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll5 = Collection.create(coll2.path, uuid.uuid4().hex)
    resc1 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    Collection.delete_all("/{}/".format(coll1_name))
    
    assert Collection.find(coll1_name) == None
    
    assert Collection.delete_all("/unknown/") == None


def test_delete():
    coll1_name = uuid.uuid4().hex
    coll1 = Collection.create("/", coll1_name)
    coll2 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll3 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll4 = Collection.create(coll2.path, uuid.uuid4().hex)
    coll5 = Collection.create(coll4.path, uuid.uuid4().hex)
    resc1 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    coll1.delete()
    
    assert Collection.find(coll1_name) == None


def test_find():
    coll1 = Collection.create("/", "a")
    
    assert Collection.find("/a") == None
    assert Collection.find("/a/") != None
    assert Collection.find("/a/", 1) == None
    
    coll1.delete()
    
 
def test_get_root():
    coll = Collection.get_root()
    assert coll.path == "/"
    coll.delete()
 
 
def test_create_acl():
    list_read = ['grp1']
    list_write = ['grp1']
    user1 = User.find("user1")
    user2 = User.find("user2")

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex +"/"
    coll = Collection.create('/', coll_name)

    # Test Read/Write ACL
    coll.create_acl_list(list_read, list_write)
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl_dict()
    assert acl['grp1'].acemask == 95
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, list_write)
    assert coll.get_authorized_actions(user2) == {'edit', 'write', 'delete', 'read'}
    cdmi_acl = coll.get_acl_metadata()
    assert 'cdmi_acl' in cdmi_acl
   
    # Test Read ACL
    coll.create_acl_list(list_read, [])
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl_dict()
    assert acl['grp1'].acemask == 9
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, [])
    assert coll.get_authorized_actions(user2) == {'read'}
   
    # Test Write ACL
    coll.create_acl_list([], list_write)
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl_dict()
    assert acl['grp1'].acemask == 86
    acl_list = coll.get_acl_list()
    assert acl_list == ([], list_write)
    assert coll.get_authorized_actions(user2) == {'edit', 'write', 'delete'}

#     # Test the ACL metadata returned as a dictionary
#     acl = coll.get_acl_metadata()
#     assert acl['cdmi_acl'][0]['acetype'] == "ALLOW"
#     assert acl['cdmi_acl'][0]['identifier'] == "grp1"
#     assert acl['cdmi_acl'][0]['aceflags'] == 'CONTAINER_INHERIT, OBJECT_INHERIT'
#     assert acl['cdmi_acl'][0]['acemask'] == 'DELETE_SUBCONTAINER, WRITE_METADATA, ADD_SUBCONTAINER, ADD_OBJECT'

    coll.delete()

    # Check authorized actions for root
    coll = Collection.find("/")
    assert coll.get_authorized_actions(user1) == {'edit', 'write', 'delete', 'read'}

    # Check the inheritance of the ACL 
    # (from a collection to its parents, root in this test)
    coll = Collection.create('/', coll_name)
    assert coll.get_authorized_actions(user2) == {'read'}
    coll.delete()


def test_create_acl_fail(mocker):
    list_read = ['grp1']
    list_write = ['grp1']
    user2 = User.find("user2")

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex +"/"
    coll = Collection.create('/', coll_name)
    
    mocker.patch('radon.model.collection.acemask_to_str', return_value="wrong_oper")
    coll.create_acl_list(list_read, list_write)
    coll = Collection.find('/{}'.format(coll_name))
    # Test get_acl_list wrong operation name
    acl_list = coll.get_acl_list()
    assert acl_list == ([], [])

    coll.delete()
    
    # Check authorized actions for root
    coll = Collection.find("/")
    mocker.patch.object(Collection, 'get_acl_dict', return_value=None)
    assert coll.get_authorized_actions(user2) == set([])


def test_update_acl():
    list_read = ['grp1']
    list_write = ['grp1']

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    
    cdmi_acl = [
        {'identifier': 'grp1',
         'acetype': 'ALLOW',
         'aceflags': "INHERITED",
         'acemask': "READ"
        }
    ]
    coll.update_acl_cdmi("cdmi")

#     coll = Collection.find('/{}'.format(coll_name))
#     acl_list = coll.get_acl_list()
#     assert acl_list == (list_read, list_write)

    coll.delete()


def test_get_child():
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
    coll2 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll3 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll4 = Collection.create(coll1.path, uuid.uuid4().hex)
    resc1 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    resc2 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")

    coll_childs, resc_childs = coll1.get_child()
    
    assert set(coll_childs) == set([coll2.name, coll3.name, coll4.name])
    assert set(resc_childs) == set([resc1.get_name(), resc2.get_name()])
    assert coll1.get_child_resource_count() == 2
    
    coll_root = Collection.find("/")
    # Test for a resource where the url has been lost somehow
    resc3 = Resource.create(coll_root.path, uuid.uuid4().hex)
    resc3.update(object_url=None)
    resc3 = Resource.find(resc3.path)
    coll_childs, resc_childs = coll_root.get_child()
    assert set(coll_childs) == set(["1/", "2/", coll1.name])
    assert set(resc_childs) == set([resc3.get_name(), 'g'])

    coll1.delete()
    
    

def test_metadata():
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
    metadata = {
        "test" : "val",
        "test_json" : '["t", "e", "s", "t"]'
    }
    coll1.update(metadata=metadata)

    coll1 = Collection.find("/{}/".format(coll_name))
    assert coll1.get_list_user_meta() == [('test', 'val'), ('test_json', '["t", "e", "s", "t"]')]
    assert coll1.get_user_meta_key("test_json") == '["t", "e", "s", "t"]'
    
    sys_meta = coll1.get_cdmi_sys_meta()
    assert "radon_create_ts" in sys_meta
    assert "radon_modify_ts" in sys_meta
    
    #get_acl_metadata


def test_to_dict():
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
    coll_dict = coll1.to_dict()
    assert coll_dict['id'] == coll1.uuid
    assert coll_dict['name'] == coll_name + '/'
    assert coll_dict['path'] == "/{}/".format(coll_name)

    # Specify a user to get ACL
    # user 1 is admin, he can do everything
    coll_dict = coll1.to_dict(User.find("user1"))
    assert coll_dict['can_read'] == True
    assert coll_dict["can_write"] == True
    assert coll_dict["can_edit"] == True
    assert coll_dict["can_delete"] == True

    # user 2 should have limited access
    coll_dict = coll1.to_dict(User.find("user2"))
    assert coll_dict['can_read'] == True
    assert coll_dict["can_write"] == False
    assert coll_dict["can_edit"] == False
    assert coll_dict["can_delete"] == False

    coll1.delete()
 
 
def test_update():
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
     
    coll1.update(username="user1")
     
    coll1.delete()




