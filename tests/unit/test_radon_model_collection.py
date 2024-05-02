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
import json

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


def teardown_module(module):
    destroy()


def test_collection():
    grp_name = uuid.uuid4().hex
    grp = Group.create(name=grp_name)
    coll = Collection.create("/", uuid.uuid4().hex)
    
    coll1 = Collection.create(coll.path, uuid.uuid4().hex)
    
    coll2 = Collection.create(coll1.path, uuid.uuid4().hex)
    meta_dict = {"meta": "val"}
    coll3 = Collection.create(coll.path, uuid.uuid4().hex, metadata=meta_dict)
    assert coll3.get_cdmi_user_meta() == meta_dict
    coll4 = Collection.create(coll.path, uuid.uuid4().hex, sender="test")
    # test if name ends with '/'
    coll5 = Collection.create(coll.path, uuid.uuid4().hex + "/", read_access=[grp_name],
                              write_access=[grp_name])
   
    coll_err = Collection.create("unknown", uuid.uuid4().hex)
    assert coll_err == None
    
    test_resc = uuid.uuid4().hex
    r = Resource.create(coll.path, test_resc)

    coll_err = Collection.create(coll.path, test_resc)
    assert coll_err == None

    coll_err = Collection.create(coll.path, coll1.name)
    assert coll_err == None

    coll.delete()
    grp.delete()


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
    coll = Collection.create("/", uuid.uuid4().hex)
    
    coll1_name = uuid.uuid4().hex
    coll1 = Collection.create(coll.path, coll1_name)
    coll2 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll3 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll4 = Collection.create(coll2.path, uuid.uuid4().hex)
    coll5 = Collection.create(coll4.path, uuid.uuid4().hex)
    resc1 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    coll1.delete()
    assert Collection.find(coll1_name) == None
    coll.delete()
    
    # Delete root
    coll_root = Collection.get_root()
    coll_root.delete()


def test_find():
    coll = Collection.create("/", uuid.uuid4().hex)
    
    coll1 = Collection.create(coll.path, "a")
    assert Collection.find("{}a".format(coll.path)) == None
    assert Collection.find("{}a/".format(coll.path)) != None
    assert Collection.find("{}a/".format(coll.path), 1) == None
    coll.delete()


def test_get_root():
    coll = Collection.get_root()
    assert coll.path == "/"


def test_create_acl():
    user1_login = uuid.uuid4().hex
    user2_login = uuid.uuid4().hex
    user1_pwd = uuid.uuid4().hex
    user2_pwd = uuid.uuid4().hex
    grp_name = uuid.uuid4().hex
    
    grp = Group.create(name=grp_name)
    u1 = User.create(login=user1_login, password=user1_pwd, administrator=True)
    u2 = User.create(login=user2_login, password=user2_pwd, administrator=False)
    grp.add_users([user2_login])
    
    list_read = [grp_name]
    list_write = [grp_name]

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex +"/"
    coll = Collection.create('/', coll_name)

    # Test Read/Write ACL
    coll.create_acl_list(list_read, list_write)
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl_dict()
    assert acl[grp_name].acemask == 95
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, list_write)
    assert coll.get_authorized_actions(u2) == {'edit', 'write', 'delete', 'read'}
    cdmi_acl = coll.get_acl_metadata()
    assert 'cdmi_acl' in cdmi_acl
   
    # Test Read ACL
    coll.create_acl_list(list_read, [])
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl_dict()
    assert acl[grp_name].acemask == 9
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, [])
    assert coll.get_authorized_actions(u2) == {'read'}
   
    # Test Write ACL
    coll.create_acl_list([], list_write)
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl_dict()
    assert acl[grp_name].acemask == 86
    acl_list = coll.get_acl_list()
    assert acl_list == ([], list_write)
    assert coll.get_authorized_actions(u2) == {'edit', 'write', 'delete'}

#     # Test the ACL metadata returned as a dictionary
#     acl = coll.get_acl_metadata()
#     assert acl['cdmi_acl'][0]['acetype'] == "ALLOW"
#     assert acl['cdmi_acl'][0]['identifier'] == "grp1"
#     assert acl['cdmi_acl'][0]['aceflags'] == 'CONTAINER_INHERIT, OBJECT_INHERIT'
#     assert acl['cdmi_acl'][0]['acemask'] == 'DELETE_SUBCONTAINER, WRITE_METADATA, ADD_SUBCONTAINER, ADD_OBJECT'

    coll.delete()

    # Check authorized actions for root
    coll = Collection.find("/")
    assert coll.get_authorized_actions(u1) == {'edit', 'write', 'delete', 'read'}

    # Check the inheritance of the ACL 
    # (from a collection to its parents, root in this test)
    coll = Collection.create('/', coll_name)
    assert coll.get_authorized_actions(u2) == {'read'}
    coll.delete()
    
    assert coll.get_authorized_actions(None) == set([])
    grp.delete()
    u1.delete()
    u2.delete()


def test_create_acl_fail(mocker):
    grp_name = uuid.uuid4().hex
    grp = Group.create(name=grp_name)
    user_login = uuid.uuid4().hex
    user_pwd = uuid.uuid4().hex
    
    user = User.create(login=user_login, password=user_pwd, administrator=False)
    grp.add_users([user_login])
    
    list_read = [grp_name]
    list_write = [grp_name]

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
    assert coll.get_authorized_actions(user) == set([])

    grp.delete()
    user.delete()


def test_update_acl():
    grp_name = uuid.uuid4().hex
    grp = Group.create(name=grp_name)
    
    list_read = [grp_name]
    list_write = [grp_name]


    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    
    cdmi_acl = [
        {'identifier': grp_name,
         'acetype': 'ALLOW',
         'aceflags': "INHERITED",
         'acemask': "READ"
        }
    ]
    coll.update_acl_cdmi(cdmi_acl)

    coll = Collection.find('/{}/'.format(coll_name))
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, [])

    coll.delete()
    grp.delete()


def test_update_acl_via_metadata():
    grp_name = uuid.uuid4().hex
    grp = Group.create(name=grp_name)
    
    list_read = [grp_name]
    list_write = [grp_name]


    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    
    metadata = {
        "cdmi_acl": [
            {'identifier': grp_name,
             'acetype': 'ALLOW',
             'aceflags': "INHERITED",
             'acemask': "READ"
            }
        ]
    }
    coll.update(metadata=metadata)

    coll = Collection.find('/{}/'.format(coll_name))
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, [])

    coll.delete()
    grp.delete()


def test_update_acl_list():
    grp_name = uuid.uuid4().hex
    grp = Group.create(name=grp_name)
    
    list_read = [grp_name]
    list_write = [grp_name]

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll = Collection.create('/', coll_name)
    
    coll.update_acl_list(list_read, list_write)

    coll = Collection.find('/{}/'.format(coll_name))
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, list_write)

    coll.delete()
    grp.delete()


def test_get_acl():
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
    
    # Change the name of the node to simulate an error
    coll1.node.name = uuid.uuid4().hex
    assert coll1.get_acl_list() == ([], [])
    
    coll1.delete()


def test_get_child():
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
    coll2 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll3 = Collection.create(coll1.path, uuid.uuid4().hex)
    coll4 = Collection.create(coll1.path, uuid.uuid4().hex)
    resc1 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    resc2 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    coll_root = Collection.get_root()

    coll_childs, resc_childs = coll1.get_child()
    
    assert set(coll_childs) == set([coll2.name, coll3.name, coll4.name])
    assert set(resc_childs) == set([resc1.get_name(), resc2.get_name()])
    assert coll1.get_child_resource_count() == 2
    
    
    root_coll_childs, root_resc_childs = coll_root.get_child()
    assert set(root_coll_childs) == set([coll1.name])
    assert set(root_resc_childs) == set([])
    
    coll_root = Collection.find("/")
    # Test for a resource where the url has been lost somehow
    resc3 = Resource.create(coll1.path, uuid.uuid4().hex)
    resc3.update(object_url=None)
    resc3 = Resource.find(resc3.path)
    coll_childs, resc_childs = coll1.get_child()
    
    assert set(resc_childs) == set([resc1.get_name(), resc2.get_name(), resc3.get_name(),])

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

    coll1.delete()


def test_to_dict():
    user1_login = uuid.uuid4().hex
    user2_login = uuid.uuid4().hex
    user1_pwd = uuid.uuid4().hex
    user2_pwd = uuid.uuid4().hex
    u1 = User.create(login=user1_login, password=user1_pwd, administrator=True)
    u2 = User.create(login=user2_login, password=user2_pwd, administrator=False)
    
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
    coll_dict = coll1.to_dict()
    assert coll_dict['uuid'] == coll1.uuid
    assert coll_dict['name'] == coll_name + '/'
    assert coll_dict['path'] == "/{}/".format(coll_name)

    # Specify a user to get ACL
    # user 1 is admin, he can do everything
    coll_dict = coll1.to_dict(u1)
    assert coll_dict['can_read'] == True
    assert coll_dict["can_write"] == True
    assert coll_dict["can_edit"] == True
    assert coll_dict["can_delete"] == True

    # user 2 should have limited access
    coll_dict = coll1.to_dict(u2)
    assert coll_dict['can_read'] == True
    assert coll_dict["can_write"] == False
    assert coll_dict["can_edit"] == False
    assert coll_dict["can_delete"] == False

    coll1.delete()
    u1.delete()
    u2.delete()
 
 
def test_update():
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create('/', coll_name)
     
    coll1.update(sender="user1")
     
    coll1.delete()


if __name__ == "__main__":
    setup_module()
    # test_collection()
    # test_create_acl()
    test_get_child()
    #test_get_acl()
    #test_update_acl_list()
    destroy()

