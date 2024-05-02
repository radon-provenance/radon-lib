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


import uuid

from radon.model.config import cfg
from radon.database import (
    connect,
    destroy,
    initialise,
    create_root,
    create_tables
)
from radon.model.payload import (
    PayloadCreateCollectionRequest,
    PayloadDeleteCollectionRequest,
    PayloadUpdateCollectionRequest,
    PayloadCreateGroupRequest,
    PayloadDeleteGroupRequest,
    PayloadUpdateGroupRequest,
    PayloadCreateResourceRequest,
    PayloadDeleteResourceRequest,
    PayloadUpdateResourceRequest,
    PayloadCreateUserRequest,
    PayloadDeleteUserRequest,
    PayloadUpdateUserRequest,
)
from radon.model.microservices import (
    Microservices,
    ERR_PAYLOAD_CLASS,
)
from radon.util import (
    merge
)
from radon.model.collection import Collection
from radon.model.group import Group
from radon.model.resource import Resource
from radon.model.user import User

TEST_KEYSPACE = "test_keyspace"



def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_root()



def teardown_module(module):
    destroy()


def test_create_collection():
    test_container = "/"
    test_name = uuid.uuid4().hex + "/"
    test_path = merge(test_container, test_name)
    
    ########### Wrong  payload class ############
    ok, coll, msg = Microservices.create_collection(
         {
             "obj" : {"path" : test_path},
             "meta" : {"sender": "pytest"}
         })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert coll == None
    
    ############ Missing key (path) ############
    ok, coll, msg = Microservices.create_collection(PayloadCreateCollectionRequest(
        {
            "obj" : {"container" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'path' is a required property"
    assert coll == None
    
    ############ Missing 'obj' information ############
    ok, coll, msg = Microservices.create_collection(PayloadCreateCollectionRequest(
        {
            "val" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert coll == None
    
    ############ Correct payload ############
    ok, coll, msg = Microservices.create_collection(PayloadCreateCollectionRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    coll_find = Collection.find(test_path)
    assert ok == True
    assert coll_find != None
    assert coll.path == test_path
    
    ############ Correct payload but already exist ############
    ok, coll, msg = Microservices.create_collection(PayloadCreateCollectionRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    coll_find = Collection.find(test_path)
    assert ok == False
    assert coll_find != None
    assert coll_find.path == test_path


def test_create_group():
    group_test = uuid.uuid4().hex
    
    ############ Wrong  payload class ############
    ok, grp, msg = Microservices.create_group(
        {
            "obj" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert grp == None

    ############ Missing key (name) ############
    ok, grp, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "obj" : {"login" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'name' is a required property"
    assert grp == None

    ############ Missing 'obj' information ############
    ok, grp, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "val" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert grp == None

    ############ Correct payload ############
    ok, grp, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "obj" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    grp_find = Group.find(group_test)
    assert ok == True
    assert grp_find != None
    assert grp.name == group_test
    assert grp.name == grp_find.name

    ############ Correct payload but already exist ############
    ok, grp, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "obj" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    grp_find = Group.find(group_test)
    assert ok == False
    assert grp_find != None
    assert grp_find.name == group_test


def test_create_resource():
    test_container = "/"
    test_name = uuid.uuid4().hex
    test_path = merge(test_container, test_name)
    test_name2 = uuid.uuid4().hex
    test_path2 = merge(test_container, test_name2)
    data = uuid.uuid4().hex
    
    ########### Wrong  payload class ############
    ok, resc, msg = Microservices.create_resource(
         {
             "obj" : {"path" : test_path},
             "meta" : {"sender": "pytest"}
         })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert resc == None
    
    ############ Missing key (path) ############
    ok, resc, msg = Microservices.create_resource(PayloadCreateResourceRequest(
        {
            "obj" : {"container" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'path' is a required property"
    assert resc == None
    
    ############ Missing 'obj' information ############
    ok, resc, msg = Microservices.create_resource(PayloadCreateResourceRequest(
        {
            "val" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert resc == None
    
    ############ Correct payload ############
    ok, resc, msg = Microservices.create_resource(PayloadCreateResourceRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    resc_find = Resource.find(test_path)
    assert ok == True
    assert resc_find != None
    assert resc.path == test_path
    
    ############ Correct payload but already exist ############
    ok, resc, msg = Microservices.create_resource(PayloadCreateResourceRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    resc_find = Resource.find(test_path)
    assert ok == False
    assert resc_find != None
    assert resc_find.path == test_path
    
    ############ Correct payload with data ############
    ok, resc, msg = Microservices.create_resource(PayloadCreateResourceRequest(
        {
            "obj" : {
                "path" : test_path2,
                "data" : data
            },
            "meta" : {"sender": "pytest"}
        }))
    resc_find2 = Resource.find(test_path2)
    assert ok == True
    assert resc_find2 != None
    assert resc_find2.path == test_path2
    assert resc_find2.get_size() == len(data)


def test_create_user():
    user_test = uuid.uuid4().hex
    password = uuid.uuid4().hex
    
    ############ Wrong  payload class ############
    ok, user, msg = Microservices.create_user(
        {
            "obj" : {"login" : user_test, "password" : password},
            "meta" : {"sender": "pytest"}
        })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert user == None

    ############ Missing key (login) ############
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"password" : password},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'login' is a required property"
    assert user == None

    ############ Missing 'obj' information ############
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "val" : {"login" : user_test},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert user == None

    ############ Correct payload ############
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"login" : user_test, "password" : password},
            "meta" : {"sender": "pytest"}
        }))
    user_find = User.find(user_test)
    assert ok == True
    assert user_find != None
    assert user.login == user_test
    assert user.login == user_find.login

    ############ Correct payload but already exist ############
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"login" : user_test, "password" : password},
            "meta" : {"sender": "pytest"}
        }))
    user_find = User.find(user_test)
    assert ok == False
    assert user_find != None
    assert user_find.login == user_test


################################################################################
################################################################################
################################################################################


def test_delete_collection():
    test_container = "/"
    test_name = uuid.uuid4().hex + "/"
    test_path = merge(test_container, test_name)
    
    # First create a collection that can be modified later
    ok, coll, msg = Microservices.create_collection(PayloadCreateCollectionRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    coll_find = Collection.find(test_path)
    if not coll:
        coll = coll_find # already exist
    assert coll_find != None
    
    ############ Wrong  payload class ############
    ok, coll, msg = Microservices.delete_collection(
        {
            "obj" : {"path" : test_container},
            "meta" : {"sender": "pytest"}
        })
    assert msg == ERR_PAYLOAD_CLASS
    assert coll == None

    ############ Missing key (path) ############
    ok, coll, msg = Microservices.delete_collection(PayloadDeleteCollectionRequest(
        {
            "obj" : {"container" : test_container},
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "'path' is a required property"
    assert coll == None

    ############ Coll not found ############
    ok, coll, msg = Microservices.delete_collection(PayloadDeleteCollectionRequest(
        {
            "obj" : {
                "path" : merge("/", uuid.uuid4().hex + "/"),
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "Collection not found"
    assert coll == None

    ############ Correct message ############
    ok, coll, msg = Microservices.delete_collection(PayloadDeleteCollectionRequest(
        {
            "obj" : {
                "path" : test_path,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "Collection deleted"
    assert coll != None


def test_delete_group():
    group_test = uuid.uuid4().hex
    
    # First create a group that can be modified later
    ok, grp, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "obj" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    group_find = Group.find(group_test)
    if not grp:
        grp = group_find # already exist
    assert group_find != None
    
    ############ Wrong  payload class ############
    ok, grp, msg = Microservices.delete_group(
        {
            "obj" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        })
    assert msg == ERR_PAYLOAD_CLASS
    assert grp == None

    ############ Missing key (login) ############
    ok, grp, msg = Microservices.delete_group(PayloadDeleteGroupRequest(
        {
            "obj" : {"login" : uuid.uuid4().hex},
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "'name' is a required property"
    assert grp == None

    ############ Group not found ############
    ok, grp, msg = Microservices.delete_group(PayloadDeleteGroupRequest(
        {
            "obj" : {
                "name" : uuid.uuid4().hex,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "Group not found"
    assert grp == None

    ############ Correct message ############
    ok, grp, msg = Microservices.delete_group(PayloadDeleteGroupRequest(
        {
            "obj" : {
                "name" : group_test,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "Group deleted"
    assert grp != None


def test_delete_resource():
    test_container = "/"
    test_name = uuid.uuid4().hex
    test_path = merge(test_container, test_name)
    
    # First create a resource that can be modified later
    ok, resc, msg = Microservices.create_resource(PayloadCreateResourceRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    resc_find = Resource.find(test_path)
    if not resc:
        resc = resc_find # already exist
    assert resc_find != None
    
    ############ Wrong  payload class ############
    ok, resc, msg = Microservices.delete_resource(
        {
            "obj" : {"path" : test_container},
            "meta" : {"sender": "pytest"}
        })
    assert msg == ERR_PAYLOAD_CLASS
    assert resc == None

    ############ Missing key (path) ############
    ok, resc, msg = Microservices.delete_resource(PayloadDeleteResourceRequest(
        {
            "obj" : {"container" : test_container},
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "'path' is a required property"
    assert resc == None

    ############ Resc not found ############
    ok, resc, msg = Microservices.delete_resource(PayloadDeleteResourceRequest(
        {
            "obj" : {
                "path" : merge("/", uuid.uuid4().hex),
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "Resource not found"
    assert resc == None

    ############ Correct message ############
    ok, resc, msg = Microservices.delete_resource(PayloadDeleteResourceRequest(
        {
            "obj" : {
                "path" : test_path,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "Resource deleted"
    assert resc != None


def test_delete_user():
    user_test = uuid.uuid4().hex
    password = uuid.uuid4().hex
    
    # First create a user that can be modified later
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"login" : user_test, "password" : password},
            "meta" : {"sender": "pytest"}
        }))
    user_find = User.find(user_test)
    if not user:
        user = user_find # already exist
    assert user_find != None
    
    ############ Wrong  payload class ############
    ok, user, msg = Microservices.delete_user(
        {
            "obj" : {"login" : user_test},
            "meta" : {"sender": "pytest"}
        })
    assert msg == ERR_PAYLOAD_CLASS
    assert user == None

    ############ Missing key (login) ############
    ok, user, msg = Microservices.delete_user(PayloadDeleteUserRequest(
        {
            "obj" : {"name" : uuid.uuid4().hex},
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "'login' is a required property"
    assert user == None

    ############ User not found ############
    ok, user, msg = Microservices.delete_user(PayloadDeleteUserRequest(
        {
            "obj" : {
                "login" : uuid.uuid4().hex,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "User not found"
    assert user == None

    ############ Correct message ############
    ok, user, msg = Microservices.delete_user(PayloadDeleteUserRequest(
        {
            "obj" : {
                "login" : user_test,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert msg == "User deleted"
    assert user != None


################################################################################
################################################################################
################################################################################


def test_update_collection():
    test_container = "/"
    test_name = uuid.uuid4().hex + "/"
    test_path = merge(test_container, test_name)
    
    # First create a collection that can be modified later
    ok, coll, msg = Microservices.create_collection(PayloadCreateCollectionRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    coll_find = Collection.find(test_path)
    if not coll:
        coll = coll_find # already exist
    assert coll_find != None
    
    # Create a user and a group to check ACLs
    test_user = uuid.uuid4().hex
    test_group = uuid.uuid4().hex
    password = uuid.uuid4().hex
    ok, grp, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "obj" : {"name" : test_group},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"login" : test_user,
                     "password" : password,
                     "groups" : [test_group]},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    
    ############ Wrong  payload class ############
    ok, coll, msg = Microservices.update_collection(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert coll == None

    ############ Missing key (path) ############
    ok, coll, msg = Microservices.update_collection(PayloadUpdateCollectionRequest(
        {
            "obj" : {"container" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'path' is a required property"
    assert coll == None
    
    ############ Missing 'obj' information ############
    ok, coll, msg = Microservices.update_collection(PayloadUpdateCollectionRequest(
        {
            "val" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert coll == None

    ############ Coll not found ############
    ok, coll, msg = Microservices.update_collection(PayloadUpdateCollectionRequest(
        {
            "obj" : {"path" : merge("/", uuid.uuid4().hex + "/")},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "Collection not found"
    assert coll == None

    ############ Correct message ############
    ok, coll, msg = Microservices.update_collection(PayloadUpdateCollectionRequest(
        {
            "obj" : {
                "path" : test_path,
                "read_access" : [test_group],
                "write_access" : [test_group],
                "metadata": {"test": "value"}
            },
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    assert msg == "Collection updated"
    assert coll != None
    assert coll.user_can(user, "read") == True
    assert coll.user_can(user, "write") == True
    assert coll.get_user_meta_key("test") == "value"


def test_update_group():
    group_test = uuid.uuid4().hex
    
    # First create a group that can be modified later
    ok, group, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "obj" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    group_find = Group.find(group_test)
    if not group:
        group = group_find # already exist
    assert group_find != None
    
    # Create a user we can add to the group
    test_user = uuid.uuid4().hex
    password = uuid.uuid4().hex
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"login" : test_user,
                     "password" : password},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    
    ############ Wrong  payload class ############
    ok, group, msg = Microservices.update_group(
        {
            "obj" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert group == None

    ############ Missing key (name) ############
    ok, group, msg = Microservices.update_group(PayloadUpdateGroupRequest(
        {
            "obj" : {"login" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'name' is a required property"
    assert group == None
    
    ############ Missing 'obj' information ############
    ok, group, msg = Microservices.update_group(PayloadUpdateGroupRequest(
        {
            "val" : {"name" : group_test},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert group == None

    ############ Group not found ############
    ok, group, msg = Microservices.update_group(PayloadUpdateGroupRequest(
        {
            "obj" : {"name" : uuid.uuid4().hex},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "Group not found"
    assert group == None

    ############ Correct message ############
    ok, group, msg = Microservices.update_group(PayloadUpdateGroupRequest(
        {
            "obj" : {
                "name" : group_test,
                "members": [test_user]
            },
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    assert msg == "Group updated"
    assert group != None
    assert test_user in group.get_members()


def test_update_resource():
    test_container = "/"
    test_name = uuid.uuid4().hex
    test_path = merge(test_container, test_name)
    
    # First create a resource that can be modified later
    ok, resc, msg = Microservices.create_resource(PayloadCreateResourceRequest(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    resc_find = Resource.find(test_path)
    if not resc:
        resc = resc_find # already exist
    assert resc_find != None
    
    # Create a user and a group to check ACLs
    test_user = uuid.uuid4().hex
    test_group = uuid.uuid4().hex
    password = uuid.uuid4().hex
    ok, grp, msg = Microservices.create_group(PayloadCreateGroupRequest(
        {
            "obj" : {"name" : test_group},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"login" : test_user,
                     "password" : password,
                     "groups" : [test_group]},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    
    ############ Wrong payload class ############
    ok, resc, msg = Microservices.update_resource(
        {
            "obj" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert resc == None

    ############ Missing key (path) ############
    ok, resc, msg = Microservices.update_resource(PayloadUpdateResourceRequest(
        {
            "obj" : {"container" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'path' is a required property"
    assert resc == None
    
    ############ Missing 'obj' information ############
    ok, resc, msg = Microservices.update_resource(PayloadUpdateResourceRequest(
        {
            "val" : {"path" : test_path},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert resc == None

    ############ Resc not found ############
    ok, resc, msg = Microservices.update_resource(PayloadUpdateResourceRequest(
        {
            "obj" : {"path" : merge("/", uuid.uuid4().hex + "/")},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "Resource not found"
    assert resc == None

    ############ Correct message ############
    ok, resc, msg = Microservices.update_resource(PayloadUpdateResourceRequest(
        {
            "obj" : {
                "path" : test_path,
                "read_access" : [test_group],
                "write_access" : [test_group],
                "metadata": {"test": "value"}
            },
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    assert msg == "Resource updated"
    assert resc != None
    assert resc.user_can(user, "read") == True
    assert resc.user_can(user, "write") == True
    assert resc.get_user_meta_key("test") == "value"


def test_update_user():
    user_test = uuid.uuid4().hex
    password = uuid.uuid4().hex
    new_password = uuid.uuid4().hex
    
    # First create a user that can be modified later
    ok, user, msg = Microservices.create_user(PayloadCreateUserRequest(
        {
            "obj" : {"login" : user_test, "password" : password},
            "meta" : {"sender": "pytest"}
        }))
    user_find = User.find(user_test)
    if not user:
        user = user_find # already exist
    assert user_find != None
    
    ############ Wrong  payload class ############
    ok, user, msg = Microservices.update_user(
        {
            "obj" : {"login" : user_test, "password" : new_password},
            "meta" : {"sender": "pytest"}
        })
    assert ok == False
    assert msg == ERR_PAYLOAD_CLASS
    assert user == None

    ############ Missing key (login) ############
    ok, user, msg = Microservices.update_user(PayloadUpdateUserRequest(
        {
            "obj" : {"password" : new_password},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'login' is a required property"
    assert user == None
    
    ############ Missing 'obj' information ############
    ok, user, msg = Microservices.update_user(PayloadUpdateUserRequest(
        {
            "val" : {"login" : user_test},
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "'obj' is a required property"
    assert user == None

    ############ User not found ############
    ok, user, msg = Microservices.update_user(PayloadUpdateUserRequest(
        {
            "obj" : {
                "login" : uuid.uuid4().hex,
                "email" : uuid.uuid4().hex,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert ok == False
    assert msg == "User not found"
    assert user == None

    ############ Correct message ############
    ok, user, msg = Microservices.update_user(PayloadUpdateUserRequest(
        {
            "obj" : {
                "login" : user_test,
                "email" : uuid.uuid4().hex,
                "fullname" : uuid.uuid4().hex,
                "administrator": False,
                "active" : True,
                "ldap" : False,
                "password" : uuid.uuid4().hex,
            },
            "meta" : {"sender": "pytest"}
        }))
    assert ok == True
    assert msg == "User updated"
    assert user != None


# if __name__ == "__main__":
#     setup_module()
#     # test_create_collection()
#     # test_create_group()
#     # test_create_resource()
#     # test_create_user()
#     # test_delete_collection()
#     # test_delete_group()
#     # test_delete_resource()
#     # test_delete_user()
#     #test_update_collection()
#     # test_update_group()
#     # test_update_resource()
#     # test_update_user()
#     destroy()

