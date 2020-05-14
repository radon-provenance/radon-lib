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
import uuid

from radon.models.collection import Collection
from radon.models.resource import Resource
from radon.models.user import User

from radon import cfg
from radon.models import (
    connect,
    destroy,
    initialise,
    sync
)

from radon.models.errors import(
    CollectionConflictError,
    NoSuchCollectionError,
    ResourceConflictError,
)


# Populated by create_test_env
TEST_KEYSPACE = "radon_pytest"


TEMP_KEYSPACE = "pytest_tmp"


def test_collection():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    coll1 = Collection.create(uuid.uuid4().hex)
    coll2 = Collection.create(uuid.uuid4().hex, container=coll1.path)
    meta_dict = {"meta": "val"}
    coll3 = Collection.create(uuid.uuid4().hex, metadata=meta_dict)
    assert coll3.get_cdmi_metadata() == meta_dict
    coll4 = Collection.create(uuid.uuid4().hex, username="test")
 
    with pytest.raises(NoSuchCollectionError):
        coll = Collection.create(uuid.uuid4().hex, container="unknown")

    with pytest.raises(ResourceConflictError):
        coll = Collection.create("test.txt", container="/")
        coll.delete()

    with pytest.raises(CollectionConflictError):
        coll = Collection.create(coll1.name, container="/")

    coll1.delete()
    coll2.delete()
    coll3.delete()
    coll4.delete()


def test_create_root():
    cfg.dse_keyspace = TEMP_KEYSPACE
    initialise()
    sync()

    Collection.create_root()
    coll = Collection.find("/")
    assert not coll == None
    assert coll.path == "/"

    destroy(TEMP_KEYSPACE)


def test_delete_all():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    coll1_name = uuid.uuid4().hex
    coll1 = Collection.create(coll1_name)
    coll2 = Collection.create(uuid.uuid4().hex, container=coll1.path)
    coll3 = Collection.create(uuid.uuid4().hex, container=coll1.path)
    coll4 = Collection.create(uuid.uuid4().hex, container=coll2.path)
    coll5 = Collection.create(uuid.uuid4().hex, container=coll4.path)
    resc = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    
    Collection.delete_all(coll1.path)
    assert Collection.find(coll1_name) == None

    Collection.delete_all("/{}".format(uuid.uuid4().hex))


def test_get_root():
    cfg.dse_keyspace = TEMP_KEYSPACE
    initialise()
    sync()

    coll = Collection.get_root()
    assert coll.path == "/"
    coll.delete()

    destroy(TEMP_KEYSPACE)


def test_create_acl():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    list_read = ['grp1']
    list_write = ['grp1']
    user1 = User.find("user1")

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)

    # Test Read/Write ACL
    coll.create_acl_list(list_read, list_write)
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl()
    assert acl['grp1'].acemask == 95
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, list_write)
    assert coll.get_authorized_actions(user1) == {'edit', 'write', 'delete', 'read'}

    # Test Read ACL
    coll.create_acl_list(list_read, [])
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl()
    assert acl['grp1'].acemask == 9
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, [])
    assert coll.get_authorized_actions(user1) == {'read'}

    # Test Write ACL
    coll.create_acl_list([], list_write)
    coll = Collection.find('/{}'.format(coll_name))
    acl = coll.get_acl()
    assert acl['grp1'].acemask == 86
    acl_list = coll.get_acl_list()
    assert acl_list == ([], list_write)
    assert coll.get_authorized_actions(user1) == {'edit', 'write', 'delete'}

    # Test the ACL metadata returned as a dictionary
    acl = coll.get_acl_metadata()
    assert acl['cdmi_acl'][0]['acetype'] == "ALLOW"
    assert acl['cdmi_acl'][0]['identifier'] == "grp1"
    assert acl['cdmi_acl'][0]['aceflags'] == 'CONTAINER_INHERIT, OBJECT_INHERIT'
    assert acl['cdmi_acl'][0]['acemask'] == 'DELETE_SUBCONTAINER, WRITE_METADATA, ADD_SUBCONTAINER, ADD_OBJECT'

    # Delete the new collection
    coll.delete()

    # Check authorized actions for root
    coll = Collection.find("/")
    print (coll.is_root, coll.name)
    assert coll.get_authorized_actions(user1) == {'read'}

    # Check the inheritance of the ACL 
    # (from a collection to its parents, root in this test)
    coll = Collection.create(coll_name)
    assert coll.get_authorized_actions(user1) == {'read'}
    coll.delete()


def test_update_acl():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    list_read = ['grp1']
    list_write = ['grp1']

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll = Collection.create(coll_name)
    coll.update_acl_list(list_read, list_write)

    coll = Collection.find('/{}'.format(coll_name))
    acl_list = coll.get_acl_list()
    assert acl_list == (list_read, list_write)
    
    coll.delete()


def test_get_child():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create(coll_name)
    coll2 = Collection.create(uuid.uuid4().hex, container=coll1.path)
    coll3 = Collection.create(uuid.uuid4().hex, container=coll1.path)
    coll4 = Collection.create(uuid.uuid4().hex, container=coll1.path)
    resc1 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")
    resc2 = Resource.create(coll1.path, uuid.uuid4().hex, url="http://www.google.fr")

    coll_childs, resc_childs = coll1.get_child()
    
    assert set(coll_childs) == set([coll2.name, coll3.name, coll4.name])
    assert set(resc_childs) == set([resc1.name, resc2.name])
    assert coll1.get_child_resource_count() == 2

    coll1.delete()

def test_metadata():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create(coll_name)
    metadata = {
        "test" : "val",
        "test_json" : '["t", "e", "s", "t"]'
    }
    coll1.update(metadata=metadata)
    
    coll1 = Collection.find("/{}".format(coll_name))
    assert coll1.get_list_metadata() == [('test', 'val'), ('test_json', '["t", "e", "s", "t"]')]
    assert coll1.get_metadata_key("test_json") == '["t", "e", "s", "t"]'


def test_to_dict():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create(coll_name)
    coll_dict = coll1.to_dict()
    assert coll_dict['id'] == coll1.uuid
    assert coll_dict['name'] == coll_name
    assert coll_dict['path'] == "/{}".format(coll_name)
    
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
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    # Create a new collection with a random name
    coll_name = uuid.uuid4().hex
    coll1 = Collection.create(coll_name)
    
    coll1.update(username="user1")
    
    coll1.delete()

if __name__ == "__main__":
    test_to_dict()


