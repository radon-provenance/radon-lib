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

from radon import cfg
from radon.models import (
    destroy,
    initialise,
    sync
)

from radon.models.errors import (
    GroupConflictError,
    UserConflictError
)
from radon.models.group import Group
from radon.models.user import User
from radon.models.collection import Collection
from radon.models.data_object import DataObject
from radon.models.resource import Resource

TEST_KEYSPACE = "radon_pytest"

GROUPS = ["grp1", "grp2", "grp3", "grp4"]

USERS = [("user1", "user1@test.com", "user1", True, ["grp1"]),
         ("user2", "user2@test.com", "user2", False, ["grp2"]),
         ("user3", "user3@test.com", "user3", False, ["grp1", "grp2"]),
         ("user4", "user4@test.com", "user4", False, ["grp3"])]


def create_keyspace():
    """Create a set of objects in a radon keyspace created for the test
    """
    cfg.dse_keyspace = TEST_KEYSPACE

    # Initialise connection and create keyspace
    initialise()
    # Create tables
    sync()


def create_groups():
    for group_name in GROUPS:
        try:
            grp = Group.create(name=group_name)
            print ("group '%s' created (uuid='%s')" % (grp.name, grp.uuid))
        except GroupConflictError:
            print ("group '%s' already exists" % (group_name))


def create_users():
    for (user_name, email, password, administrator, groups) in USERS:
        try:
            user = User.create(name=user_name, email=email,
                               password=password, administrator=administrator,
                               groups=groups)
            print ("user '%s' created (uuid='%s')" % (user.name, user.uuid))
        except UserConflictError:
            print ("user '%s' already exists" % (user_name))


def create_collection(parent, collection):
    try:
        Collection.create(parent, collection)
    except: # Collection already exists quite likely
        print("collection {}/{} already exists".format(parent, collection))


def create_root():
    try:
        Collection.create_root()
    except: # Collection already exists quite likely
        print("Root collection already exists")


def create_collections():
    create_root()
    create_collection("/", "coll1")
    create_collection("/coll1","coll11")
    create_collection("/coll1","coll12")
    create_collection("/coll1","coll13")
    create_collection("/", "coll2")
    create_collection("/coll2","coll21")
    create_collection("/coll2","coll22")
    create_collection("/coll2","coll23")
    create_collection("/", "coll3")
    create_collection("/coll3","coll31")
    create_collection("/coll3","coll32")
    create_collection("/coll3","coll33")


def create_resource(container, name, data):
    try:
        data_object = DataObject.create(data.encode())
        uuid = data_object.uuid
        url = "cassandra://{}".format(uuid)
        resource = Resource.create(
            name=name, 
            container=container,
            url=url,
            size=len(data)
        )
    except:
        print("resource {}/{} already exists".format(container, name))

def create_resources():
    create_resource("/", "test.txt", "test content")
    create_resource("/coll1", "test.txt", "test content in ./coll1")
    create_resource("/coll3/coll33", "test.txt", "test content in ./coll3/coll33")

    
if __name__ == "__main__":
    create_keyspace()
    create_groups()
    create_users()
    create_collections()
    create_resources()
    