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

from radon.models.errors import(
    CollectionConflictError,
    GroupConflictError,
    ModelError,
    NoSuchCollectionError,
    NoSuchResourceError,
    ResourceConflictError,
    UserConflictError
)


RESC_PATH = "/path/toresc"
COL_PATH = "/path"
GROUPNAME = "groupname"
USERNAME = "username"


def test_model():
    error = ModelError("Test error")
    assert error != None


def test_resource_conflict():
    error = ResourceConflictError(RESC_PATH)
    assert str(error) == "Resource already exists at '{}'".format(RESC_PATH)


def test_no_such_resource():
    error = NoSuchResourceError(RESC_PATH)
    assert str(error) == "Resource '{}' does not exist".format(RESC_PATH)


def test_no_collection_conflict():
    error = CollectionConflictError(COL_PATH)
    assert str(error) == "Container already exists at '{}'".format(COL_PATH)


def test_no_such_collection():
    error = NoSuchCollectionError(COL_PATH)
    assert str(error) == "Container '{}' does not exist".format(COL_PATH)


def test_group_conflict():
    error = GroupConflictError(GROUPNAME)
    assert str(error) == "Group '{}' already exists".format(GROUPNAME)


def test_user_conflict():
    error = UserConflictError(USERNAME)
    assert str(error) == "Username '{}' already in use".format(USERNAME)
    

    