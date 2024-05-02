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

from radon.util import default_uuid
from radon.model.config import cfg
from radon.database import (
    connect,
    create_default_users,
    destroy,
    initialise,
    create_root,
    create_tables
)
from radon.model.notification import (
    Notification,
    OBJ_USER,
    OP_CREATE,
    OPT_REQUEST,
    
    MSG_UNDEFINED_PATH,
    MSG_UNDEFINED_NAME,
    MSG_UNDEFINED_LOGIN,
    MSG_PAYLOAD_ERROR,
    MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
    MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
    MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE,
    
    create_collection_fail,
    create_collection_request,
    create_collection_success,
    create_group_fail,
    create_group_request,
    create_group_success,
    create_resource_fail,
    create_resource_request,
    create_resource_success,
    create_user_fail,
    create_user_request,
    create_user_success,
    
    delete_collection_fail,
    delete_collection_request,
    delete_collection_success,
    delete_group_fail,
    delete_group_request,
    delete_group_success,
    delete_resource_fail,
    delete_resource_request,
    delete_resource_success,
    delete_user_fail,
    delete_user_request,
    delete_user_success,
    
    update_collection_fail,
    update_collection_request,
    update_collection_success,
    update_group_fail,
    update_group_request,
    update_group_success,
    update_resource_fail,
    update_resource_request,
    update_resource_success,
    update_user_fail,
    update_user_request,
    update_user_success,
)
from radon.model.payload import (
    PayloadCreateCollectionFail,
    PayloadCreateCollectionRequest,
    PayloadCreateCollectionSuccess,
    PayloadCreateGroupFail,
    PayloadCreateGroupRequest,
    PayloadCreateGroupSuccess,
    PayloadCreateResourceFail,
    PayloadCreateResourceRequest,
    PayloadCreateResourceSuccess,
    PayloadCreateUserFail,
    PayloadCreateUserRequest,
    PayloadCreateUserSuccess,
    
    PayloadDeleteCollectionFail,
    PayloadDeleteCollectionRequest,
    PayloadDeleteCollectionSuccess,
    PayloadDeleteGroupFail,
    PayloadDeleteGroupRequest,
    PayloadDeleteGroupSuccess,
    PayloadDeleteResourceFail,
    PayloadDeleteResourceRequest,
    PayloadDeleteResourceSuccess,
    PayloadDeleteUserFail,
    PayloadDeleteUserRequest,
    PayloadDeleteUserSuccess,
    
    PayloadUpdateCollectionFail,
    PayloadUpdateCollectionRequest,
    PayloadUpdateCollectionSuccess,
    PayloadUpdateGroupFail,
    PayloadUpdateGroupRequest,
    PayloadUpdateGroupSuccess,
    PayloadUpdateResourceFail,
    PayloadUpdateResourceRequest,
    PayloadUpdateResourceSuccess,
    PayloadUpdateUserFail,
    PayloadUpdateUserRequest,
    PayloadUpdateUserSuccess,
)
from radon.util import (
    payload_check,
)

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


def test_publish():
    uuid = default_uuid()
    notif = Notification.new(
        op_name=OP_CREATE,
        op_type=OPT_REQUEST,
        obj_type=OBJ_USER,
        obj_key=uuid,
        sender="test_user",
        processed=True,
        payload="{}",
    )
    notif.mqtt_publish()
    notif.delete()


def test_recent():
    # If count is high it may be possible that this test fails (if there are
    # less notification than 'high' in the table
    recent = Notification.recent(count=2)
    assert len(recent) == 2

################################################################################
## Create                                                                     ##
################################################################################

def test_create_collection_fail():
    key = '/'
    
    payload = { "obj" : {"path" : key} }
    payload = PayloadCreateCollectionFail(payload)
    notif = create_collection_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadCreateCollectionFail(payload)
    notif = create_collection_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_collection_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH
    

def test_create_collection_request():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadCreateCollectionRequest(payload)
    notif = create_collection_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadCreateCollectionRequest(payload)
    notif = create_collection_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_collection_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_create_collection_success():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadCreateCollectionSuccess(payload)
    notif = create_collection_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadCreateCollectionSuccess(payload)
    notif = create_collection_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_collection_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_create_group_fail():
    key = uuid.uuid4().hex
    
    payload = { "obj" : {"name" : key} }
    payload = PayloadCreateGroupFail(payload)
    notif = create_group_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadCreateGroupFail(payload)
    notif = create_group_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"

    # Wrong class
    payload = { "obj" : {"name" : key} }
    notif = create_group_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_create_group_request():
    key = '/'
    payload = { "obj" : {"name" : key} }
    payload = PayloadCreateGroupRequest(payload)
    notif = create_group_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadCreateGroupRequest(payload)
    notif = create_group_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_group_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_create_group_success():
    key = '/'
    payload = { "obj" : {"name" : key} }
    payload = PayloadCreateGroupSuccess(payload)
    notif = create_group_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadCreateGroupSuccess(payload)
    notif = create_group_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_group_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_create_resource_fail():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadCreateResourceFail(payload)
    notif = create_resource_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadCreateResourceFail(payload)
    notif = create_resource_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_resource_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_create_resource_request():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadCreateResourceRequest(payload)
    notif = create_resource_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadCreateResourceRequest(payload)
    notif = create_resource_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_resource_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_create_resource_success():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadCreateResourceSuccess(payload)
    notif = create_resource_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadCreateResourceSuccess(payload)
    notif = create_resource_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_resource_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH



def test_create_user_fail():
    key = uuid.uuid4().hex
    payload = { "obj" : {"login" : key} }
    payload = PayloadCreateUserFail(payload)
    notif = create_user_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadCreateUserFail(payload)
    notif = create_user_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_user_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


def test_create_user_request():
    key = '/'
    payload = { "obj" : {"login" : key} }
    payload = PayloadCreateUserRequest(payload)
    notif = create_user_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadCreateUserRequest(payload)
    notif = create_user_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_user_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


def test_create_user_success():
    key = '/'
    payload = { "obj" : {"login" : key} }
    payload = PayloadCreateUserSuccess(payload)
    notif = create_user_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadCreateUserSuccess(payload)
    notif = create_user_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = create_user_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


################################################################################
## Delete                                                                     ##
################################################################################

def test_delete_collection_fail():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadDeleteCollectionFail(payload)
    notif = delete_collection_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadDeleteCollectionFail(payload)
    notif = delete_collection_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_collection_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_delete_collection_request():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadDeleteCollectionRequest(payload)
    notif = delete_collection_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadDeleteCollectionRequest(payload)
    notif = delete_collection_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_collection_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_delete_collection_success():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadDeleteCollectionSuccess(payload)
    notif = delete_collection_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadDeleteCollectionSuccess(payload)
    notif = delete_collection_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_collection_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_delete_group_fail():
    key = uuid.uuid4().hex
    payload = { "obj" : {"name" : key} }
    payload = PayloadDeleteGroupFail(payload)
    notif = delete_group_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadDeleteGroupFail(payload)
    notif = delete_group_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"

    # Wrong class
    payload = { "obj" : {"name" : key} }
    notif = delete_group_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_delete_group_request():
    key = '/'
    payload = { "obj" : {"name" : key} }
    payload = PayloadDeleteGroupRequest(payload)
    notif = delete_group_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadDeleteGroupRequest(payload)
    notif = delete_group_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"

    # Wrong class
    payload = { "obj" : {"name" : key} }
    notif = delete_group_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_delete_group_success():
    key = '/'
    payload = { "obj" : {"name" : key} }
    payload = PayloadDeleteGroupSuccess(payload)
    notif = delete_group_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadDeleteGroupSuccess(payload)
    notif = delete_group_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE

    # Wrong class
    payload = { "obj" : {"name" : key} }
    notif = delete_group_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_delete_resource_fail():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadDeleteResourceFail(payload)
    notif = delete_resource_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadDeleteResourceFail(payload)
    notif = delete_resource_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_resource_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_delete_resource_request():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadDeleteResourceRequest(payload)
    notif = delete_resource_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadDeleteResourceRequest(payload)
    notif = delete_resource_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_resource_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_delete_resource_success():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadDeleteResourceSuccess(payload)
    notif = delete_resource_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadDeleteResourceSuccess(payload)
    notif = delete_resource_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_resource_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_delete_user_fail():
    key = uuid.uuid4().hex
    payload = { "obj" : {"login" : key} }
    payload = PayloadDeleteUserFail(payload)
    notif = delete_user_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadDeleteUserFail(payload)
    notif = delete_user_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_user_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


def test_delete_user_request():
    key = '/'
    payload = { "obj" : {"login" : key} }
    payload = PayloadDeleteUserRequest(payload)
    notif = delete_user_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadDeleteUserRequest(payload)
    notif = delete_user_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_user_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


def test_delete_user_success():
    key = '/'
    payload = { "obj" : {"login" : key} }
    payload = PayloadDeleteUserSuccess(payload)
    notif = delete_user_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadDeleteUserSuccess(payload)
    notif = delete_user_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = delete_user_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


################################################################################
## Update                                                                     ##
################################################################################


def test_update_collection_fail():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadUpdateCollectionFail(payload)
    notif = update_collection_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadUpdateCollectionFail(payload)
    notif = update_collection_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_collection_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_update_collection_request():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadUpdateCollectionRequest(payload)
    notif = update_collection_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadUpdateCollectionRequest(payload)
    notif = update_collection_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_collection_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_update_collection_success():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadUpdateCollectionSuccess(payload)
    notif = update_collection_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadUpdateCollectionSuccess(payload)
    notif = update_collection_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_collection_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_update_group_fail():
    key = uuid.uuid4().hex
    payload = { "obj" : {"name" : key} }
    payload = PayloadUpdateGroupFail(payload)
    notif = update_group_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadUpdateGroupFail(payload)
    notif = update_group_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"

    # Wrong class
    payload = { "obj" : {"name" : key} }
    notif = update_group_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_update_group_request():
    key = '/'
    payload = { "obj" : {"name" : key} }
    payload = PayloadUpdateGroupRequest(payload)
    notif = update_group_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadUpdateGroupRequest(payload)
    notif = update_group_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"

    # Wrong class
    payload = { "obj" : {"name" : key} }
    notif = update_group_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_update_group_success():
    key = '/'
    payload = { "obj" : {"name" : key} }
    payload = PayloadUpdateGroupSuccess(payload)
    notif = update_group_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"name" : key} }
    payload = PayloadUpdateGroupSuccess(payload)
    notif = update_group_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE

    # Wrong class
    payload = { "obj" : {"name" : key} }
    notif = update_group_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE
    assert payload_check("/obj/name", payload_dict) == MSG_UNDEFINED_NAME


def test_update_resource_fail():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadUpdateResourceFail(payload)
    notif = update_resource_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadUpdateResourceFail(payload)
    notif = update_resource_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_resource_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_update_resource_request():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadUpdateResourceRequest(payload)
    notif = update_resource_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadUpdateResourceRequest(payload)
    notif = update_resource_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_resource_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_update_resource_success():
    key = '/'
    payload = { "obj" : {"path" : key} }
    payload = PayloadUpdateResourceSuccess(payload)
    notif = update_resource_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"path" : key} }
    payload = PayloadUpdateResourceSuccess(payload)
    notif = update_resource_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_resource_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE
    assert payload_check("/obj/path", payload_dict) == MSG_UNDEFINED_PATH


def test_update_user_fail():
    key = uuid.uuid4().hex
    payload = { "obj" : {"login" : key} }
    payload = PayloadUpdateUserFail(payload)
    notif = update_user_fail(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadUpdateUserFail(payload)
    notif = update_user_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_user_fail(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


def test_update_user_request():
    key = '/'
    payload = { "obj" : {"login" : key} }
    payload = PayloadUpdateUserRequest(payload)
    notif = update_user_request(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadUpdateUserRequest(payload)
    notif = update_user_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == "'obj' is a required property"
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_user_request(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_PAYLOAD_ERROR
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


def test_update_user_success():
    key = '/'
    payload = { "obj" : {"login" : key} }
    payload = PayloadUpdateUserSuccess(payload)
    notif = update_user_success(payload)
    assert notif.obj_key == key

    # Wrong payload
    payload = { "notvalid" : {"login" : key} }
    payload = PayloadUpdateUserSuccess(payload)
    notif = update_user_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE
    
    # Wrong class
    payload = { "obj" : {"path" : key} }
    notif = update_user_success(payload)
    payload_dict = notif.to_dict()['payload']
    assert payload_check("/meta/msg", payload_dict) == MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE
    assert payload_check("/obj/login", payload_dict) == MSG_UNDEFINED_LOGIN


if __name__ == "__main__":
    setup_module()
    destroy()
