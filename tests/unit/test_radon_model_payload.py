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
import json

from radon.model.config import cfg
from radon.database import (
    connect,
    destroy,
    initialise,
    create_root,
    create_tables
)
from radon.model.payload import (
    Payload,
    PayloadCreateCollectionFail,
    PayloadDeleteCollectionRequest,
    PayloadDeleteResourceRequest,
    PayloadDeleteUserSuccess,
    OP_CREATE,
    OP_DELETE,
    OP_UPDATE,
    OPT_REQUEST,
    OPT_SUCCESS,
    OPT_FAIL,
    OBJ_RESOURCE,
    OBJ_COLLECTION,
    OBJ_USER,
    OBJ_GROUP,
)

TEST_KEYSPACE = "test_keyspace"



def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_root()



def teardown_module(module):
    destroy()


def test_payload():
    # Test create without meta
    payload = {
             "obj" : {"path" : "/{}".format(uuid.uuid4().hex)}
         }
    payload_user = {
             "obj" : {"login" : uuid.uuid4().hex}
        }
    p = Payload(OP_CREATE, OPT_REQUEST, OBJ_RESOURCE, payload)
    assert p.get_object_key() == payload['obj']['path']
    
    # Test unknown object
    p = Payload(OP_CREATE, OPT_REQUEST, "unknown", payload)
    assert p.get_object_key() == "Unknown_Object"
    
    # Test repr
    p = Payload(OP_CREATE, OPT_REQUEST, OBJ_RESOURCE, payload)
    payload_str = repr(p)
    payload_json = json.loads(payload_str)
    assert payload_json['obj']['path'] == payload['obj']['path']
    
    # Test PayloadCreateFail
    p = PayloadCreateCollectionFail(payload)
    assert p.json['meta']['msg'] == "Create failed"
    
    # Test PayloadDeleteCollectionRequest
    path = "/{}/".format(uuid.uuid4().hex)
    p = PayloadDeleteCollectionRequest.default(path)
    assert p.json['obj']['path'] == path
    
    # Test PayloadDeleteResourceRequest
    path = "/{}/".format(uuid.uuid4().hex)
    p = PayloadDeleteResourceRequest.default(path)
    assert p.json['obj']['path'] == path
    
    # Test PayloadDeleteUserSuccess
    
    p = PayloadDeleteUserSuccess(payload_user)
    assert p.json['obj']['login'] == payload_user['obj']['login']


if __name__ == "__main__":
    setup_module()
    test_payload()
    destroy()
