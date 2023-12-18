"""Copyright 2023

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



from radon.model.config import cfg
from radon.database import (
    connect,
    destroy,
    initialise,
    create_root,
    create_tables
)
from radon.model.payload import (
    PayloadCreateRequestUser
)
from radon.model.microservices import (
    Microservices,
    ERR_PAYLOAD_CLASS,
)
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


def test_create_user():
    
    payload_json = {
        "obj" : {
            "login" : "test",
            "password" : "test"
        },
        "meta" : {
            "sender": "pytest"
        }
    }
    
    wrong_payload1 = {
        "obj" : {
            "password" : "test"
        },
        "meta" : {
            "sender": "pytest"
        }
    }
    wrong_payload2 = {
        "val" : {
            "login" : "test",
        },
        "meta" : {
            "sender": "pytest"
        }
    }
    
    # Wrong  payload class
    user, msg = Microservices.create_user(payload_json)
    assert msg == ERR_PAYLOAD_CLASS
    assert user == None
    
    # Missing key (login)
    payload = PayloadCreateRequestUser(wrong_payload1)
    user, msg = Microservices.create_user(payload)
    assert msg == "'login' is a required property"
    assert user == None
    
    # Missing 'obj' information
    payload = PayloadCreateRequestUser(wrong_payload2)
    user, msg = Microservices.create_user(payload)
    assert msg == "'obj' is a required property"
    assert user == None
    
    # Corrert payload
    payload = PayloadCreateRequestUser(payload_json)
    user, msg = Microservices.create_user(payload)
    user_find = User.find(payload_json["obj"]["login"])
    if not user:
        user = user_find # already exist
    assert user_find != None
    assert user.login == payload_json["obj"]["login"]
    assert user.login == user_find.login
    
    
    
if __name__ == "__main__":
    setup_module()
    test_create_user()
    destroy()

