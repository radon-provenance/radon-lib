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

from radon import cfg
from radon.models import connect
from radon.models.user import User
from radon.models.errors import UserConflictError


# Populated by create_test_env
TEST_KEYSPACE = "radon_pytest"



def test_authenticate():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']

    user = User.create(name=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    user = User.find(user_name)
    
    assert user.authenticate(password)
    assert not user.authenticate(uuid.uuid4().hex)


def test_users():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']
    notification_username = uuid.uuid4().hex

    # Simple create
    user = User.create(name=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    user = User.find(user_name)
    assert user.get_groups() == groups
    assert user.is_active() == True
    assert user.is_authenticated() == True

    with pytest.raises(UserConflictError):
        user = User.create(name=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    user.delete()

    # Create with a username for notification
    user = User.create(name=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups,
                       username=notification_username)
    user.delete()


def test_dict():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']
    notification_username = uuid.uuid4().hex

    # Simple create
    user = User.create(name=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    user = User.find(user_name)

    user_dict = user.to_dict()
    assert user_dict['uuid'] == user.uuid
    assert user_dict['administrator'] == True
    user.delete()


def test_update():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']
    notification_username = uuid.uuid4().hex

    # Simple create
    user = User.create(name=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    new_password = uuid.uuid4().hex
    new_email = uuid.uuid4().hex
    user.update(password=new_password)
    user.update(email=new_email, username=notification_username)
    user = User.find(user_name)







