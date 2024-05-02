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

from radon.model.config import cfg
from radon.database import (
    connect,
    create_default_users,
    create_root,
    create_tables,
    destroy,
    initialise,
)
from radon.model.group import Group
from radon.model.user import User
from radon.model.errors import (
    UserConflictError
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


def test_authenticate(mocker):
    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']

    user = User.create(login=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    user = User.find(user_name)
    
    assert user.authenticate(password)
    assert not user.authenticate(uuid.uuid4().hex)    
    
    # Check ldap authentication (mock the actual test)
    cfg.auth_ldap_server_uri = "ldap://ldap.example.com"
    cfg.auth_ldap_user_dn_template = "uid=%(user)s,ou=users,dc=example,dc=com"
    mocker.patch('ldap.ldapobject.SimpleLDAPObject.simple_bind_s', return_value=True)
    user.update(ldap=True)
    assert user.authenticate(password)
    
    # Check inactive user
    user.update(active=False)
    assert not user.authenticate(password)


def test_delete():
    user_name = uuid.uuid4().hex
    password = uuid.uuid4().hex

    user = User.create(login=user_name, password=password)
    
    # Class method delete
    User.delete_user(user_name)
    # User not found
    User.delete_user(uuid.uuid4().hex)



def test_dict():
    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']
    notification_username = uuid.uuid4().hex

    # Simple create
    user = User.create(login=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    user = User.find(user_name)

    user_dict = user.to_dict()
    assert user_dict['uuid'] == user.uuid
    assert user_dict['administrator'] == True
    user.delete()


def test_group():
    grp1_name = uuid.uuid4().hex
    grp2_name = uuid.uuid4().hex
    grp1 = Group.create(name=grp1_name)
    grp2 = Group.create(name=grp2_name)
    
    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = [grp1_name, grp2_name]
    user = User.create(login=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    
    assert set(user.get_groups()) == set([grp1_name, grp2_name])
    
    user.rm_group(grp1_name)
    assert set(user.get_groups()) == set([grp2_name])
    user.delete()
    
    # Get empty groups list when user not found
    user_name = uuid.uuid4().hex
    password = uuid.uuid4().hex
    user = User.create(login=user_name, password=password)
    user.login = uuid.uuid4().hex
    assert user.get_groups() == []
    user.delete()
    
    grp1.delete()
    grp2.delete()


def test_update():
    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']
    notification_username = uuid.uuid4().hex

    # Simple create
    user = User.create(login=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    new_password = uuid.uuid4().hex
    new_email = uuid.uuid4().hex
    user.update(password=new_password)
    user.update(email=new_email, sender=notification_username)
    user = User.find(user_name)
    
    # Try to update login (doesn't do anything)
    user.update(login=uuid.uuid4().hex)
    


def test_users():
    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    groups = ['grp1']
    notification_username = uuid.uuid4().hex

    # Simple create
    user = User.create(login=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)
    user = User.find(user_name)
    assert user.get_groups() == groups
    assert user.is_active() == True
    assert user.is_authenticated() == True
    user.delete()

    # Create with a username for notification
    user = User.create(login=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups,
                       sender=notification_username)
    user.delete()





