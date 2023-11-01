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

from radon.util import default_uuid
from radon import cfg
from radon.database import (
    connect,
    create_default_users,
    create_root,
    create_tables,
    destroy,
    initialise,
)
from radon.model import (
    Group,
    User
)
from radon.model.errors import (
    GroupConflictError,
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


def test_create():
    grp_name = uuid.uuid4().hex
    grp = Group.create(name=grp_name)
    
    assert grp.name == grp_name
    
    # Test already existing group
    with pytest.raises(GroupConflictError):
        grp = Group.create(name=grp_name)

    grp.delete()
    
    # Test the username for the notification
    grp = Group.create(name=grp_name, username="pytest")
    grp.delete()


def create_random_user(groups):
    user_name = uuid.uuid4().hex
    email = uuid.uuid4().hex
    password = uuid.uuid4().hex
    administrator = True
    
    return User.create(name=user_name,
                       email=email,
                       password=password, 
                       administrator=administrator,
                       groups=groups)

def test_add_user():
    grp1_name = uuid.uuid4().hex
    grp2_name = uuid.uuid4().hex
    grp3_name = uuid.uuid4().hex
    
    g1 = Group.create(name=grp1_name)
    g2 = Group.create(name=grp2_name)
    g3 = Group.create(name=grp3_name)
    
    u1 = create_random_user([])
    u2 = create_random_user([])
    u3 = create_random_user([])
    u4 = create_random_user([g2.name, g3.name])
    
    # g2 = [u4]
    # g3 = [u4]
    
    g1.add_user(u1.name)
    assert u1.name in g1.get_members()
    # g1 = [u1]
    
    added, not_added, already_there = g2.add_users([u1.name, u2.name, u4.name, "unknown_user"])
    # g2 = [u1, u2, u4]
    assert u1.name in g2.get_members()
    assert u2.name in g2.get_members()
    assert u4.name in g2.get_members()    # From create
    assert added == [u1.name, u2.name]
    assert not_added == ["unknown_user"]
    assert already_there == [u4.name]
    
    g2.rm_user(u4.name)
    # g2 = [u1, u2]
    assert not u4.name in g2.get_members()

    removed, not_there, not_exist = g2.rm_users([u1.name, u2.name, u4.name, "unknown_user"])
    assert removed == [u1.name, u2.name]
    assert not_there == [u4.name]
    assert not_exist == ["unknown_user"]

    g1.delete()
    g2.delete()
    g3.delete()
    u1.delete()
    u2.delete()
    u3.delete()
    u4.delete()


def test_to_dict():
    grp1_name = uuid.uuid4().hex
    g1 = Group.create(name=grp1_name)
    
    u1 = create_random_user([g1.name])

    g_dict = g1.to_dict()
    assert g_dict['uuid'] == g1.uuid
    assert g_dict['name'] == grp1_name
    assert g_dict['members'] == [u1.name]
    
    g1.delete()
    u1.delete()


def test_update():
    grp1_name = uuid.uuid4().hex
    g1 = Group.create(name=grp1_name)
    
    g1.update(uuid=default_uuid())
    g1.update(uuid=default_uuid(), username="user1")
    
    g1.delete()





