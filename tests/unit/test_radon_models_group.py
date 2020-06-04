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

from radon.util import default_uuid
from radon import cfg
from radon.models import connect
from radon.models.group import Group
from radon.models.user import User
from radon.models.errors import (
    GroupConflictError,
    UserConflictError
)


# Populated by create_test_env
TEST_KEYSPACE = "radon_pytest"

GRP1 = "test_grp1"
GRP2 = "test_grp2"
GRP3 = "test_grp3"
USER1 = "test_user1"
USER2 = "test_user2"
USER3 = "test_user3"
USER4 = "test_user4"

USERS = [(USER1, "user1@test.com", "user1", True),
         (USER2, "user2@test.com", "user2", False),
         (USER3, "user3@test.com", "user3", False),
         (USER4, "user4@test.com", "user4", False, [GRP2, GRP3])]


def test_create():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    try:
        grp = Group.create(name=GRP1)
    except GroupConflictError:
        grp = Group.find(GRP1)
    assert grp.name == GRP1
    
    # Test already existing group
    with pytest.raises(GroupConflictError):
        grp = Group.create(name=GRP1)

    grp.delete()
    
    # Test the username for the notification
    grp = Group.create(name=GRP1, username="pytest")
    grp.delete()


def test_add_user():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    try:
        g1 = Group.create(name=GRP1)
    except GroupConflictError:
        g1 = Group.find(GRP1)
    try:
        g2 = Group.create(name=GRP2)
    except GroupConflictError:
        g2 = Group.find(GRP2)
    try:
        g3 = Group.create(name=GRP3)
    except GroupConflictError:
        g3 = Group.find(GRP3)
    try:
        u1 = User.create(name=USERS[0][0], email=USERS[0][1], password=USERS[0][2],
                         administrator=USERS[0][3])
    except UserConflictError:
        u1 = User.find(USERS[0][0])
    try:
        u2 = User.create(name=USERS[1][0], email=USERS[1][1], password=USERS[2][2],
                         administrator=USERS[1][3])
    except UserConflictError:
        u2 = User.find(USERS[1][0])
    try:
        u3 = User.create(name=USERS[2][0], email=USERS[2][1], password=USERS[2][2],
                         administrator=USERS[2][3])
    except UserConflictError:
        u3 = User.find(USERS[2][0])
    try:
        u4 = User.create(name=USERS[3][0], email=USERS[3][1], password=USERS[2][2],
                         administrator=USERS[3][3], groups=USERS[3][4])
    except UserConflictError:
        u4 = User.find(USERS[3][0])
    
    # g2 = [u4]
    # g3 = [u4]

    g1.add_user(u1.name)
    assert u1.name in g1.get_usernames()
    # g1 = [u1]
    
    added, not_added, already_there = g2.add_users([u1.name, u2.name, u4.name, "unknown_user"])
    # g2 = [u1, u2, u4]
    assert u1.name in g2.get_usernames()
    assert u2.name in g2.get_usernames()
    assert u4.name in g2.get_usernames()    # From create
    assert added == [u1.name, u2.name]
    assert not_added == ["unknown_user"]
    assert already_there == [u4.name]
    
    g2.rm_user(u4.name)
    # g2 = [u1, u2]
    assert not u4.name in g2.get_usernames()

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
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
    try:
        g1 = Group.create(name=GRP1)
    except GroupConflictError:
        g1 = Group.find(GRP1)
    try:
        u1 = User.create(name=USERS[0][0], email=USERS[0][1], password=USERS[0][2],
                         administrator=USERS[0][3], groups=[g1.name])
    except UserConflictError:
        u1 = User.find(USERS[0][0])

    g_dict = g1.to_dict()
    print(g_dict)
    assert g_dict['uuid'] == g1.uuid
    assert g_dict['name'] == GRP1
    assert g_dict['members'] == [u1.name]
    
    g1.delete()
    u1.delete()


def test_update():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    try:
        g1 = Group.create(name=GRP1)
    except GroupConflictError:
        g1 = Group.find(GRP1)
    
    g1.update(uuid=default_uuid())
    g1.update(uuid=default_uuid(), username="user1")
    
    g1.delete()



