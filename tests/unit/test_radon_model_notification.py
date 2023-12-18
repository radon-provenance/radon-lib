"""Copyright 2021

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
    OP_CREATE
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
    cfg.dse_keyspace = TEST_KEYSPACE
    cfg.mqtt_host = "172.17.0.2"
    connect()
    uuid = default_uuid()
    notif = Notification.new(
        operation=OP_CREATE,
        object_type=OBJ_USER,
        object_uuid=uuid,
        username="test_user",
        processed=True,
        payload="{}",
    )
    notif.mqtt_publish()
    notif.delete()


def test_recent():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    # If count is high it may be possible that this test fails (if there are
    # less notification than 'high' in the table
    recent = Notification.recent(count=2)
    assert len(recent) == 2






