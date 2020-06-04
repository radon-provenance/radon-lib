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
from radon.models.notification import (
    Notification,
    OBJ_USER,
    OP_CREATE
)



# Populated by create_test_env
TEST_KEYSPACE = "radon_pytest"


def test_publish():
    cfg.dse_keyspace = TEST_KEYSPACE
    cfg.mqtt_host = "172.17.0.5"
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

    recent = Notification.recent(count=10)
    assert len(recent) == 10
    


