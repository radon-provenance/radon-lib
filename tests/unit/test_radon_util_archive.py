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

from radon.util_archive import (
    is_collection,
    is_resource,
    path_exists
)

from radon import cfg
from radon.models import connect

TEST_KEYSPACE = "radon_pytest"

def test_is_collection():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    assert is_collection("/")
    assert is_collection("/coll1")
    assert not is_collection("/undefined_coll")


def test_is_resource():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    assert is_resource("/test.txt")
    assert is_resource("/coll1/test.txt")
    assert not is_resource("/undefined_coll/test.txt")


def test_path_exists():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    assert path_exists("/")
    assert path_exists("/coll1")
    assert not path_exists("/undefined_coll")




