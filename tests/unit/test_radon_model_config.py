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


import os
import pytest

from radon.model.config import (
    cfg,
    LocalConfig,
    DEFAULT_DSE_HOST,
    DEFAULT_DSE_KEYSPACE,
    DEFAULT_DSE_STRATEGY,
    DEFAULT_DSE_REPL_FACTOR,
    DEFAULT_MQTT_HOST,
    ENV_DSE_HOST_VAR,
    ENV_MQTT_HOST_VAR
)


def test_config():
    # Test DSE HOST VAR
    os.environ[ENV_DSE_HOST_VAR] = "192.168.56.100"
    cfg = LocalConfig()
    assert cfg.dse_host == ["192.168.56.100"]

    os.environ[ENV_DSE_HOST_VAR] = "192.168.56.100 192.168.56.101"
    cfg = LocalConfig()
    assert cfg.dse_host == ["192.168.56.100", "192.168.56.101"]

    del os.environ[ENV_DSE_HOST_VAR]
    cfg = LocalConfig()
    assert cfg.dse_host == [DEFAULT_DSE_HOST,]

    # Test MQTT HOST VAR
    os.environ[ENV_MQTT_HOST_VAR] = "192.168.56.100"
    cfg = LocalConfig()
    assert cfg.mqtt_host == "192.168.56.100"

    del os.environ[ENV_MQTT_HOST_VAR]
    cfg = LocalConfig()
    assert cfg.mqtt_host == DEFAULT_MQTT_HOST

    cfg = LocalConfig()
    assert cfg.dse_keyspace == DEFAULT_DSE_KEYSPACE
    assert cfg.dse_strategy == DEFAULT_DSE_STRATEGY
    assert cfg.dse_repl_factor == DEFAULT_DSE_REPL_FACTOR


def test_to_dict():
    cfg_dict = cfg.to_dict()
    test_dict = {
        "dse_host" : cfg.dse_host,
        "dse_keyspace" : cfg.dse_keyspace,
        "dse_dc_replication_map" : cfg.dse_dc_replication_map,
        "dse_strategy" : cfg.dse_strategy,
        "dse_repl_factor": cfg.dse_repl_factor,
        "mqtt_host" : cfg.mqtt_host,
        "debug" : cfg.debug
    }
    assert cfg_dict == test_dict


def test_repr():
    test_dict = {
        "dse_host" : cfg.dse_host,
        "dse_keyspace" : cfg.dse_keyspace,
        "dse_dc_replication_map" : cfg.dse_dc_replication_map,
        "dse_strategy" : cfg.dse_strategy,
        "dse_repl_factor": cfg.dse_repl_factor,
        "mqtt_host" : cfg.mqtt_host,
        "debug" : cfg.debug
    }
    assert str(test_dict) == str(cfg)




