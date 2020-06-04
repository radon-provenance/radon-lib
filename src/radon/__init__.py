"""Copyright 2019 -

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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

__version__ = "1.0.1"

ENV_DSE_HOST_VAR = "DSE_HOST"
ENV_MQTT_HOST_VAR = "MQTT_HOST"

DEFAULT_DSE_HOST = "127.0.0.1"
DEFAULT_DSE_KEYSPACE = "radon"
DEFAULT_DSE_STRATEGY = "SimpleStrategy"
DEFAULT_DSE_REPL_FACTOR = 1
DEFAULT_MQTT_HOST = "127.0.0.1"


class Config(object):
    """Store the configuration options for radon
    - Environment variables to set:
        DSE_HOST: space separated list of IP/Host address (default: ('127.0.0.1',))
        MQTT_HOST: IP/Host address of the MQTT server (default: '127.0.0.1')
    """

    def __init__(self):
        # List of host address for the DSE cluster
        dse_host_var = os.environ.get(ENV_DSE_HOST_VAR)
        if dse_host_var:
            self.dse_host = dse_host_var.split(" ")
        else:
            self.dse_host = [DEFAULT_DSE_HOST,]
        # Cassandra keyspace ("SimpleStrategy" or "NetworkTopologyStrategy")
        self.dse_keyspace = DEFAULT_DSE_KEYSPACE
        # Not used for Simple Strategy
        # map of dc_names: replication_factor for NetworkTopologyStrategy
        self.dse_dc_replication_map = {}
        self.dse_strategy = DEFAULT_DSE_STRATEGY
        self.dse_repl_factor = DEFAULT_DSE_REPL_FACTOR

        # IP address of the MQTT server
        self.mqtt_host = os.environ.get(ENV_MQTT_HOST_VAR, DEFAULT_MQTT_HOST)
        
        # Debug mode
        self.debug = False


cfg = Config()
