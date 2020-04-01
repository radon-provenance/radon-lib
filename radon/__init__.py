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

# Load environment variables from a .env file (root dir of the package)
load_dotenv()
# BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))


__version__ = "1.0.0"


class Config(object):
    """Store the configuration options for radon
    - Environment variables to set:
        DSE_HOST: space separated list of IP/Host address (default: ('127.0.0.1',))
        MQTT_HOST: IP/Host address of the MQTT server (default: '127.0.0.1')
    """

    def __init__(self):
        # List of host address for the DSE cluster
        dse_host_var = os.environ.get("DSE_HOST")
        if dse_host_var:
            self.dse_host = dse_host_var.split(" ")
        else:
            self.dse_host = ("127.0.0.1",)
        # Cassandra keyspace
        self.dse_keyspace = "radon"
        self.dse_strategy = "SimpleStrategy"
        self.dse_repl_factor = 1

        # IP address of the MQTT server
        self.mqtt_host = os.environ.get("MQTT_HOST", "127.0.0.1")


cfg = Config()
