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

import unittest

from radon import cfg

from radon.models import initialise, sync


class RadonTest(unittest.TestCase):
    def test_cfg(self):
        """Check that the global config variable is correctly set up and check
        that the type values are correct"""
        assert isinstance(cfg.dse_host, list)
        assert isinstance(cfg.dse_keyspace, str)
        assert isinstance(cfg.dse_strategy, str)
        assert isinstance(cfg.dse_repl_factor, int)
        assert isinstance(cfg.mqtt_host, str)

    def test_initialise(self):
        """Check the DSE connection"""
        initialise()

    # I'm not sure it's wise to test this one on a production system as it
    # may change the schemas


#     def test_sync(self):
#         """Check the sync method"""
#         sync()


# TODO

# test_user:
#   - create user
#   - modify user
#   - delete user
#   - user.to_dict()
#   - User.find()
