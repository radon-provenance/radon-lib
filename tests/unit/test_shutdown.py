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
import os
import shutil
import tempfile

from cli_test_helpers import ArgvContext, EnvironContext
from unittest.mock import patch
  
from radon.model.config import cfg
import radon.cli



SESSION_PATH = os.path.join(os.path.expanduser("~/.radon"), "session.pickle")

TEST_KEYSPACE = "test_keyspace"

def test():
    #####################
    ## Create Database ##
    #####################
    
    cfg.dse_keyspace = TEST_KEYSPACE
    
    app = radon.cli.RadonApplication(SESSION_PATH)
    app.init()
    

    app.drop({"-f" : True})
    
    # with ArgvContext('radmin', 'drop'):
    #     radon.cli.main()
    
    
test()

