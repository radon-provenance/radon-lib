# Copyright 2021
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
  
import radon.cli



SESSION_PATH = os.path.join(os.path.expanduser("~/.radon"), "session.pickle")


def test():
    #####################
    ## Create Database ##
    #####################
    
    radon.cfg.dse_keyspace = "temp_radon"
    
    app = radon.cli.RadonApplication(SESSION_PATH)
    app.init()
    

    app.drop({"-f" : True})
    
    # with ArgvContext('radmin', 'drop'):
    #     radon.cli.main()
    
    
test()

