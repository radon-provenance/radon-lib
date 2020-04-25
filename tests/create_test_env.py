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

from radon import cfg
from radon.models import (
    destroy,
    initialise,
    sync
)

from radon.models.errors import GroupConflictError
from radon.models.group import Group

TEST_KEYSPACE = "radon_pytest"

GROUPS = ["grp1", "grp2", "grp3", "grp4"]

def create_keyspace():
    """Create a set of objects in a radon keyspace created for the test
    """
    cfg.dse_keyspace = TEST_KEYSPACE

    # Initialise connection and create keyspace
    initialise()
    # Create tables
    sync()

def create_groups():
    for group_name in GROUPS:
        try:
            grp = Group.create(name=group_name)
            print ("group '%s' created (uuid='%s')" % (grp.name, grp.uuid))
        except GroupConflictError:
            print ("group '%s' already exists" % (group_name))
    


    
if __name__ == "__main__":
    create_keyspace()
    create_groups()
    