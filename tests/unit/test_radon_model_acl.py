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
import json

import radon.model.acl as acl
from radon.model.acl import (
    aceflag_to_cdmi_str,
    acemask_to_cdmi_str,
    acemask_to_str,
    acl_cdmi_to_cql,
    acl_list_to_cql,
    cdmi_str_to_aceflag,
    cdmi_str_to_acemask,
    serialize_acl_metadata,
    str_to_acemask,
)
from radon import cfg
from radon.model import (
    Collection,
    Group
)
from radon.database import (
    connect,
    destroy,
    initialise,
    create_root,
    create_tables
)

TEST_KEYSPACE = "test_keyspace"


def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_root()
    Group.create(name="grp1")


def teardown_module(module):
    destroy()


def test_acemask_to_str():
    assert acemask_to_str(0x0, True) == "none"
    assert acemask_to_str(0x09, True) == "read"
    assert acemask_to_str(0x56, True) == "write"
    assert acemask_to_str(0x5F, True) == "read/write"
    assert acemask_to_str(0x01, True) == ""

    assert acemask_to_str(0x0, False) == "none"
    assert acemask_to_str(0x09, False) == "read"
    assert acemask_to_str(0x56, False) == "write"
    assert acemask_to_str(0x5F, False) == "read/write"
    assert acemask_to_str(0x01, False) == ""


def test_cdmi_str_to_aceflag():
    assert cdmi_str_to_aceflag("INHERITED") == 0x00000080
    assert cdmi_str_to_aceflag("IDENTIFIER_GROUP") == 0x00000040
    assert cdmi_str_to_aceflag("INHERIT_ONLY") == 0x00000008
    assert cdmi_str_to_aceflag("NO_PROPAGATE") == 0x00000004
    assert cdmi_str_to_aceflag("CONTAINER_INHERIT") == 0x00000002
    assert cdmi_str_to_aceflag("OBJECT_INHERIT") == 0x00000001
    assert cdmi_str_to_aceflag("NO_FLAGS") == 0x00000000


def test_cdmi_str_to_acemask():
    assert cdmi_str_to_acemask("NONE", True) == 0x0
    assert cdmi_str_to_acemask("READ", True) == 0x09
    assert cdmi_str_to_acemask("WRITE", True) == 0x56
    assert cdmi_str_to_acemask("READ/WRITE", True) == 0x56 | 0x09
    assert cdmi_str_to_acemask("EDIT", True) == 0x56
    assert cdmi_str_to_acemask("DELETE", True) == 0x00010000
    assert cdmi_str_to_acemask("SYNCHRONIZE", True) == 0x00100000
    assert cdmi_str_to_acemask("WRITE_OWNER", True) == 0x00080000
    assert cdmi_str_to_acemask("WRITE_ACL", True) == 0x00040000
    assert cdmi_str_to_acemask("READ_ACL", True) == 0x00020000
    assert cdmi_str_to_acemask("WRITE_RETENTION_HOLD", True) == 0x00000400
    assert cdmi_str_to_acemask("WRITE_RETENTION", True) == 0x00000200
    assert cdmi_str_to_acemask("WRITE_ATTRIBUTES", True) == 0x00000100
    assert cdmi_str_to_acemask("READ_ATTRIBUTES", True) == 0x00000080
    assert cdmi_str_to_acemask("DELETE_OBJECT", True) == 0x00000040
    assert cdmi_str_to_acemask("EXECUTE", True) == 0x00000020
    assert cdmi_str_to_acemask("WRITE_METADATA", True) == 0x00000010
    assert cdmi_str_to_acemask("READ_METADATA", True) == 0x00000008
    assert cdmi_str_to_acemask("APPEND_DATA", True) == 0x00000004
    assert cdmi_str_to_acemask("WRITE_OBJECT", True) == 0x00000002
    assert cdmi_str_to_acemask("READ_OBJECT", True) == 0x00000001

    assert cdmi_str_to_acemask("NONE", False) == 0x0
    assert cdmi_str_to_acemask("READ", False) == 0x09
    assert cdmi_str_to_acemask("WRITE", False) == 0x56
    assert cdmi_str_to_acemask("READ/WRITE", False) == 0x56 | 0x09
    assert cdmi_str_to_acemask("EDIT", False) == 0x56
    assert cdmi_str_to_acemask("DELETE", False) == 0x00010040
    assert cdmi_str_to_acemask("SYNCHRONIZE", False) == 0x00100000
    assert cdmi_str_to_acemask("WRITE_OWNER", False) == 0x00080000
    assert cdmi_str_to_acemask("WRITE_ACL", False) == 0x00040000
    assert cdmi_str_to_acemask("READ_ACL", False) == 0x00020000
    assert cdmi_str_to_acemask("WRITE_RETENTION_HOLD", False) == 0x00000400
    assert cdmi_str_to_acemask("WRITE_RETENTION", False) == 0x00000200
    assert cdmi_str_to_acemask("WRITE_ATTRIBUTES", False) == 0x00000100
    assert cdmi_str_to_acemask("READ_ATTRIBUTES", False) == 0x00000080
    assert cdmi_str_to_acemask("DELETE_SUBCONTAINER", False) == 0x00000040
    assert cdmi_str_to_acemask("EXECUTE", False) == 0x00000020
    assert cdmi_str_to_acemask("WRITE_METADATA", False) == 0x00000010
    assert cdmi_str_to_acemask("READ_METADATA", False) == 0x00000008
    assert cdmi_str_to_acemask("ADD_SUBCONTAINER", False) == 0x00000004
    assert cdmi_str_to_acemask("ADD_OBJECT", False) == 0x00000002
    assert cdmi_str_to_acemask("LIST_CONTAINER", False) == 0x00000001


def test_acl_cdmi_to_cql():
    cdmi_acl = [
        {'identifier': 'grp1',
         'acetype': 'ALLOW',
         'aceflags': "INHERITED",
         'acemask': "READ"
        },
        {'identifier': 'unk_grp',
         'acetype': 'ALLOW',
         'aceflags': "INHERIT_ONLY",
         'acemask': "DELETE"
        },
        {'identifier': 'AUTHENTICATED@',
         'acetype': 'ALLOW',
         'aceflags': "NO_FLAGS",
         'acemask': "ADD_OBJECT"
        },
        {'identifier': 'ANONYMOUS@',
         'acetype': 'ALLOW',
         'aceflags': "CONTAINER_INHERIT",
         'acemask': "LIST_CONTAINER"
        },
        # Syntax error
        {'identifer': 'ANONYMOUS@',
         'acetype': 'ALLOW',
         'aceflags': "CONTAINER_INHERIT",
         'acemask': "LIST_CONTAINER"
        },
    ]
    assert isinstance(acl_cdmi_to_cql(cdmi_acl), str)


def test_acl_list_to_cql():
    # read
    acl = acl_list_to_cql(['grp1'], [])
    assert acl == "{'grp1': {acetype: 'ALLOW', identifier: 'grp1', aceflags: 0, acemask: 9}}"
    # write
    acl = acl_list_to_cql([], ['grp1'])
    assert acl == "{'grp1': {acetype: 'ALLOW', identifier: 'grp1', aceflags: 0, acemask: 86}}"
    # read/write
    acl = acl_list_to_cql(['grp1'], ['grp1'])
    assert acl == "{'grp1': {acetype: 'ALLOW', identifier: 'grp1', aceflags: 0, acemask: 95}}"
    acl = acl_list_to_cql(['AUTHENTICATED@'], [])
    assert acl == "{'AUTHENTICATED@': {acetype: 'ALLOW', identifier: 'AUTHENTICATED@', aceflags: 0, acemask: 9}}"
    acl = acl_list_to_cql([], ['ANONYMOUS@'])
    assert acl == "{'ANONYMOUS@': {acetype: 'ALLOW', identifier: 'ANONYMOUS@', aceflags: 0, acemask: 86}}"
    acl = acl_list_to_cql(["UnknownGroup"], [])
    assert acl == "{}"


def test_str_to_acemask():
    assert str_to_acemask("none", True) == 0x0
    assert str_to_acemask("read", True) == 0x09
    assert str_to_acemask("write", True) == 0x56
    assert str_to_acemask("read/write", True) == 0x56 | 0x09
    assert str_to_acemask("edit", True) == 0x56
    assert str_to_acemask("delete", True) == 0x10000
    assert str_to_acemask("SYNCHRONIZE", True) == 0x00100000
    assert str_to_acemask("WRITE_OWNER", True) == 0x00080000
    assert str_to_acemask("WRITE_ACL", True) == 0x00040000
    assert str_to_acemask("READ_ACL", True) == 0x00020000
    assert str_to_acemask("DELETE", True) == 0x00010000
    assert str_to_acemask("WRITE_RETENTION_HOLD", True) == 0x00000400
    assert str_to_acemask("WRITE_RETENTION", True) == 0x00000200
    assert str_to_acemask("WRITE_ATTRIBUTES", True) == 0x00000100
    assert str_to_acemask("READ_ATTRIBUTES", True) == 0x00000080
    assert str_to_acemask("DELETE_OBJECT", True) == 0x00000040
    assert str_to_acemask("EXECUTE", True) == 0x00000020
    assert str_to_acemask("WRITE_METADATA", True) == 0x00000010
    assert str_to_acemask("READ_METADATA", True) == 0x00000008
    assert str_to_acemask("APPEND_DATA", True) == 0x00000004
    assert str_to_acemask("WRITE_OBJECT", True) == 0x00000002
    assert str_to_acemask("READ_OBJECT", True) == 0x00000001
     
    assert str_to_acemask("none", False) == 0x0
    assert str_to_acemask("read", False) == 0x09
    assert str_to_acemask("write", False) == 0x56
    assert str_to_acemask("read/write", False) == 0x56 | 0x09
    assert str_to_acemask("edit", False) == 0x56
    assert str_to_acemask("delete", False) == 0x10040
    assert str_to_acemask("SYNCHRONIZE", False) == 0x00100000
    assert str_to_acemask("WRITE_OWNER", False) == 0x00080000
    assert str_to_acemask("WRITE_ACL", False) == 0x00040000
    assert str_to_acemask("READ_ACL", False) == 0x00020000
    assert str_to_acemask("WRITE_RETENTION_HOLD", False) == 0x00000400
    assert str_to_acemask("WRITE_RETENTION", False) == 0x00000200
    assert str_to_acemask("WRITE_ATTRIBUTES", False) == 0x00000100
    assert str_to_acemask("READ_ATTRIBUTES", False) == 0x00000080
    assert str_to_acemask("DELETE_SUBCONTAINER", False) == 0x00000040
    assert str_to_acemask("EXECUTE", False) == 0x00000020
    assert str_to_acemask("WRITE_METADATA", False) == 0x00000010
    assert str_to_acemask("READ_METADATA", False) == 0x00000008
    assert str_to_acemask("ADD_SUBCONTAINER", False) == 0x00000004
    assert str_to_acemask("ADD_OBJECT", False) == 0x00000002
    assert str_to_acemask("LIST_CONTAINER", False) == 0x00000001


def test_str():
    coll = Collection.find("/")
    acl = coll.node.acl
    for ace in acl:
        assert isinstance(str(acl[ace]), str)


def test_aceflag_to_cdmi_str():
    assert aceflag_to_cdmi_str(0x00000080) == "INHERITED"
    assert aceflag_to_cdmi_str(0x00000040) == "IDENTIFIER_GROUP"
    assert aceflag_to_cdmi_str(0x00000008) == "INHERIT_ONLY"
    assert aceflag_to_cdmi_str(0x00000004) == "NO_PROPAGATE"
    assert aceflag_to_cdmi_str(0x00000002) == "CONTAINER_INHERIT"
    assert aceflag_to_cdmi_str(0x00000001) == "OBJECT_INHERIT"
    assert aceflag_to_cdmi_str(0x00000000) == "NO_FLAGS"
    assert aceflag_to_cdmi_str(0x00000010) == "NO_FLAGS"


def test_acemask_to_cdmi_str():
    assert acemask_to_cdmi_str(0x00100000, True) == "SYNCHRONIZE"
    assert acemask_to_cdmi_str(0x00080000, True) == "WRITE_OWNER"
    assert acemask_to_cdmi_str(0x00040000, True) == "WRITE_ACL"
    assert acemask_to_cdmi_str(0x00020000, True) == "READ_ACL"
    assert acemask_to_cdmi_str(0x00010000, True) == "DELETE"
    assert acemask_to_cdmi_str(0x00000400, True) == "WRITE_RETENTION_HOLD"
    assert acemask_to_cdmi_str(0x00000200, True) == "WRITE_RETENTION"
    assert acemask_to_cdmi_str(0x00000100, True) == "WRITE_ATTRIBUTES"
    assert acemask_to_cdmi_str(0x00000080, True) == "READ_ATTRIBUTES"
    assert acemask_to_cdmi_str(0x00000040, True) == "DELETE_OBJECT"
    assert acemask_to_cdmi_str(0x00000020, True) == "EXECUTE"
    assert acemask_to_cdmi_str(0x00000010, True) == "WRITE_METADATA"
    assert acemask_to_cdmi_str(0x00000008, True) == "READ_METADATA"
    assert acemask_to_cdmi_str(0x00000004, True) == "APPEND_DATA"
    assert acemask_to_cdmi_str(0x00000002, True) == "WRITE_OBJECT"
    assert acemask_to_cdmi_str(0x00000001, True) == "READ_OBJECT"
    
    assert acemask_to_cdmi_str(0x00100000, False) == "SYNCHRONIZE"
    assert acemask_to_cdmi_str(0x00080000, False) == "WRITE_OWNER"
    assert acemask_to_cdmi_str(0x00040000, False) == "WRITE_ACL"
    assert acemask_to_cdmi_str(0x00020000, False) == "READ_ACL"
    assert acemask_to_cdmi_str(0x00010000, False) == "DELETE"
    assert acemask_to_cdmi_str(0x00000400, False) == "WRITE_RETENTION_HOLD"
    assert acemask_to_cdmi_str(0x00000200, False) == "WRITE_RETENTION"
    assert acemask_to_cdmi_str(0x00000100, False) == "WRITE_ATTRIBUTES"
    assert acemask_to_cdmi_str(0x00000080, False) == "READ_ATTRIBUTES"
    assert acemask_to_cdmi_str(0x00000040, False) == "DELETE_SUBCONTAINER"
    assert acemask_to_cdmi_str(0x00000020, False) == "EXECUTE"
    assert acemask_to_cdmi_str(0x00000010, False) == "WRITE_METADATA"
    assert acemask_to_cdmi_str(0x00000008, False) == "READ_METADATA"
    assert acemask_to_cdmi_str(0x00000004, False) == "ADD_SUBCONTAINER"
    assert acemask_to_cdmi_str(0x00000002, False) == "ADD_OBJECT"
    assert acemask_to_cdmi_str(0x00000001, False) == "LIST_CONTAINER"


def test_serialize_acl_metadata():
    coll = Collection.find("/")
    
    cdmi_acl = serialize_acl_metadata(coll)
    
    assert 'cdmi_acl' in cdmi_acl




