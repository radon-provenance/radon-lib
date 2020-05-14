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
import json

import radon.models.acl as acl
from radon.models.acl import (
    aceflag_to_cdmi_str,
    acemask_to_cdmi_str,
    acemask_to_str,
    acl_list_to_cql,
    str_to_acemask,
    cdmi_str_to_aceflag,
    cdmi_str_to_acemask,
    serialize_acl_metadata
)
from radon import cfg
from radon.models import connect
from radon.models.collection import Collection


# Populated by create_test_env
TEST_KEYSPACE = "radon_pytest"


def test_aceflag_to_cdmi_str():
    assert aceflag_to_cdmi_str(acl.ACEFLAG_INHERITED) == "INHERITED"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_IDENTIFIER_GROUP) == "IDENTIFIER_GROUP"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_INHERIT_ONLY) == "INHERIT_ONLY"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_NO_PROPAGATE) == "NO_PROPAGATE"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_CONTAINER_INHERIT) == "CONTAINER_INHERIT"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_OBJECT_INHERIT) == "OBJECT_INHERIT"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_NONE) == "NO_FLAGS"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_INHERITED|acl.ACEFLAG_IDENTIFIER_GROUP) == "INHERITED, IDENTIFIER_GROUP"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_INHERIT_ONLY|acl.ACEFLAG_CONTAINER_INHERIT) == "INHERIT_ONLY, CONTAINER_INHERIT"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_IDENTIFIER_GROUP|acl.ACEFLAG_CONTAINER_INHERIT|acl.ACEFLAG_OBJECT_INHERIT) == "IDENTIFIER_GROUP, CONTAINER_INHERIT, OBJECT_INHERIT"
    assert aceflag_to_cdmi_str(acl.ACEFLAG_CONTAINER_INHERIT |acl.ACEFLAG_NO_PROPAGATE) == "NO_PROPAGATE, CONTAINER_INHERIT"
    assert aceflag_to_cdmi_str(0x10) == "NO_FLAGS"



def test_acemask_to_cdmi_str():    
    assert acemask_to_cdmi_str(acl.ACEMASK_SYNCHRONIZE, True) == "SYNCHRONIZE"
    assert acemask_to_cdmi_str(acl.ACEMASK_SYNCHRONIZE, False) == "SYNCHRONIZE"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_OWNER, True) == "WRITE_OWNER"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_OWNER, False) == "WRITE_OWNER"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_ACL, True) == "WRITE_ACL"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_ACL, False) == "WRITE_ACL"
    assert acemask_to_cdmi_str(acl.ACEMASK_READ_ACL, True) == "READ_ACL"
    assert acemask_to_cdmi_str(acl.ACEMASK_READ_ACL, False) == "READ_ACL"
    assert acemask_to_cdmi_str(acl.ACEMASK_DELETE, True) == "DELETE"
    assert acemask_to_cdmi_str(acl.ACEMASK_DELETE, False) == "DELETE"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_RETENTION_HOLD, True) == "WRITE_RETENTION_HOLD"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_RETENTION_HOLD, False) == "WRITE_RETENTION_HOLD"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_RETENTION, True) == "WRITE_RETENTION"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_RETENTION, False) == "WRITE_RETENTION"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_ATTRIBUTES, True) == "WRITE_ATTRIBUTES"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_ATTRIBUTES, False) == "WRITE_ATTRIBUTES"
    assert acemask_to_cdmi_str(acl.ACEMASK_READ_ATTRIBUTES, True) == "READ_ATTRIBUTES"
    assert acemask_to_cdmi_str(acl.ACEMASK_READ_ATTRIBUTES, False) == "READ_ATTRIBUTES"
    assert acemask_to_cdmi_str(acl.ACEMASK_DELETE_OBJECT, True) == "DELETE_OBJECT"
    assert acemask_to_cdmi_str(acl.ACEMASK_DELETE_SUBCONTAINER, False) == "DELETE_SUBCONTAINER"
    assert acemask_to_cdmi_str(acl.ACEMASK_EXECUTE, True) == "EXECUTE"
    assert acemask_to_cdmi_str(acl.ACEMASK_EXECUTE, False) == "EXECUTE"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_METADATA, True) == "WRITE_METADATA"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_METADATA, False) == "WRITE_METADATA"
    assert acemask_to_cdmi_str(acl.ACEMASK_READ_METADATA, True) == "READ_METADATA"
    assert acemask_to_cdmi_str(acl.ACEMASK_READ_METADATA, False) == "READ_METADATA"
    assert acemask_to_cdmi_str(acl.ACEMASK_APPEND_DATA, True) == "APPEND_DATA"
    assert acemask_to_cdmi_str(acl.ACEMASK_ADD_SUBCONTAINER, False) == "ADD_SUBCONTAINER"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_OBJECT, True) == "WRITE_OBJECT"
    assert acemask_to_cdmi_str(acl.ACEMASK_ADD_OBJECT, False) == "ADD_OBJECT"
    assert acemask_to_cdmi_str(acl.ACEMASK_READ_OBJECT, True) == "READ_OBJECT"
    assert acemask_to_cdmi_str(acl.ACEMASK_LIST_CONTAINER, False) == "LIST_CONTAINER"
    assert acemask_to_cdmi_str(acl.ACEMASK_WRITE_OBJECT|acl.ACEMASK_READ_OBJECT, True) == "WRITE_OBJECT, READ_OBJECT"
    assert acemask_to_cdmi_str(0, True) == ""


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


def test_acl_list_to_cql():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()
    
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
    acl = acl_list_to_cql(["UnknownUser"], [])
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


def test_cdmi_str_to_aceflag():
    assert cdmi_str_to_aceflag("INHERITED") == acl.ACEFLAG_INHERITED
    assert cdmi_str_to_aceflag("IDENTIFIER_GROUP") == acl.ACEFLAG_IDENTIFIER_GROUP
    assert cdmi_str_to_aceflag("INHERIT_ONLY") == acl.ACEFLAG_INHERIT_ONLY
    assert cdmi_str_to_aceflag("NO_PROPAGATE") == acl.ACEFLAG_NO_PROPAGATE
    assert cdmi_str_to_aceflag("CONTAINER_INHERIT") == acl.ACEFLAG_CONTAINER_INHERIT
    assert cdmi_str_to_aceflag("OBJECT_INHERIT") == acl.ACEFLAG_OBJECT_INHERIT
    assert cdmi_str_to_aceflag("NO_FLAGS") == acl.ACEFLAG_NONE
    assert cdmi_str_to_aceflag("UNKNOWN_STRING") == 0


def test_cdmi_str_to_acemask():
    assert cdmi_str_to_acemask("none", True) == 0x0
    assert cdmi_str_to_acemask("read", True) == 0x09
    assert cdmi_str_to_acemask("write", True) == 0x56
    assert cdmi_str_to_acemask("read/write", True) == 0x56 | 0x09
    assert cdmi_str_to_acemask("edit", True) == 0x56
    assert cdmi_str_to_acemask("delete", True) == 0x10000
    assert cdmi_str_to_acemask("SYNCHRONIZE", True) == 0x00100000
    assert cdmi_str_to_acemask("WRITE_OWNER", True) == 0x00080000
    assert cdmi_str_to_acemask("WRITE_ACL", True) == 0x00040000
    assert cdmi_str_to_acemask("READ_ACL", True) == 0x00020000
    assert cdmi_str_to_acemask("DELETE", True) == 0x00010000
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
    
    assert cdmi_str_to_acemask("none", False) == 0x0
    assert cdmi_str_to_acemask("read", False) == 0x09
    assert cdmi_str_to_acemask("write", False) == 0x56
    assert cdmi_str_to_acemask("read/write", False) == 0x56 | 0x09
    assert cdmi_str_to_acemask("edit", False) == 0x56
    assert cdmi_str_to_acemask("delete", False) == 0x10040
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


def test_serialize_acl_metadata():
    cfg.dse_keyspace = TEST_KEYSPACE
    connect()

    coll = Collection.find("/")
    acl = serialize_acl_metadata(coll)
    assert acl['cdmi_acl'][0]['acetype'] == "ALLOW"
    assert acl['cdmi_acl'][0]['identifier'] == "AUTHENTICATED@"
    assert acl['cdmi_acl'][0]['aceflags'] == 'CONTAINER_INHERIT, OBJECT_INHERIT'
    assert acl['cdmi_acl'][0]['acemask'] == 'READ_METADATA, LIST_CONTAINER'


