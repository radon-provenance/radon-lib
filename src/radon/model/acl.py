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


# aceflag: an int value taken from the constant ACEFLAG_*
# acemask:  an int value taken from the constant ACEMASK_*
# str: A simplified access level name (read, write, read/write or null)
# cdmi_str: A comma separated list of flags or masks, used in cdmi
#             ("READ_ACL, READ_ATTRIBUTES", ..)


from dse.cqlengine.usertype import UserType
from dse.cqlengine import columns
from collections import OrderedDict

from radon.model.config import cfg


# ACE Flags for ACL in CDMI
ACEFLAG_NONE = 0x00000000
ACEFLAG_OBJECT_INHERIT = 0x00000001
ACEFLAG_CONTAINER_INHERIT = 0x00000002
ACEFLAG_NO_PROPAGATE = 0x00000004
ACEFLAG_INHERIT_ONLY = 0x00000008
ACEFLAG_IDENTIFIER_GROUP = 0x00000040
ACEFLAG_INHERITED = 0x00000080

# ACE Mask bits for ACL in CDMI
ACEMASK_READ_OBJECT = 0x00000001
ACEMASK_LIST_CONTAINER = 0x00000001
ACEMASK_WRITE_OBJECT = 0x00000002
ACEMASK_ADD_OBJECT = 0x00000002
ACEMASK_APPEND_DATA = 0x00000004
ACEMASK_ADD_SUBCONTAINER = 0x00000004
ACEMASK_READ_METADATA = 0x00000008
ACEMASK_WRITE_METADATA = 0x00000010
ACEMASK_EXECUTE = 0x00000020
ACEMASK_DELETE_OBJECT = 0x00000040
ACEMASK_DELETE_SUBCONTAINER = 0x00000040
ACEMASK_READ_ATTRIBUTES = 0x00000080
ACEMASK_WRITE_ATTRIBUTES = 0x00000100
ACEMASK_WRITE_RETENTION = 0x00000200
ACEMASK_WRITE_RETENTION_HOLD = 0x00000400
ACEMASK_DELETE = 0x00010000
ACEMASK_READ_ACL = 0x00020000
ACEMASK_WRITE_ACL = 0x00040000
ACEMASK_WRITE_OWNER = 0x00080000
ACEMASK_SYNCHRONIZE = 0x00100000

ACCESS_STR_NONE = "none"
ACCESS_STR_READ = "read"
ACCESS_STR_WRITE = "write"
ACCESS_STR_RW = "read/write"

# Ace flags table
ACEFLAG_TABLE = [
    (0x00000080, "INHERITED"),
    (0x00000040, "IDENTIFIER_GROUP"),
    (0x00000008, "INHERIT_ONLY"),
    (0x00000004, "NO_PROPAGATE"),
    (0x00000002, "CONTAINER_INHERIT"),
    (0x00000001, "OBJECT_INHERIT"),
    (0x00000000, "NO_FLAGS"),
]

# Ace masks table
ACEMASK_TABLE = [
    (0x00100000, "SYNCHRONIZE", "SYNCHRONIZE"),
    (0x00080000, "WRITE_OWNER", "WRITE_OWNER"),
    (0x00040000, "WRITE_ACL", "WRITE_ACL"),
    (0x00020000, "READ_ACL", "READ_ACL"),
    (0x00010000, "DELETE", "DELETE"),
    (0x00000400, "WRITE_RETENTION_HOLD", "WRITE_RETENTION_HOLD"),
    (0x00000200, "WRITE_RETENTION", "WRITE_RETENTION"),
    (0x00000100, "WRITE_ATTRIBUTES", "WRITE_ATTRIBUTES"),
    (0x00000080, "READ_ATTRIBUTES", "READ_ATTRIBUTES"),
    (0x00000040, "DELETE_OBJECT", "DELETE_SUBCONTAINER"),
    (0x00000020, "EXECUTE", "EXECUTE"),
    (0x00000010, "WRITE_METADATA", "WRITE_METADATA"),
    (0x00000008, "READ_METADATA", "READ_METADATA"),
    (0x00000004, "APPEND_DATA", "ADD_SUBCONTAINER"),
    (0x00000002, "WRITE_OBJECT", "ADD_OBJECT"),
    (0x00000001, "READ_OBJECT", "LIST_CONTAINER"),
]

ACEMASK_STR_INT_OBJ = {
    "NONE": 0x0,
    "READ": ACEMASK_READ_OBJECT | ACEMASK_READ_METADATA,  # 0x09
    "WRITE": 0x56,  # ACEMASK_WRITE_OBJECT | ACEMASK_APPEND_DATA |
    # ACEMASK_WRITE_METADATA | ACEMASK_DELETE_OBJECT,
    "READ/WRITE": 0x56 | 0x09,
    "EDIT": 0x56,
    "DELETE": ACEMASK_DELETE,
    "SYNCHRONIZE": 0x00100000,
    "WRITE_OWNER": 0x00080000,
    "WRITE_ACL": 0x00040000,
    "READ_ACL": 0x00020000,
    "WRITE_RETENTION_HOLD": 0x00000400,
    "WRITE_RETENTION": 0x00000200,
    "WRITE_ATTRIBUTES": 0x00000100,
    "READ_ATTRIBUTES": 0x00000080,
    "DELETE_OBJECT": 0x00000040,
    "EXECUTE": 0x00000020,
    "WRITE_METADATA": 0x00000010,
    "READ_METADATA": 0x00000008,
    "APPEND_DATA": 0x00000004,
    "WRITE_OBJECT": 0x00000002,
    "READ_OBJECT": 0x00000001,
}

ACEMASK_INT_STR_OBJ = {
    0x0: ACCESS_STR_NONE,
    0x09: ACCESS_STR_READ,
    0x56: ACCESS_STR_WRITE,
    0x5F: ACCESS_STR_RW,
}

ACEMASK_STR_INT_COL = {
    "NONE": 0x0,
    "READ": ACEMASK_LIST_CONTAINER | ACEMASK_READ_METADATA,
    "WRITE": 0x56,  # ACEMASK_ADD_OBJECT | ACEMASK_ADD_SUBCONTAINER |
    # ACEMASK_WRITE_METADATA | ACEMASK_DELETE_SUBCONTAINER,
    "READ/WRITE": 0x56 | 0x09,
    "EDIT": 0x56,
    "DELETE": (ACEMASK_DELETE | ACEMASK_DELETE_OBJECT | ACEMASK_DELETE_SUBCONTAINER),
    "SYNCHRONIZE": 0x00100000,
    "WRITE_OWNER": 0x00080000,
    "WRITE_ACL": 0x00040000,
    "READ_ACL": 0x00020000,
    "WRITE_RETENTION_HOLD": 0x00000400,
    "WRITE_RETENTION": 0x00000200,
    "WRITE_ATTRIBUTES": 0x00000100,
    "READ_ATTRIBUTES": 0x00000080,
    "DELETE_SUBCONTAINER": 0x00000040,
    "EXECUTE": 0x00000020,
    "WRITE_METADATA": 0x00000010,
    "READ_METADATA": 0x00000008,
    "ADD_SUBCONTAINER": 0x00000004,
    "ADD_OBJECT": 0x00000002,
    "LIST_CONTAINER": 0x00000001,
}

ACEMASK_INT_STR_COL = {
    0x0: ACCESS_STR_NONE,
    0x09: ACCESS_STR_READ,
    0x56: ACCESS_STR_WRITE,
    0x5F: ACCESS_STR_RW,
}

ACEFLAG_STR_INT = {
    "INHERITED": 0x00000080,
    "IDENTIFIER_GROUP": 0x00000040,
    "INHERIT_ONLY": 0x00000008,
    "NO_PROPAGATE": 0x00000004,
    "CONTAINER_INHERIT": 0x00000002,
    "OBJECT_INHERIT": 0x00000001,
    "NO_FLAGS": 0x00000000,
}


def aceflag_to_cdmi_str(num_value):
    """Return the string value for ACE flag value given

    Return the string value for the ACE flag value given. It returns a text
    expression simpler to understand.

    :param num_value: ACE flag numeric value
    :type num_value: integer
    
    :return: The string value for the aceflag
    :rtype: string
    """
    if num_value == 0:
        return "NO_FLAGS"
    res = []
    for idx in range(len(ACEFLAG_TABLE)):
        if num_value == 0:
            return ', '.join(res)

        if num_value & ACEFLAG_TABLE[idx][0] == ACEFLAG_TABLE[idx][0]:
            res.append(ACEFLAG_TABLE[idx][1])
            num_value = num_value ^ ACEFLAG_TABLE[idx][0]
    return ', '.join(res)


def acemask_to_cdmi_str(num_value, is_object):
    """Return the string value for ACE mask value given.

    Return the string value for the ACE mask value given. It returns a
    text expression simpler to understand.

    :param num_value: ACE mask numeric value
    :type num_value: integer
    :param is_object: True if the ACE relates to an object, False for a collection
    :type is_object: boolean
    
    :return: The string value for the acemask
    :rtype: string
    """
    res = []
    for idx in range(len(ACEMASK_TABLE)):
        if num_value == 0:
            return ', '.join(res)

        if num_value & ACEMASK_TABLE[idx][0] == ACEMASK_TABLE[idx][0]:
            if is_object:
                res.append(ACEMASK_TABLE[idx][1])
            else:
                res.append(ACEMASK_TABLE[idx][2])
            num_value = num_value ^ ACEMASK_TABLE[idx][0]
    return ', '.join(res)


def acemask_to_str(acemask, is_object):
    """Return the simplified access level from an acemask
    
    :param acemask: is in [0x0, 0x09, 0x56, 0x5F] which is translated to
                    "none, "read", "write" or "read/write"
    :type acemask: int
    :param is_object: defined if the mask corresponds to a data object or a
                     collection. They are similar but the CDMI standard 
                     separate them so we may need the option
    :type is_object: bool

    :return: the string that corresponds to the mask
    :rtype: str
    """
    if is_object:
        return ACEMASK_INT_STR_OBJ.get(acemask, "")
    else:
        return ACEMASK_INT_STR_COL.get(acemask, "")


def acl_cdmi_to_cql(cdmi_acl):
    """Convert a list of ACL for groups stored in cdmi format to the cql 
    string used to update the Cassandra model
    
    :param cdmi_acl: a cdmi string for
    :type cdmi_acl: List[dict]
    
    :return: A CQL string that can be used to update values in Cassandra
    :rtype: str
    """
    from radon.model.group import Group
    ls_access = []
    for cdmi_ace in cdmi_acl:
        if 'identifier' in cdmi_ace:
            gid = cdmi_ace['identifier']
        else:
            # Wrong syntax for the ace
            cfg.logger.warning(
                "Wrong format for the cdmi string for ACL, 'identifier' field not found"
            )
            continue
        group = Group.find(gid)
        if group:
            ident = group.name
        elif gid.upper() == "AUTHENTICATED@":
            ident = "AUTHENTICATED@"
        elif gid.upper() == "ANONYMOUS@":
            ident = "ANONYMOUS@"
        else:
            cfg.logger.warning(
                "Wrong format for the cdmi string for ACL, {} group not found".format(
                    gid)
            )
            continue
        s = (u"'{}': {{"
              "acetype: '{}', "
              "identifier: '{}', "
              "aceflags: {}, "
              "acemask: {}"
              "}}").format(ident,
                          cdmi_ace['acetype'].upper(),
                          ident,
                          cdmi_str_to_aceflag(cdmi_ace['aceflags']),
                          cdmi_str_to_acemask(cdmi_ace['acemask'], False)
                         )
        ls_access.append(s)
    acl = u"{{{}}}".format(", ".join(ls_access))
    return acl


def acl_list_to_cql(read_access, write_access):
    """Convert a list of read/write access for groups to the cql string used
    to update the Cassandra model
    
    :param read_access: A list of group names which have read access
    :type read_access: List[str]
    :param write_access: A list of group names which have write access
    :type write_access: List[str]

    :return: A CQL string that can be used to update values in Cassandra
    :rtype: str
    """
    from radon.model.group import Group
    access = {}
    for gname in read_access:
        access[gname] = ACCESS_STR_READ
    for gname in write_access:
        if gname in access:
            access[gname] = ACCESS_STR_RW
        else:
            access[gname] = ACCESS_STR_WRITE
    ls_access = []
    for gname in access:
        g = Group.find(gname)
        if g:
            ident = g.name
        elif gname.upper() == "AUTHENTICATED@":
            ident = "AUTHENTICATED@"
        elif gname.upper() == "ANONYMOUS@":
            ident = "ANONYMOUS@"
        else: 
            cfg.logger.warning(
                "The group {0} doesn't exist".format(
                    gname
                )
            )
            continue
        s = (
            u"'{}': {{"
            "acetype: 'ALLOW', "
            "identifier: '{}', "
            "aceflags: {}, "
            "acemask: {}"
            "}}"
        ).format(ident, ident, 0, str_to_acemask(access[gname], False))
        ls_access.append(s)
    acl = u"{{{}}}".format(", ".join(ls_access))
    return acl


def cdmi_str_to_aceflag(cdmi_str):
    """
    Return the aceflag from a cdmi string
    
    :param cdmi_str: A comma-separated list of aceflags
    :type cdmi_str: str
    
    :return: The aceflag created from the string
    :rtype: int
    """
    aceflag = 0
    ls_flag = cdmi_str.split(",")
    for flag in ls_flag:
        flag = flag.strip().upper()
        aceflag |= ACEFLAG_STR_INT.get(flag, 0)
    return aceflag


def cdmi_str_to_acemask(cdmi_str, is_object):
    """
    Return the aceflag from a cdmi string
    
    :param cdmi_str: A comma-separated list of aceflags
    :type cdmi_str: str
    :param is_object: True if the ACE relates to an object, False for a collection
    :type is_object: boolean
    
    :return: The aceflag created from the string
    :rtype: int
    """
    if is_object:
        map_dict = ACEMASK_STR_INT_OBJ
    else:
        map_dict = ACEMASK_STR_INT_COL
    acemask = 0
    ls_mask = cdmi_str.split(",")
    for mask in ls_mask:
        mask = mask.strip().upper()
        acemask |= map_dict.get(mask, 0)
    return acemask


def serialize_acl_metadata(obj):
    """
    Create a dictionary of acl from object metadata (stored in Cassandra
    dictionary)
    
    :param obj: A Collection or a resource
    :type obj: :class:`radon.model.Collection` or :class:`radon.model.Resource`
    """
    from radon.model.resource import Resource
    is_object = isinstance(obj, Resource)
    mapped_md = []
    # Create a list of ACE from the dictionary we created
    for _, ace in obj.node.acl.items():
        acl_md = OrderedDict()
        acl_md["acetype"] = ace.acetype
        acl_md["identifier"] = ace.identifier
        aceflags = ACEFLAG_OBJECT_INHERIT | ACEFLAG_CONTAINER_INHERIT
        acl_md["aceflags"] = aceflag_to_cdmi_str(aceflags)
        acemask = ace.acemask
        acl_md["acemask"] = acemask_to_cdmi_str(acemask, is_object)
        mapped_md.append(acl_md)
    return {"cdmi_acl": mapped_md}


def str_to_acemask(lvl, is_object):
    """Return the acemask from a simplified access level
    
    :param lvl: the access level to map, in  "none, "read", "write" or 
                "read/write"
    :type lvl: str
    :param is_object: defined if the level corresponds to a data object or a
                     collection.
    :type is_object: bool
    
    :return: The corresponding acemask, in [0x0, 0x09, 0x56, 0x5F]
    :rtype: int
    """
    if is_object:
        return ACEMASK_STR_INT_OBJ.get(lvl.upper(), 0)
    else:
        return ACEMASK_STR_INT_COL.get(lvl.upper(), 0)


class Ace(UserType):
    """A user type to describe an Access Control Entry for a specific user
    or group. This can be used in an Access Control List in a resource or a
    collection

    The two types of ACEs in CDMI are ALLOW and DENY. An ALLOW ACE grants some
    form of access to a principal. Principals are either users or groups and
    are represented by identifiers. A DENY ACE denies access of some kind to a
    principal.

    In addition to principals some special identifiers can be used:
      - "EVERYONE@": The world
      - "ANONYMOUS@": Access without authentication
      - "AUTHENTICATED@": Any authenticated user (opposite of ANONYMOUS)

    CDMI allows for nested containers and mandates that objects and
    subcontainers be able to inherit access permissions from their parent
    containers. However, it is not enough to simply inherit all permissions
    from the parent; it might be desirable, for example, to have different
    default permissions on child objects and subcontainers of a given
    container.

    The mask field of an ACE contains a set of permissions allowed or denied.

    :param acetype: ALLOW or DENY
    :type acetype: :class:`columns.Text`
    :param identifier: groups the ACL refers to
    :type identifier: :class:`columns.Text`
    :param aceflags: not used yet
    :type aceflags: :class:`columns.Integer`
    :param acemask: the mask that defines the access (read, write, r/w or none)
    :type acemask: :class:`columns.Integer`
    """

    acetype = columns.Text()
    identifier = columns.Text()
    # aceflags isn't used yet, future versions may use it
    aceflags = columns.Integer()
    acemask = columns.Integer()
    
    
    def __str__(self):
        """ 
        ACE object represented as a string
        
        :return: A string representation
        :rtype: str
        """
        return "(acetype: {}, identifier: {}, aceflags: {}, acemask: {})".format(
            self.acetype,
            self.identifier,
            self.aceflags,
            self.acemask)




