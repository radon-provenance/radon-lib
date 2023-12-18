# Copyright 2023
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

import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError 

from radon.model.config import cfg
from radon.util import (
    datetime_serializer,
    payload_check,
)

OP_CREATE = "create"
OP_DELETE = "delete"
OP_UPDATE = "update"
OPT_REQUEST = "request"
OPT_SUCCESS = "success"
OPT_FAIL = "fail"
OBJ_RESOURCE = "resource"      # path
OBJ_COLLECTION = "collection"  # path
OBJ_USER = "user"              # name
OBJ_GROUP = "group"            # name


P_META_MSG = "/meta/msg"
P_META_SENDER = "/meta/sender"

MSG_CREATE_FAILED = "Create failed"
MSG_UPDATE_FAILED = "update Failed"
MSG_DELETE_FAILED = "Delete failed"


# Fields for the metadata part
fields_meta = {
    "sender" : {"type" : "string"},
    "msg" : {"type" : "string"},
}

# Schema for the metadata part
s_meta = {
    "type" : "object",
    "properties" : fields_meta,
}


# Fields for a user (key = login)
fields_user = {
    "uuid": {"type" : "string"},
    "login": {"type" : "string"},
    "fullname": {"type" : "string"},
    "password": {"type" : "string"},
    "email": {"type" : "string"},
    "administrator": {"type" : "boolean"},
    "active": {"type" : "boolean"},
    "ldap": {"type" : "boolean"},
    "groups": {"type": "array",
               "items": {
                   "type": "string"
                }
              }
}

# Fields for a group (key = name)
fields_group = {
    "uuid": {"type" : "string"},
    "name": {"type" : "string"},
    "create_ts": {"type" : "string"},
}

# Fields for a collection (key = path)
fields_coll = {
    "uuid": {"type" : "string"},
    "container": {"type" : "string"},
    "name": {"type" : "string"},
    "path": {"type" : "string"},
    "created": {"type" : "string"},
    "user_meta": {"type" : "object"},
    "sys_meta": {"type" : "object"},
    
    "can_read": {"type" : "boolean"},
    "can_write": {"type" : "boolean"},
    "can_edit": {"type" : "boolean"},
    "can_delete": {"type" : "boolean"},
}

# Fields for a resource (key = path)
fields_resc = {
    "uuid": {"type" : "string"},
    "container": {"type" : "string"},
    "name": {"type" : "string"},
    "path": {"type" : "string"},
    "is_reference": {"type" : "boolean"},
    "mimetype": {"type" : "string"},
    "type": {"type" : "string"},
    "size": {"type" : "integer"},
    "created": {"type" : "string"},
    "user_meta": {"type" : "object"},
    "sys_meta": {"type" : "object"},
    
    "can_read": {"type" : "boolean"},
    "can_write": {"type" : "boolean"},
    "can_edit": {"type" : "boolean"},
    "can_delete": {"type" : "boolean"},
}

# Schemas for a user
s_user = {
    "type" : "object",
    "properties" : {

        "obj" : {
            "type" : "object",
            "properties" : fields_user,
            "required" : ["login"],
            "additionalProperties": False
        },

        "meta" : s_meta,
    },
    "required" : ["obj"]
}
s_user_request = {
    "type" : "object",
    "properties" : {

        "obj" : {
            "type" : "object",
            "properties" : fields_user,
            "required" : ["login", "password"],
            "additionalProperties": False
        },

        "meta" : s_meta,
    },
    "required" : ["obj"]
}

# Schema for a group
s_group = {
    "type" : "object",
    "properties" : {

        "obj" : {
            "type" : "object",
            "properties" : fields_group,
            "required" : ["name"],
            "additionalProperties": False
        },

        "meta" : s_meta,
    }
}

# Schema for a collection
s_coll = {
    "type" : "object",
    "properties" : {

        "obj" : {
            "type" : "object",
            "properties" : fields_coll,
            "required" : ["path"],
            "additionalProperties": False
        },

        "meta" : s_meta,
    }
}

# Schema for a resource
s_resc = {
    "type" : "object",
    "properties" : {

        "obj" : {
            "type" : "object",
            "properties" : fields_resc,
            "required" : ["path"],
            "additionalProperties": False
        },

        "meta" : s_meta,
    }
}


class Payload(object):
    
    def __init__(self, op_name, op_type, obj_type, json):
        self.op_name = op_name
        self.op_type = op_type
        self.obj_type = obj_type
        self.json = json
        self.schema = {}
        if 'meta' not in self.json:
            self.json['meta'] = {}
        sender = payload_check(P_META_SENDER, self.json)
        if not sender:
            self.json['meta']['sender'] = cfg.sys_lib_user


    def get_json(self):
        return self.json


    def get_operation_name(self):
        return self.op_name


    def get_operation_type(self):
        return self.op_type


    def get_object_type(self):
        return self.obj_type


    def get_object_key(self):
        if self.obj_type in [OBJ_RESOURCE, OBJ_COLLECTION]:
            return payload_check("/obj/path", self.json, "Unknown_path")
        elif self.obj_type == OBJ_USER:
            return payload_check("/obj/login", self.json, "Unknown_user")
        elif self.obj_type == OBJ_GROUP:
            return payload_check("/obj/name", self.json, "Unknown_group")
        else:
            return "Unknown_Object"


    def get_sender(self):
        return payload_check(P_META_SENDER, self.json, cfg.sys_lib_user)


    def set_msg(self, msg):
        self.json['meta']['msg'] = msg


    def validate(self):
        try:
            validate(self.json, self.schema)
        except ValidationError as e:
            return (False, e.message)
        return (True, "json is valid")


    def __repr__(self):
        return json.dumps(self.json, default=datetime_serializer)


################################################################################


class PayloadCreate(Payload):

    def __init__(self, op_type, obj_type, json):
        super().__init__(OP_CREATE, op_type, obj_type, json)

class PayloadUpdate(Payload):

    def __init__(self, op_type, obj_type, json):
        super().__init__(OP_UPDATE, op_type, obj_type, json)

class PayloadDelete(Payload):

    def __init__(self, op_type, obj_type, json):
        super().__init__(OP_DELETE, op_type, obj_type, json)


################################################################################


class PayloadCreateRequest(PayloadCreate):

    def __init__(self, obj_type, json):
        super().__init__(OPT_REQUEST, obj_type, json)

class PayloadCreateSuccess(PayloadCreate):

    def __init__(self, obj_type, json):
        super().__init__(OPT_SUCCESS, obj_type, json)

class PayloadCreateFail(PayloadCreate):

    def __init__(self, obj_type, json):
        if not payload_check(P_META_MSG, json):
            json['meta']['msg'] = MSG_CREATE_FAILED
        super().__init__(OPT_FAIL, obj_type, json)

####################################

class PayloadUpdateRequest(PayloadUpdate):

    def __init__(self, obj_type, json):
        super().__init__(OPT_REQUEST, obj_type, json)

class PayloadUpdateSuccess(PayloadUpdate):

    def __init__(self, obj_type, json):
        super().__init__(OPT_SUCCESS, obj_type, json)

class PayloadUpdateFail(PayloadUpdate):

    def __init__(self, obj_type, json):
        super().__init__(OPT_FAIL, obj_type, json)

####################################

class PayloadDeleteRequest(PayloadDelete):

    def __init__(self, obj_type, json):
        super().__init__(OPT_REQUEST, obj_type, json)

class PayloadDeleteSuccess(PayloadDelete):

    def __init__(self, obj_type, json):
        super().__init__(OPT_SUCCESS, obj_type, json)

class PayloadDeleteFail(PayloadDelete):

    def __init__(self, obj_type, json):
        super().__init__(OPT_FAIL, obj_type, json)


################################################################################


class PayloadCreateRequestCollection(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadCreateRequestGroup(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadCreateRequestResource(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadCreateRequestUser(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user_request

####################################

class PayloadCreateSuccessCollection(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadCreateSuccessGroup(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadCreateSuccessResource(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadCreateSuccessUser(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadCreateFailCollection(PayloadCreateFail):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadCreateFailCollection(payload_json)


class PayloadCreateFailGroup(PayloadCreateFail):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"name" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadCreateFailGroup(payload_json)


class PayloadCreateFailResource(PayloadCreateFail):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadCreateFailResource(payload_json)


class PayloadCreateFailUser(PayloadCreateFail):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"login" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadCreateFailUser(payload_json)


####################################
####################################
####################################


class PayloadUpdateRequestCollection(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadUpdateRequestGroup(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadUpdateRequestResource(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadUpdateRequestUser(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadUpdateSuccessCollection(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadUpdateSuccessGroup(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadUpdateSuccessResource(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadUpdateSuccessUser(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadUpdateFailCollection(PayloadUpdateFail):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadUpdateFailCollection(payload_json)


class PayloadUpdateFailGroup(PayloadUpdateFail):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"name" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadUpdateFailGroup(payload_json)


class PayloadUpdateFailResource(PayloadUpdateFail):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadUpdateFailResource(payload_json)


class PayloadUpdateFailUser(PayloadUpdateFail):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"login" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadUpdateFailUser(payload_json)


####################################
####################################
####################################

class PayloadDeleteRequestCollection(PayloadDeleteRequest):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

    @classmethod
    def default(cls, key, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {"sender": sender}
        }
        return PayloadDeleteRequestCollection(payload_json)


class PayloadDeleteRequestGroup(PayloadDeleteRequest):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group


class PayloadDeleteRequestResource(PayloadDeleteRequest):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

    @classmethod
    def default(cls, key, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {"sender": sender}
        }
        return PayloadDeleteRequestResource(payload_json)


class PayloadDeleteRequestUser(PayloadDeleteRequest):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadDeleteSuccessCollection(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadDeleteSuccessGroup(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadDeleteSuccessResource(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadDeleteSuccessUser(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadDeleteFailCollection(PayloadDeleteFail):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadDeleteFailCollection(payload_json)


class PayloadDeleteFailGroup(PayloadDeleteFail):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"name" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadDeleteFailGroup(payload_json)


class PayloadDeleteFailResource(PayloadDeleteFail):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"path" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadDeleteFailResource(payload_json)


class PayloadDeleteFailUser(PayloadDeleteFail):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

    @classmethod
    def default(cls, key, msg, sender=cfg.sys_lib_user):
        """Create a new payload with minimal information""" 
        payload_json = {
            "obj": {"login" : key},
            "meta": {
                "sender": sender,
                "msg": msg
            }
        }
        return PayloadDeleteFailUser(payload_json)






