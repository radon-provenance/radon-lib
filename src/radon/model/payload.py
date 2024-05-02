# Radon Copyright 2023, University of Oxford
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
from radon.util import (
    new_request_id,
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
P_META_REQ_ID = "/meta/req_id"

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
    "members": {"type" : "array"},
}

# Fields for a collection (key = path)
fields_coll = {
    "uuid": {"type" : "string"},
    "container": {"type" : "string"},
    "name": {"type" : "string"},
    "path": {"type" : "string"},
    "created": {"type" : "string"},
    "create_ts": {"type" : "string"},
    "modify_ts": {"type" : "string"},
    "user_meta": {"type" : "object"},
    "sys_meta": {"type" : "object"},
    "metadata": {"type" : "object"},
    "read_access": {"type" : "array"},
    "write_access": {"type" : "array"},
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
    "create_ts": {"type" : "string"},
    "modify_ts": {"type" : "string"},
    "user_meta": {"type" : "object"},
    "sys_meta": {"type" : "object"},
    "metadata": {"type" : "object"},
    "read_access": {"type" : "array"},
    "write_access": {"type" : "array"},
    "url": {"type" : "string"},
    "data": {"type" : "string"},
    
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
    },
    "required" : ["obj"]
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
    },
    "required" : ["obj"]
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
    },
    "required" : ["obj"]
}


class Payload(object):
    """Payload
    
    This is used to share information between components in the MQTT payload, 
    each notifiaction has its own JSON schema it can use to validate that the 
    payload contains the minimal information needed. This is the base class, 
    every notification payload inherits from here
    """
    
    def __init__(self, op_name, op_type, obj_type, json):
        """Constructor
        
        :param op_name: The operation name (create, delete or update)
        :type op_name: str
        :param op_type: The operation type(request, success, fail)
        :type op_type: str
        :param obj_type: The type of the associated object(collection, resource,
           group, user)
        :type obj_type: str
        :param json: The JSON dict that will be send  in the MQTT payload
        :type json: dict
        """
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
        req_id = payload_check(P_META_REQ_ID, self.json)
        if not req_id:
            self.json['meta']['req_id'] = new_request_id()


    def get_json(self):
        """
        Get the json dictionary that will be sent to MQTT
        
        :return: The json dict
        :rtype: dict
        """
        return self.json


    def get_operation_name(self):
        """
        Get the Operation name
        
        :return: The operation name
        :rtype: str
        """
        return self.op_name


    def get_operation_type(self):
        """
        Get the Operation type
        
        :return: The operation type
        :rtype: str
        """
        return self.op_type


    def get_object_type(self):
        """
        Get the Object type
        
        :return: The object type
        :rtype: str
        """
        return self.obj_type


    def get_object_key(self):
        """
        Get the key which can uniquely identifies the object in Cassandra,
        depending on its type. It has to be found in the payload dict, at the 
        right path ('/obj/path', '/obj/login' or '/onj/name').
        
        :return: The key that can be used in the CQL query
        :rtype: str
        """
        if self.obj_type in [OBJ_RESOURCE, OBJ_COLLECTION]:
            return payload_check("/obj/path", self.json, "Unknown_path")
        elif self.obj_type == OBJ_USER:
            return payload_check("/obj/login", self.json, "Unknown_user")
        elif self.obj_type == OBJ_GROUP:
            return payload_check("/obj/name", self.json, "Unknown_group")
        else:
            return "Unknown_Object"


    def get_req_id(self):
        """
        Try to find the defined id which has been used when the notification
        was sent. If no information is found in the payload dict it generates a
        new one so we can have a link somehow between request and result
        
        :return: The request id
        :rtype: str
        """
        return payload_check(P_META_REQ_ID, self.json, new_request_id())


    def get_sender(self):
        """
        Try to find the sender who is trying to send the notification at 
        '/msg/sender'. If no information is found in the payload dict it 
        assumes that it comes from the system
        
        :return: The name of the sender
        :rtype: str
        """
        return payload_check(P_META_SENDER, self.json, cfg.sys_lib_user)


    def validate(self):
        """
        Validate the json according to the associated schema for this type
        of notification
        
        :return: The result of the validation and an error message
        :rtype: Tuple(bool, str)
        """
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
            if 'meta' not in json:
                json['meta'] = {}
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


class PayloadCreateCollectionRequest(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadCreateGroupRequest(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadCreateResourceRequest(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadCreateUserRequest(PayloadCreateRequest):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user_request

####################################

class PayloadCreateCollectionSuccess(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadCreateGroupSuccess(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadCreateResourceSuccess(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadCreateUserSuccess(PayloadCreateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadCreateCollectionFail(PayloadCreateFail):

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
        return PayloadCreateCollectionFail(payload_json)


class PayloadCreateGroupFail(PayloadCreateFail):

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
        return PayloadCreateGroupFail(payload_json)


class PayloadCreateResourceFail(PayloadCreateFail):

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
        return PayloadCreateResourceFail(payload_json)


class PayloadCreateUserFail(PayloadCreateFail):

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
        return PayloadCreateUserFail(payload_json)


####################################
####################################
####################################


class PayloadUpdateCollectionRequest(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadUpdateGroupRequest(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadUpdateResourceRequest(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadUpdateUserRequest(PayloadUpdateRequest):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadUpdateCollectionSuccess(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadUpdateGroupSuccess(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadUpdateResourceSuccess(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadUpdateUserSuccess(PayloadUpdateSuccess):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadUpdateCollectionFail(PayloadUpdateFail):

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
        return PayloadUpdateCollectionFail(payload_json)


class PayloadUpdateGroupFail(PayloadUpdateFail):

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
        return PayloadUpdateGroupFail(payload_json)


class PayloadUpdateResourceFail(PayloadUpdateFail):

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
        return PayloadUpdateResourceFail(payload_json)


class PayloadUpdateUserFail(PayloadUpdateFail):

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
        return PayloadUpdateUserFail(payload_json)


####################################
####################################
####################################

class PayloadDeleteCollectionRequest(PayloadDeleteRequest):

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
        return PayloadDeleteCollectionRequest(payload_json)


class PayloadDeleteGroupRequest(PayloadDeleteRequest):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group


class PayloadDeleteResourceRequest(PayloadDeleteRequest):

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
        return PayloadDeleteResourceRequest(payload_json)


class PayloadDeleteUserRequest(PayloadDeleteRequest):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadDeleteCollectionSuccess(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_COLLECTION, json)
        self.schema = s_coll

class PayloadDeleteGroupSuccess(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_GROUP, json)
        self.schema = s_group

class PayloadDeleteResourceSuccess(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_RESOURCE, json)
        self.schema = s_resc

class PayloadDeleteUserSuccess(PayloadDeleteSuccess):

    def __init__(self, json):
        super().__init__(OBJ_USER, json)
        self.schema = s_user

####################################

class PayloadDeleteCollectionFail(PayloadDeleteFail):

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
        return PayloadDeleteCollectionFail(payload_json)


class PayloadDeleteGroupFail(PayloadDeleteFail):

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
        return PayloadDeleteGroupFail(payload_json)


class PayloadDeleteResourceFail(PayloadDeleteFail):

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
        return PayloadDeleteResourceFail(payload_json)


class PayloadDeleteUserFail(PayloadDeleteFail):

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
        return PayloadDeleteUserFail(payload_json)






