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


from dse.cqlengine import columns
from dse.cqlengine.models import connection, Model
from dse.query import SimpleStatement
import json
import logging
import paho.mqtt.publish as publish
 
import radon
from radon.util import (
    datetime_serializer,
    default_date,
    default_time,
    default_uuid,
    last_x_days,
    merge,
    payload_check,
)

# Operations that could lead to a new notification

OP_CREATE = "create"
OP_DELETE = "delete"
OP_UPDATE = "update"

# Operations types

OPT_REQUEST = "request"
OPT_SUCCESS = "success"
OPT_FAIL = "fail"

# Types of objects with the element needed to identify the object
OBJ_RESOURCE = "resource"      # path
OBJ_COLLECTION = "collection"  # path
OBJ_USER = "user"              # name
OBJ_GROUP = "group"            # name

P_META_MSG = "/meta/msg"
P_META_SENDER = "/meta/sender"
P_OBJ_CONTAINER = "/obj/container"
P_OBJ_NAME = "/obj/name"
P_OBJ_LOGIN = "/obj/login"
P_PRE_NAME = "/pre/name"
P_PRE_CONTAINER = "/pre/container"

MSG_CREATE_FAILED = "Create failed"
MSG_UPDATE_FAILED = "update Failed"
MSG_DELETE_FAILED = "Delete failed"
MSG_INFO_MISSING = "Information is missing for the {}: {}"



class Notification(Model):
    """Notification Model
    
    This is used to store notifications for later use (audit or rule engine)
    
    :param date: A simple date used to partition notification
    :type date: :class:`columns.Text`
    :param when: The full date/time for the notification
    :type when: :class:`columns.TimeUUID`
    :param op_name: The name of operation (create, delete, update)
    :type op_name: :class:`columns.Text`    
    :param op_type: The specific type of operation (request, success, fail)
    :type op_type: :class:`columns.Text`
    :param obj_type: The type of the object concerned (Collection, Resource,
      User, Group, ...)
    :type obj_type: :class:`columns.Text`
    :param obj_key: The key of the object concerned, the key used to find
      the object
    :type obj_key: :class:`columns.Text`
    :param sender: The user who initiates the operation
    :type sender: :class:`columns.Text`
    :param processed: True if a successful workflow has been executed for this
      notification
    :type processed: :class:`columns.Boolean`
    :param payload: The payload of the message which is sent to MQTT
    :type payload: :class:`columns.Text`
    
    """

    date = columns.Text(default=default_date, partition_key=True)
    when = columns.TimeUUID(
        primary_key=True, default=default_time, clustering_order="DESC"
    )
    # The type of operation (create, delete, update, [Index, Move, ...])
    op_name = columns.Text(primary_key=True)
    # The subtype of operation (request, success, fail)
    op_type = columns.Text(primary_key=True)
    # The type of the object concerned (Collection, Resource, User, Group, ...)
    obj_type = columns.Text(primary_key=True)
    # The uuid of the object concerned, the key used to find the corresponding
    # object (path, uuid, ...)
    obj_key = columns.Text(primary_key=True)

    # The user who initiates the operation
    sender = columns.Text()
    # True if the corresponding workflow has been executed correctly (for Move
    # or indexing for instance)
    # True if nothing has to be done
    processed = columns.Boolean()
    # The payload of the message which is sent to MQTT
    payload = columns.Text()
    
    
    def mqtt_publish(self):
        """
        Try to publish the notification on MQTT
        """
        topic = u"{0}/{1}/{2}/{3}".format(
            self.op_name,
            self.op_type, 
            self.obj_type, 
            self.obj_key)
        # Clean up the topic by removing superfluous slashes.
        topic = "/".join(filter(None, topic.split("/")))
        # Remove MQTT wildcards from the topic. Corner-case: If the collection name is made entirely of # and + and a
        # script is set to run on such a collection name. But that's what you get if you use stupid names for things.
        topic = topic.replace("#", "").replace("+", "")
        logging.info(u'Publishing on topic "{0}"'.format(topic))
        try:
            publish.single(topic, self.payload, hostname=radon.cfg.mqtt_host)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.update(processed=False)
            logging.error(u'Problem while publishing on topic "{0}"'.format(topic))


    def to_dict(self, user=None):
        """
        Return a dictionary which describes a notification for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        data = {
            "date": self.date,
            "when": self.when,
            "operation_name": self.op_name,
            "operation_type": self.op_type,
            "object_type": self.obj_type,
            "object_key": self.obj_key,
            "sender": self.sender,
            "payload": json.loads(self.payload),
        }
        return data



###################
## Class Methods ##
###################



    @classmethod
    def create_fail_collection(cls, payload):
        """
        The creation of a collection failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_CREATE_FAILED
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)

        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)

        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_COLLECTION,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_fail_group(cls, payload):
        """
        The creation of a group failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_CREATE_FAILED

        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)
        
        name = payload_check(P_OBJ_NAME, payload, "Undefined")
         
        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_COLLECTION,
            obj_key=name,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_fail_resource(cls, payload):
        """
        The creation of a resource failed, publish the message on MQTT
        
        :param sender: The user who initiates the operation
        :type sender: str
        :param obj: The dictionary that contains Resource information
        :type obj: dict
        :param msg: A message to explain what was wrong
        :type msg: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_CREATE_FAILED
        
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)
        
        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)
            
        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_RESOURCE,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_fail_user(cls, payload):
        """
        The creation of a user failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_CREATE_FAILED

        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)
        
        name = payload_check(P_OBJ_NAME, payload, "Undefined")
         
        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_USER,
            obj_key=name,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_request_collection(cls, payload):
        """
        Ask for the creation of a collection and publish the message on MQTT
        
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if not payload_check(P_OBJ_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("collection", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.create_fail_collection(payload)

        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_COLLECTION,
            obj_key=merge(payload['obj']['container'], payload['obj']['name']),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_request_group(cls, payload):
        """
        Ask for the creation of a group and publish the message on MQTT
        
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if missing:
            msg = MSG_INFO_MISSING.format("group", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.create_fail_group(payload)

        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_GROUP,
            obj_key=payload['obj']['name'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_request_resource(cls, payload):
        """
        Ask for the creation of a resource and publish the message on MQTT
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if not payload_check(P_OBJ_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("resource", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.create_fail_resource(payload)

        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_RESOURCE,
            obj_key=payload['obj']['path'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new
    

    @classmethod
    def create_request_user(cls, payload):
        """
        Ask for the creation of a user and publish the message on MQTT
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_LOGIN, payload):
             missing.append("login")
        if not payload_check("/obj/password", payload):
             missing.append("password")
        if missing:
            msg = MSG_INFO_MISSING.format("user", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.create_fail_user(payload)

        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_USER,
            obj_key=payload['obj']['login'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_success_collection(cls, payload):
        
        
        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_COLLECTION,
            obj_key=payload['obj']['path'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_success_group(cls, payload):
        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_GROUP,
            obj_key=payload['obj']['name'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_success_resource(cls, payload):
        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_RESOURCE,
            obj_key=payload['obj']['path'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def create_success_user(cls, payload):
        new = cls.new(
            op_name=OP_CREATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_USER,
            obj_key=payload['obj']['login'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_fail_collection(cls, payload):
        """
        The deletion of a collection failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_DELETE_FAILED
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)

        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_FAIL,
            obj_type=OBJ_COLLECTION,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_fail_group(cls, payload):
        """
        The deletion of a group failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_DELETE_FAILED

        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)
        
        name = payload_check(P_OBJ_NAME, payload, "Undefined")
         
        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_FAIL,
            obj_type=OBJ_GROUP,
            obj_key=name,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_fail_resource(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_DELETE_FAILED
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)

        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_FAIL,
            obj_type=OBJ_RESOURCE,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_fail_user(cls, payload):
        """
        The deletion of a user failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_DELETE_FAILED

        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)
        
        login = payload_check(P_OBJ_LOGIN, payload, "Undefined")
         
        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_FAIL,
            obj_type=OBJ_USER,
            obj_key=login,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new

    @classmethod
    def delete_request_collection(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if not payload_check(P_OBJ_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("collection", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.delete_fail_collection(payload)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_COLLECTION,
            obj_key=merge(payload['obj']['container'], payload['obj']['name']),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_request_group(cls, payload):
        """
        Ask for the deletion of a group and publish the message on MQTT
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if missing:
            msg = MSG_INFO_MISSING.format("group", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.delete_fail_group(payload)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_GROUP,
            obj_key=payload['obj']['name'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_request_resource(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if not payload_check(P_OBJ_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("resource", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.delete_fail_resource(payload)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_RESOURCE,
            obj_key=merge(payload['obj']['container'], payload['obj']['name']),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_request_user(cls, payload):
        """
        Ask for the deletion of a user and publish the message on MQTT
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_LOGIN, payload):
             missing.append("login")
        if missing:
            msg = MSG_INFO_MISSING.format("user", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.delete_fail_user(payload)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_USER,
            obj_key=payload['obj']['login'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_success_collection(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_CREATE_FAILED
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)

        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_COLLECTION,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_success_group(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if missing:
            msg = MSG_INFO_MISSING.format("group", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.delete_fail_user(payload)
        
        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_GROUP,
            obj_key=payload_check(P_OBJ_NAME, payload),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_success_resource(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_CREATE_FAILED
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)

        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)

        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_RESOURCE,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def delete_success_user(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_LOGIN, payload):
             missing.append("login")
        if missing:
            msg = MSG_INFO_MISSING.format("user", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.delete_fail_user(payload)
        
        new = cls.new(
            op_name=OP_DELETE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_USER,
            obj_key=payload_check(P_OBJ_LOGIN, payload),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def new(cls, **kwargs):
        """
        Create a new Notification
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`"""
        new = super(Notification, cls).create(**kwargs)
        return new


    @classmethod
    def recent(cls, count=20):
        """
        Return the latest activities
        
        :param count: The number of notifications we want to get
        :type count: int
        
        :return: A list of 'count' notifications (or less if there are not 
          enough yet in the base)
        :rtype: List[:class:`columns.model.Notification`]
        """
        #         return Notification.objects.filter(date__in=last_x_days())\
        #             .order_by("-when").all().limit(count)
        session = connection.get_session()
        keyspace = radon.cfg.dse_keyspace
        session.set_keyspace(keyspace)
        # I couldn't find how to disable paging in cqlengine in the "model" view
        # so I create the cal query directly
        query = SimpleStatement(
            u"""SELECT * from Notification WHERE
            date IN ({})
            ORDER BY when DESC
            limit {}""".format(
                ",".join(["'%s'" % el for el in last_x_days()]), count
            )
        )
        # Disable paging for this query (we use IN and ORDER BY in the same
        # query
        query.fetch_size = None
        res = []
        for row in session.execute(query):
            res.append(Notification(**row).to_dict())
        return res


    @classmethod
    def update_fail_collection(cls, payload):
        """
        The modification of a collection failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_UPDATE_FAILED
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)

        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_COLLECTION,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_fail_group(cls, payload):
        """
        The upate of a group failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_UPDATE_FAILED

        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)
        
        name = payload_check(P_OBJ_NAME, payload, "Undefined")
         
        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_GROUP,
            obj_key=name,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_fail_resource(cls, payload):
        """
        The modification of a resource failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_UPDATE_FAILED
        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)

        container = payload_check(P_OBJ_CONTAINER, payload, "/")
        name = payload_check(P_OBJ_NAME, payload, "")
        path = merge(container, name)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_RESOURCE,
            obj_key=path,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_fail_user(cls, payload):
        """
        The upate of a user failed, publish the message on MQTT
        
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        if not payload_check(P_META_MSG, payload):
            payload['meta']['msg'] = MSG_UPDATE_FAILED

        payload['meta']['sender'] = payload_check(P_META_SENDER, 
                                                  payload, 
                                                  radon.cfg.sys_lib_user)
        
        login = payload_check(P_OBJ_LOGIN, payload, "Undefined")
         
        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_FAIL,
            obj_type=OBJ_USER,
            obj_key=login,
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_request_collection(cls, payload):
        """
        Ask for the modification of a collection and publish the message on MQTT
        
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if not payload_check(P_OBJ_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("collection", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_collection(payload)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_COLLECTION,
            obj_key=merge(payload['obj']['container'], payload['obj']['name']),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_request_group(cls, payload):
        """
        Ask for the modification of a group and publish the message on MQTT
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if missing:
            msg = MSG_INFO_MISSING.format("group", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_group(payload)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_GROUP,
            obj_key=payload['obj']['name'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_request_resource(cls, payload):
        """
        Ask for the modification of a resource and publish the message on MQTT
        
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_NAME, payload):
             missing.append("name")
        if not payload_check(P_OBJ_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("resource", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_resource(payload)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_RESOURCE,
            obj_key=merge(payload['obj']['container'], payload['obj']['name']),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_request_user(cls, payload):
        """
        Ask for the modification of a user and publish the message on MQTT
        
        :param payload: The dictionary that contains message information
        :type obj: dict
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_OBJ_LOGIN, payload):
             missing.append("login")
        if missing:
            msg = MSG_INFO_MISSING.format("user", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_user(payload)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_REQUEST,
            obj_type=OBJ_USER,
            obj_key=payload['obj']['login'],
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_success_collection(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_PRE_NAME, payload):
             missing.append("name")
        if not payload_check(P_PRE_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("collection", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_collection(payload)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_COLLECTION,
            obj_key=merge(payload['pre']['container'], payload['pre']['name']),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_success_group(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_PRE_NAME, payload):
             missing.append("name")
        if missing:
            msg = "Information is missing for the name: {}".format(
                ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_group(payload)
        
        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_GROUP,
            obj_key=payload_check(P_PRE_NAME, payload),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_success_resource(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check(P_PRE_NAME, payload):
             missing.append("name")
        if not payload_check(P_PRE_CONTAINER, payload):
             missing.append("container")
        if missing:
            msg = MSG_INFO_MISSING.format("resource", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_resource(payload)

        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_RESOURCE,
            obj_key=merge(payload['pre']['container'], payload['pre']['name']),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def update_success_user(cls, payload):
        if 'meta' not in payload:
            payload['meta'] = {}
        sender = payload_check(P_META_SENDER, payload)
        if not sender:
            if 'meta' not in payload:
                payload['meta'] = {}
            payload['meta']['sender'] = radon.cfg.sys_lib_user
        missing = []
        if not payload_check("/pre/login", payload):
             missing.append("login")
        if missing:
            msg = MSG_INFO_MISSING.format("user", ", ".join(missing))
            payload['meta']['msg'] = msg
            return cls.update_fail_user(payload)
        
        new = cls.new(
            op_name=OP_UPDATE,
            op_type=OPT_SUCCESS,
            obj_type=OBJ_USER,
            obj_key=payload_check("/pre/login", payload),
            sender=payload['meta']['sender'],
            processed=True,
            payload=json.dumps(payload, default=datetime_serializer),
        )
        new.mqtt_publish()
        return new





