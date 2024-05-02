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


from cassandra.cqlengine import columns
from cassandra.cqlengine.models import connection, Model
from cassandra.query import SimpleStatement
import json
import time
import logging
import paho.mqtt.publish as publish
 
from radon.model.config import cfg
from radon.util import (
    datetime_serializer,
    default_date,
    default_time,
    default_uuid,
    last_x_days,
    merge,
    payload_check,
)

from radon.model.payload import (
    PayloadCreateCollectionRequest,
    PayloadCreateCollectionSuccess,
    PayloadCreateCollectionFail,
    PayloadCreateGroupRequest,
    PayloadCreateGroupSuccess,
    PayloadCreateGroupFail,
    PayloadCreateResourceRequest,
    PayloadCreateResourceSuccess,
    PayloadCreateResourceFail,
    PayloadCreateUserRequest,
    PayloadCreateUserSuccess,
    PayloadCreateUserFail,
    
    PayloadDeleteCollectionRequest,
    PayloadDeleteCollectionSuccess,
    PayloadDeleteCollectionFail,
    PayloadDeleteGroupRequest,
    PayloadDeleteGroupSuccess,
    PayloadDeleteGroupFail,
    PayloadDeleteResourceRequest,
    PayloadDeleteResourceSuccess,
    PayloadDeleteResourceFail,
    PayloadDeleteUserRequest,
    PayloadDeleteUserSuccess,
    PayloadDeleteUserFail,
    
    PayloadUpdateCollectionRequest,
    PayloadUpdateCollectionSuccess,
    PayloadUpdateCollectionFail,
    PayloadUpdateGroupRequest,
    PayloadUpdateGroupSuccess,
    PayloadUpdateGroupFail,
    PayloadUpdateResourceRequest,
    PayloadUpdateResourceSuccess,
    PayloadUpdateResourceFail,
    PayloadUpdateUserRequest,
    PayloadUpdateUserSuccess,
    PayloadUpdateUserFail,
    
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


MSG_UNDEFINED_PATH = "Undefined path"
MSG_UNDEFINED_NAME = "Undefined group"
MSG_UNDEFINED_LOGIN = "Undefined group"
MSG_PAYLOAD_ERROR = "Problem with the payload"
MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE = "Object deleted but success message not valid"
MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE = "Object created but success message not valid"
MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE = "Object updated but success message not valid"


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
    # An id that can be used to link the request and the result of the request
    # (success or failure)
    req_id = columns.Text()
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
            publish.single(topic, self.payload, hostname=cfg.mqtt_host)
        except (ValueError, TypeError) :
            self.update(processed=False)
            logging.error(u'Problem while publishing on topic "{0}"'.format(topic))


    def to_dict(self, user=None):
        """
        Return a dictionary which describes a notification for the web ui
        
        :param user: If present, display the actions this specific user can do
        :type user: :class:`radon.model.user.User`
        
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
            "req_id": self.req_id,
            "payload": json.loads(self.payload),
        }
        return data


    @classmethod
    def create_notification(cls, payload):
        """
        Create a new entry in the database
        
        :param payload: The payload object which stores notification information
        :type payload: :class:`columns.model.payload.Payload`
        
        :return: The new notification
        :rtype: :class:`radon.model.notification.Notification`
        """
        new = Notification.new(
            op_name=payload.get_operation_name(),
            op_type=payload.get_operation_type(),
            obj_type=payload.get_object_type(),
            obj_key=payload.get_object_key(),
            sender=payload.get_sender(),
            req_id=payload.get_req_id(),
            processed=True,
            payload=json.dumps(payload.get_json(), default=datetime_serializer),
        )
        new.mqtt_publish()
        return new


    @classmethod
    def new(cls, **kwargs):
        """
        Create a new Notification
        
        :return: The notification
        :rtype: :class:`radon.model.notification.Notification`"""
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
        keyspace = cfg.dse_keyspace
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


def create_collection_fail(payload):
    """
    Create a new notification when the creation of the collection failed
    
    :param payload: The information regarding the collection whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateCollectionFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateCollectionFail):
        payload = PayloadCreateCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_collection_request(payload):
    """
    Create a new notification when the creation of the collection is requested
    
    :param payload: The information regarding the collection whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateCollectionRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateCollectionRequest):
        payload = PayloadCreateCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_collection_success(payload):
    """
    Create a new notification when the creation of the collection has been
    successful
    
    :param payload: The information regarding the collection which has been
                    created
    :type payload: :class:`radon.model.payload.PayloadCreateCollectionSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateCollectionSuccess):
        payload = PayloadCreateCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateCollectionFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_group_fail(payload):
    """
    Create a new notification when the creation of the group failed
    
    :param payload: The information regarding the group whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateGroupFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateGroupFail):
        payload = PayloadCreateGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_group_request(payload):
    """
    Create a new notification when the creation of the group is requested
    
    :param payload: The information regarding the group whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateGroupRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateGroupRequest):
        payload = PayloadCreateGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_group_success(payload):
    """
    Create a new notification when the creation of the group has been
    successful
    
    :param payload: The information regarding the group which has been
                    created
    :type payload: :class:`radon.model.payload.PayloadCreateGroupSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateGroupSuccess):
        payload = PayloadCreateGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateGroupFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_resource_fail(payload):
    """
    Create a new notification when the creation of the resource failed
    
    :param payload: The information regarding the resource whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateResourceFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateResourceFail):
        payload = PayloadCreateResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid:# Create valid payload
            payload = PayloadCreateResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_resource_request(payload):
    """
    Create a new notification when the creation of the resource is requested
    
    :param payload: The information regarding the resource whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateResourceRequest`  
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateResourceRequest):
        payload = PayloadCreateResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_resource_success(payload):
    """
    Create a new notification when the creation of the resource has been
    successful
    
    :param payload: The information regarding the resource which has been
                    created
    :type payload: :class:`radon.model.payload.PayloadCreateResourceSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateResourceSuccess):
        payload = PayloadCreateResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateResourceFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_user_fail(payload):
    """
    Create a new notification when the creation of the user failed
    
    :param payload: The information regarding the user whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateUserFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateUserFail):
        payload = PayloadCreateUserFail.default(
            MSG_UNDEFINED_LOGIN,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid:# Create valid payload
            payload = PayloadCreateUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_user_request(payload):
    """
    Create a new notification when the creation of the user is requested
    
    :param payload: The information regarding the user whose creation has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateUserRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateUserRequest):
        payload = PayloadCreateUserFail.default(
            MSG_UNDEFINED_LOGIN,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def create_user_success(payload):
    """
    Create a new notification when the creation of the user has been
    successful
    
    :param payload: The information regarding the user which has been
                    created
    :type payload: :class:`radon.model.payload.PayloadCreateUserSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadCreateUserSuccess):
        payload = PayloadCreateUserFail.default(
            MSG_UNDEFINED_LOGIN,
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadCreateUserFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_collection_fail(payload):
    """
    Create a new notification when the deletion of the collection failed
    
    :param payload: The information regarding the collection whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteCollectionFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteCollectionFail):
        payload = PayloadDeleteCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_collection_request(payload):
    """
    Create a new notification when the deletion of the collection is requested
    
    :param payload: The information regarding the collection whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteCollectionRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteCollectionRequest):
        payload = PayloadDeleteCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_collection_success(payload):
    """
    Create a new notification when the deletion of the collection has been
    successful
    
    :param payload: The information regarding the collection which has been
                    deleted
    :type payload: :class:`radon.model.payload.PayloadDeleteCollectionSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteCollectionSuccess):
        payload = PayloadDeleteCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteCollectionFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_group_fail(payload):
    """
    Create a new notification when the deletion of the group failed
    
    :param payload: The information regarding the group whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteGroupFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteGroupFail):
        payload = PayloadDeleteGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid:# Create valid payload
            payload = PayloadDeleteGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_group_request(payload):
    """
    Create a new notification when the deletion of the group is requested
    
    :param payload: The information regarding the group whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteGroupRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteGroupRequest):
        payload = PayloadDeleteGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_group_success(payload):
    """
    Create a new notification when the deletion of the group has been
    successful
    
    :param payload: The information regarding the group which has been
                    deleted
    :type payload: :class:`radon.model.payload.PayloadCreateGroupSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteGroupSuccess):
        payload = PayloadDeleteGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteGroupFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_resource_fail(payload):
    """
    Create a new notification when the deletion of the resource failed
    
    :param payload: The information regarding the resource whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteResourceFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteResourceFail):
        payload = PayloadDeleteResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid:# Create valid payload
            payload = PayloadDeleteResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_resource_request(payload):
    """
    Create a new notification when the deletion of the resource is requested
    
    :param payload: The information regarding the resource whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteResourceRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteResourceRequest):
        payload = PayloadDeleteResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_resource_success(payload):
    """
    Create a new notification when the deletion of the resource has been
    successful
    
    :param payload: The information regarding the resource which has been
                    deleted
    :type payload: :class:`radon.model.payload.PayloadDeleteResourceSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteResourceSuccess):
        payload = PayloadDeleteResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteResourceFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_user_fail(payload):
    """
    Create a new notification when the deletion of the user failed
    
    :param payload: The information regarding the user whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteUserFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteUserFail):
        payload = PayloadDeleteUserFail.default(
            MSG_UNDEFINED_LOGIN,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid:# Create valid payload
            payload = PayloadDeleteUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_user_request(payload):
    """
    Create a new notification when the deletion of the user is requested
    
    :param payload: The information regarding the user whose deletion has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadDeleteUserRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteUserRequest):
        payload = PayloadDeleteUserFail.default(
            MSG_UNDEFINED_LOGIN,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def delete_user_success(payload):
    """
    Create a new notification when the deletion of the user has been
    successful
    
    :param payload: The information regarding the user which has been
                    deleted
    :type payload: :class:`radon.model.payload.PayloadDeleteUserSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadDeleteUserSuccess):
        payload = PayloadDeleteUserFail.default(
            MSG_UNDEFINED_NAME,
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadDeleteUserFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_collection_fail(payload):
    """
    Create a new notification when the update of the collection failed
    
    :param payload: The information regarding the collection whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadUpdateCollectionFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateCollectionFail):
        payload = PayloadUpdateCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_collection_request(payload):
    """
    Create a new notification when the update of the collection is requested
    
    :param payload: The information regarding the collection whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadUpdateCollectionRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateCollectionRequest):
        payload = PayloadUpdateCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_collection_success(payload):
    """
    Create a new notification when the update of the collection has been
    successful
    
    :param payload: The information regarding the collection which has been
                    updated
    :type payload: :class:`radon.model.payload.PayloadUpdateCollectionSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateCollectionSuccess):
        payload = PayloadUpdateCollectionFail.default(
            MSG_UNDEFINED_PATH,
            MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateCollectionFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_group_fail(payload):
    """
    Create a new notification when the update of the group failed
    
    :param payload: The information regarding the group whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadCreateGroupFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateGroupFail):
        payload = PayloadUpdateGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_group_request(payload):
    """
    Create a new notification when the update of the group is requested
    
    :param payload: The information regarding the group whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadUpdateGroupRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateGroupRequest):
        payload = PayloadUpdateGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_group_success(payload):
    """
    Create a new notification when the update of the group has been
    successful
    
    :param payload: The information regarding the group which has been
                    updated
    :type payload: :class:`radon.model.payload.PayloadUpdateGroupSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateGroupSuccess):
        payload = PayloadUpdateGroupFail.default(
            MSG_UNDEFINED_NAME,
            MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateGroupFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_resource_fail(payload):
    """
    Create a new notification when the update of the resource failed
    
    :param payload: The information regarding the resource whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadUpdateResourceFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateResourceFail):
        payload = PayloadUpdateResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_resource_request(payload):
    """
    Create a new notification when the update of the resource is requested
    
    :param payload: The information regarding the resource whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadUpdateResourceRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateResourceRequest):
        payload = PayloadUpdateResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_resource_success(payload):
    """
    Create a new notification when the update of the resource has been
    successful
    
    :param payload: The information regarding the resource which has been
                    updated
    :type payload: :class:`radon.model.payload.PayloadUpdateResourceSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateResourceSuccess):
        payload = PayloadUpdateResourceFail.default(
            MSG_UNDEFINED_PATH,
            MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateResourceFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_user_fail(payload):
    """
    Create a new notification when the update of the user failed
    
    :param payload: The information regarding the user whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadUpdateUserFail`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateUserFail):
        payload = PayloadUpdateUserFail.default(
            MSG_UNDEFINED_LOGIN,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payloadx
            payload = PayloadUpdateUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_user_request(payload):
    """
    Create a new notification when the update of the user is requested
    
    :param payload: The information regarding the user whose update has
                    been requested
    :type payload: :class:`radon.model.payload.PayloadUpdateUserRequest`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateUserRequest):
        payload = PayloadUpdateUserFail.default(
            MSG_UNDEFINED_LOGIN,
            MSG_PAYLOAD_ERROR)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
    return Notification.create_notification(payload)


def update_user_success(payload):
    """
    Create a new notification when the update of the user has been
    successful
    
    :param payload: The information regarding the user which has been
                    updated
    :type payload: :class:`radon.model.payload.PayloadUpdateUserSuccess`
    
    :return: The notification row created in the database
    :rtype: :class:`radon.model.notification.Notification`
    """
    if not isinstance(payload, PayloadUpdateUserSuccess):
        payload = PayloadUpdateUserFail.default(
            MSG_UNDEFINED_NAME,
            MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE)
    else:
        (is_valid, msg) = payload.validate()
        if not is_valid: # Create valid payload
            payload = PayloadUpdateUserFail.default(
                payload.get_object_key(),
                MSG_SUCCESS_PAYLOAD_PROBLEM_UPDATE,
                payload.get_sender())
    return Notification.create_notification(payload)


def wait_response(req_id):
    """
    Wait for a success or a failure for a specific request id
    
    :param req_id: The id of the request that was made to create a resource
    :type size: str
    
    :return: The result of the operation (0->success, 1->fail, 2->timeout)
    :rtype: int
    """
    # Number of requests before failure
    MAX_REQUEST = 5
    # Delay between checking in s
    TIMEOUT = 1
    
    cpt = 1
    found = False
    res = 2
    
    while cpt < MAX_REQUEST and not found:
        session = connection.get_session()
        keyspace = cfg.dse_keyspace
        session.set_keyspace(keyspace)
        query = SimpleStatement(
            u"""SELECT * FROM notification_by_req_id
            WHERE req_id=%s
            AND op_type IN (%s, %s)""")
        rows = session.execute(query, (req_id, OPT_SUCCESS, OPT_FAIL))
        if rows:
            op_type = rows.one().get("op_type", None)
            if op_type == OPT_SUCCESS:
                found = True
                res = 0
            elif op_type == OPT_FAIL:
                found = True
                res = 1
        cpt += 1
        time.sleep(TIMEOUT)
        
    return res



