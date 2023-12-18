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
    PayloadCreateFailCollection,
    PayloadCreateFailGroup,
    PayloadCreateFailResource,
    PayloadCreateFailUser,
    PayloadDeleteFailCollection,
    PayloadDeleteFailGroup,
    PayloadDeleteFailResource,
    PayloadDeleteFailUser,
    PayloadUpdateFailCollection,
    PayloadUpdateFailGroup,
    PayloadUpdateFailResource,
    PayloadUpdateFailUser
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


MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE = "Object deleted but success message not valid"
MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE = "Object created but success message not valid"


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
            publish.single(topic, self.payload, hostname=cfg.mqtt_host)
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


    @classmethod
    def create_notification(cls, payload):
        new = Notification.new(
            op_name=payload.get_operation_name(),
            op_type=payload.get_operation_type(),
            obj_type=payload.get_object_type(),
            obj_key=payload.get_object_key(),
            sender=payload.get_sender(),
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


def create_fail_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailCollection.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_fail_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid:# Create valid payload
        payload = PayloadCreateFailGroup.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_fail_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid:# Create valid payload
        payload = PayloadCreateFailResource.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)



def create_fail_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid:# Create valid payload
        payload = PayloadCreateFailUser.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_request_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailCollection.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_request_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailGroup.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_request_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailResource.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_request_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailUser.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_success_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailCollection.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_success_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailGroup.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_success_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailResource.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
            payload.get_sender())
    return Notification.create_notification(payload)


def create_success_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadCreateFailUser.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_CREATE,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_fail_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailCollection.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_fail_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid:# Create valid payload
        payload = PayloadDeleteFailGroup.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_fail_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid:# Create valid payload
        payload = PayloadDeleteFailResource.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_fail_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid:# Create valid payload
        payload = PayloadDeleteFailUser.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_request_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailCollection.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_request_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailGroup.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_request_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailResource.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_request_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailUser.default(
            payload.get_object_key(),
            payload.get_sender(),
            msg)
    return Notification.create_notification(payload)


def delete_success_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailCollection.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_success_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailGroup.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_success_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailResource.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
            payload.get_sender())
    return Notification.create_notification(payload)


def delete_success_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadDeleteFailUser.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_fail_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailCollection.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_fail_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailGroup.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_fail_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailResource.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_fail_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payloadx
        payload = PayloadUpdateFailUser.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_request_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailCollection.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_request_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailGroup.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_request_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailResource.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_request_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailUser.default(
            payload.get_object_key(),
            msg,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_success_collection(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailCollection.default(
            payload.get_object_key(),
            payload.get_sender(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE)
    return Notification.create_notification(payload)


def update_success_group(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailGroup.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_success_resource(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailResource.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
            payload.get_sender())
    return Notification.create_notification(payload)


def update_success_user(payload):
    (is_valid, msg) = payload.validate()
    if not is_valid: # Create valid payload
        payload = PayloadUpdateFailUser.default(
            payload.get_object_key(),
            MSG_SUCCESS_PAYLOAD_PROBLEM_DELETE,
            payload.get_sender())
    return Notification.create_notification(payload)


