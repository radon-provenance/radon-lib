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
    default_date,
    default_time,
    last_x_days
)


# Operations that could lead to a new notification
OP_CREATE = "create"
OP_DELETE = "delete"
OP_UPDATE = "update"

# Types of objects with the element needed to identify the object
OBJ_RESOURCE = "resource"  # path
OBJ_COLLECTION = "collection"  # path
OBJ_USER = "user"  # name
OBJ_GROUP = "group"  # name

TEMPLATES = {
    OP_CREATE: {},
    OP_DELETE: {},
    OP_UPDATE: {},
}


TEMPLATES[OP_CREATE][
    OBJ_RESOURCE
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} created a new item '<a href='{% url "archive:resource_view" path=object.path %}'>{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>

"""
TEMPLATES[OP_CREATE][
    OBJ_COLLECTION
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} created a new collection '<a href='{% url "archive:view" path=object.path %}'>{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_CREATE][
    OBJ_USER
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} created a new user '<a href='{% url "users:view" name=object.name %}'>{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_CREATE][
    OBJ_GROUP
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} created a new group '<a href='{% url "groups:view" name=object.name %}'>{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""

TEMPLATES[OP_DELETE][
    OBJ_RESOURCE
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} deleted the '{{ object.name }}' item</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_DELETE][
    OBJ_COLLECTION
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} deleted the collection '{{ object.name }}'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_DELETE][
    OBJ_USER
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} deleted user '{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_DELETE][
    OBJ_GROUP
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} deleted group '{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""

TEMPLATES[OP_UPDATE][
    OBJ_RESOURCE
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} edited the '<a href='{% url "archive:resource_view" path=object.path %}'>{{ object.name }}</a>' item</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_UPDATE][
    OBJ_COLLECTION
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} edited the '<a href='{% url "archive:view" path=object.path %}'>{{ object.name }}</a>' collection </span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_UPDATE][
    OBJ_USER
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} edited user '<a href='{% url "users:view" name=object.name %}'>{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""
TEMPLATES[OP_UPDATE][
    OBJ_GROUP
] = """
{% load gravatar %}
{% gravatar user.email 40 %}
<span class="activity-message">{{ user.name }} edited group '<a href='{% url "groups:view" name=object.name %}'>{{ object.name }}</a>'</span>
<span class="activity-timespan">{{ when|date:"M d, Y - P" }}</span>
"""


class Notification(Model):
    """Notification Model
    
    This is used to store notifications for later use (audit or rule engine)
    
    :param date: A simple date used to partition notification
    :type date: :class:`columns.Text`
    :param when: The full date/time fir the notification
    :type when: :class:`columns.TimeUUID`
    :param operation: The type of operation (Create, Delete, Update, Index, 
      Move...)
    :type operation: :class:`columns.Text`
    :param object_type: The type of the object concerned (Collection, Resource,
      User, Group, ...)
    :type object_type: :class:`columns.Text`
    :param object_uuid: The uuid of the object concerned, the key used to find
      the object
    :type object_uuid: :class:`columns.Text`
    :param username: The user who initiates the operation
    :type username: :class:`columns.Text`
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
    # The type of operation (Create, Delete, Update, Index, Move...)
    operation = columns.Text(primary_key=True)
    # The type of the object concerned (Collection, Resource, User, Group, ...)
    object_type = columns.Text(primary_key=True)
    # The uuid of the object concerned, the key used to find the corresponding
    # object (path, uuid, ...)
    object_uuid = columns.Text(primary_key=True)

    # The user who initiates the operation
    username = columns.Text()
    # True if the corresponding workflow has been executed correctly (for Move
    # or indexing for instance)
    # True if nothing has to be done
    processed = columns.Boolean()
    # The payload of the message which is sent to MQTT
    payload = columns.Text()

    @classmethod
    def create_collection(cls, username, path, payload):
        """
        Create a new collection and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param path: Path of the new collection
        :type path: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_CREATE,
            object_type=OBJ_COLLECTION,
            object_uuid=path,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def create_group(cls, username, uuid, payload):
        """
        Create a new group and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param uuid: Name of the new group
        :type uuid: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_CREATE,
            object_type=OBJ_GROUP,
            object_uuid=uuid,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def create_resource(cls, username, path, payload):
        """
        Create a new resource and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param path: Path of the resource
        :type path: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_CREATE,
            object_type=OBJ_RESOURCE,
            object_uuid=path,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def create_user(cls, username, uuid, payload):
        """
        Create a new user and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param uuid: Name of the new user
        :type uuid: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_CREATE,
            object_type=OBJ_USER,
            object_uuid=uuid,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def delete_collection(cls, username, path, payload):
        """
        Delete a collection and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param path: Path of the deleted collection
        :type path: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_DELETE,
            object_type=OBJ_COLLECTION,
            object_uuid=path,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def delete_group(cls, username, uuid, payload):
        """
        Delete a group and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param uuid: Name of the deleted group
        :type uuid: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_DELETE,
            object_type=OBJ_GROUP,
            object_uuid=uuid,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def delete_resource(cls, username, path, payload):
        """
        Delete a resource and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param path: Path of the resource
        :type path: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_DELETE,
            object_type=OBJ_RESOURCE,
            object_uuid=path,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def delete_user(cls, username, uuid, payload):
        """
        Delete a user and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param uuid: Name of the new user
        :type uuid: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_DELETE,
            object_type=OBJ_USER,
            object_uuid=uuid,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new


    def mqtt_publish(self):
        """
        Try to publish the notification on MQTT
        """
        topic = u"{0}/{1}/{2}".format(
            self.operation, 
            self.object_type, 
            self.object_uuid)
        # Clean up the topic by removing superfluous slashes.
        topic = "/".join(filter(None, topic.split("/")))
        # Remove MQTT wildcards from the topic. Corner-case: If the collection name is made entirely of # and + and a
        # script is set to run on such a collection name. But that's what you get if you use stupid names for things.
        topic = topic.replace("#", "").replace("+", "")
        logging.info(u'Publishing on topic "{0}"'.format(topic))
        publish.single(topic, self.payload, hostname=radon.cfg.mqtt_host)
        # try:
        #     publish.single(topic, self.payload, hostname=radon.cfg.mqtt_host)
        # except:
        #     self.update(processed=False)
        #     logging.error(u'Problem while publishing on topic "{0}"'.format(topic))

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

    def tmpl(self):
        """
        Return the template associated to the notification (based on the 
        operation and the object involved
        
        :return: The string template that will be used to create meaningful
          messages in the web interface/
        :rtype: str
        """
        return TEMPLATES[self.operation][self.object_type]

    def to_dict(self, user=None):
        """
        Return a dictionary which describes a notification for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        data = {
            "date": self.date,
            "when": self.when,
            "operation": self.operation,
            "object_type": self.object_type,
            "object_uuid": self.object_uuid,
            "username": self.username,
            "tmpl": self.tmpl(),
            "payload": json.loads(self.payload),
        }
        return data

    @classmethod
    def update_collection(cls, username, path, payload):
        """
        Update a  collection and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param path: Path of the collection
        :type path: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_UPDATE,
            object_type=OBJ_COLLECTION,
            object_uuid=path,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def update_group(cls, username, uuid, payload):
        """
        Update a group and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param uuid: Name of the group
        :type uuid: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_UPDATE,
            object_type=OBJ_GROUP,
            object_uuid=uuid,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def update_resource(cls, username, path, payload):
        """
        Update a resource and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param path: Path of the resource
        :type path: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_UPDATE,
            object_type=OBJ_RESOURCE,
            object_uuid=path,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new

    @classmethod
    def update_user(cls, username, uuid, payload):
        """
        Update a user and publish the message on MQTT
        
        :param username: The user who initiates the operation
        :type username: str
        :param uuid: Name of the user
        :type uuid: str
        :param payload: The payload of the message which is sent to MQTT
        :type payload: str
        
        :return: The notification
        :rtype: :class:`columns.model.Notification`
        """
        new = cls.new(
            operation=OP_UPDATE,
            object_type=OBJ_USER,
            object_uuid=uuid,
            username=username,
            processed=True,
            payload=payload,
        )
        new.mqtt_publish()
        return new


