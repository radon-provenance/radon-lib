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
from dse.cqlengine.models import Model
import json

import radon
from radon.model import (
    Group,
    Notification
)
from radon.model.errors import UserConflictError
from radon.util import (
    datetime_serializer,
    default_uuid,
    encrypt_password,
    verify_ldap_password,
    verify_password,
)


class User(Model):
    """User Model
    
    This is used to store a Radon user
    
    :param uuid: A uuid associated to the user
    :type uuid: :class:`columns.Text`
    :param name: The user name, used as the primary key
    :type name: :class:`columns.Text`
    :param email: The user email
    :type email: :class:`columns.Text`
    :param password: The user password, stored hashed
    :type password: :class:`columns.Text`
    :param administrator: A boolean if the user has admin access
    :type administrator: :class:`columns.Boolean`
    :param active: A boolean if the user is active or not
    :type active: :class:`columns.Boolean`
    :param ldap: A boolean if the user password has to be checked on a 
      LDAP server
    :type ldap: :class:`columns.Boolean`
    :param groups: A list of group names
    :type groups: :class:`columns.List`
    """

    uuid = columns.Text(default=default_uuid)
    name = columns.Text(primary_key=True, required=True)
    email = columns.Text(required=True)
    password = columns.Text(required=True)
    administrator = columns.Boolean(required=True, default=False)
    active = columns.Boolean(required=True, default=True)
    ldap = columns.Boolean(required=True, default=False)
    groups = columns.List(columns.Text, index=True)


    def add_group(self, groupname, username=None):
        """
        Add the user to a group
        
        :param groupname: The group to be added to
        :type groupname: str
        :param username: the name of the user who made the action
        :type username: str, optional
        """
        self.add_groups([groupname], username)


    def add_groups(self, ls_group, username=None):
        """
        Add the user to a list of groups
        
        :param ls_group: The groups to be added to
        :type groupname: List[str]
        :param username: the name of the user who made the action
        :type username: str, optional
        """
        new_groups = self.get_groups() + ls_group
        # remove duplicate
        new_groups = list(set(new_groups))
        self.update(groups=new_groups, username=username)


    def authenticate(self, password):
        """
        Check user password against an existing hash (hash)
    
        :param password: the password we want to test (plain)
        :type password: str
        
        :return: a boolean which indicate if the password is correct
        :rtype: bool
        """
        if self.active:
            if self.ldap:
                return verify_ldap_password(self.name, password)
            else:
                return verify_password(password, self.password)
        return False


    @classmethod
    def create(cls, **kwargs):
        """Create a user

        We intercept the create call so that we can correctly
        hash the password into an unreadable form
        
        :param name: the name of the user
        :type name: str
        :param password: The plain password to encrypt
        :type password: str
        :param username: the name of the user who made the action
        :type username: str, optional
        
        :return: The new created user
        :rtype: :class:`radon.model.User`
        """
        # username is the name of the user who initiated the call, it has to
        # be removed for the Cassandra call
        if "username" in kwargs:
            username = kwargs["username"]
            del kwargs["username"]
        else:
            username = radon.cfg.sys_lib_user
        kwargs["password"] = encrypt_password(kwargs["password"])

        if cls.objects.filter(name=kwargs["name"]).count():
            raise UserConflictError(kwargs["name"])

        user = super(User, cls).create(**kwargs)

        state = user.mqtt_get_state()
        payload = user.mqtt_payload({}, state)
        Notification.create_user(username, user.name, payload)
        return user

    def delete(self, username=None):
        """
        Delete the user in the database.
        
        :param username: the name of the user who made the action
        :type username: str, optional
        """
        state = self.mqtt_get_state()
        super(User, self).delete()
        payload = self.mqtt_payload(state, {})
        # username is the id of the user who did the operation
        # user.uuid is the id of the new user
        Notification.delete_user(username, self.name, payload)

    @classmethod
    def find(cls, name):
        """
        Find a user from his name.
        
        :param name: the name of the user
        :type name: str
        
        :return: The user which has been found
        :rtype: :class:`radon.model.User`
        """
        return cls.objects.filter(name=name).first()

    def get_groups(self):
        """
        Return the list of group names for the user
        
        :return: The list of groups
        :rtype: List[str]
        """
        return self.groups

    def is_active(self):
        """
        Check if the user is active
        
        :return: The user status
        :rtype: bool
        """
        return self.active

    def is_authenticated(self):
        """
        Check if the user is authenticated
        
        :return: The user status
        :rtype: bool
        """
        return True

    def mqtt_get_state(self):
        """
        Get the user state that will be used in the payload
        
        :return: The user state as a dictionary
        :rtype: dict
        """
        payload = dict()
        payload["uuid"] = self.uuid
        payload["name"] = self.name
        payload["email"] = self.email
        payload["active"] = self.active
        payload["groups"] = [g.name for g in Group.find_all(self.groups)]
        return payload

    def mqtt_payload(self, pre_state, post_state):
        """
        Get a string version of the payload of the message, with the pre and
        post states. The pre and post states are stored in a dictionary and
        dumped in a JSON string.
        
        :param pre_state: The dictionary which describes the state of the user
          before a modification
        :type pre_state: dict
        :param post_state: The dictionary which describes the state of the user
          after a modification
        :type post_state: dict
        
        :return: The payload as a JSON string
        :rtype: str
        """
        payload = dict()
        payload["pre"] = pre_state
        payload["post"] = post_state
        return json.dumps(payload, default=datetime_serializer)


    def rm_group(self, groupname, username=None):
        """
        Remove the user from a group.
        
        :param groupname: The group to be removed from
        :type groupname: str
        :param username: the name of the user who made the action
        :type username: str, optional
        """
        self.rm_groups([groupname])


    def rm_groups(self, ls_group, username=None):
        """
        Remove the user from a list of groups.
        
        :param groupname: The groups to be removed from
        :type groupname: List[str]
        :param username: the name of the user who made the action
        :type username: str, optional
        """
        new_groups = set(self.get_groups()) - set(ls_group)
        # remove duplicate
        new_groups = list(set(new_groups))
        self.update(groups=new_groups, username=username)


    def to_dict(self):
        """
        Return a dictionary which describes a resource for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        return {
            "uuid": self.uuid,
            "name": self.name,
            "email": self.email,
            "administrator": self.administrator,
            "active": self.active,
            "ldap": self.ldap,
            "groups": [g.to_dict() for g in Group.find_all(self.groups)],
        }

    def update(self, **kwargs):
        """
        Update a user. We intercept the call to encrypt the password if we
        modify it.
        
        :param username: the name of the user who made the action
        :type username: str, optional
        :param password: The plain password to encrypt
        :type password: str
        
        :return: The modified user
        :rtype: :class:`radon.model.User`
        """
        pre_state = self.mqtt_get_state()
        # If we want to update the password we need to encrypt it first

        if "password" in kwargs:
            kwargs["password"] = encrypt_password(kwargs["password"])

        if "username" in kwargs:
            username = kwargs["username"]
            del kwargs["username"]
        else:
            username = None

        super(User, self).update(**kwargs)
        user = User.find(self.name)
        post_state = user.mqtt_get_state()
        payload = user.mqtt_payload(pre_state, post_state)
        if (pre_state != post_state):
            Notification.update_user(username, user.name, payload)
        return self


