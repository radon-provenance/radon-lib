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

from radon.model.config import cfg
from radon.model.group import Group
from radon.model.notification import (
    create_fail_user,
    create_success_user,
    delete_success_user,
    update_success_user
)
from radon.model.payload import (
    PayloadCreateFailUser,
    PayloadCreateSuccessUser,
    PayloadDeleteSuccessResource,
    PayloadUpdateSuccessUser
)
from radon.model.errors import UserConflictError
from radon.util import (
    datetime_serializer,
    default_time,
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
    :param login: The user name, used as the primary key
    :type login: :class:`columns.Text`
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
    :param fullname: The user full name
    :type fullname: :class:`columns.Text`
    :param create_ts: The date/time for the creation of the user
    :type create_ts: :class:`columns.TimeUUID`
    """

    login = columns.Text(primary_key=True, required=True)
    password = columns.Text(required=True)
    fullname = columns.Text(index=True)
    email = columns.Text()
    administrator = columns.Boolean(default=False)
    active = columns.Boolean(default=True)
    ldap = columns.Boolean(default=False)
    groups = columns.List(columns.Text, index=True)
    uuid = columns.Text(default=default_uuid)
    create_ts = columns.TimeUUID(default=default_time)


    def add_group(self, groupname, sender=None):
        """
        Add the user to a group
        
        :param groupname: The group to be added to
        :type groupname: str
        :param sender: the name of the user who made the action
        :type sender: str, optional
        """
        self.add_groups([groupname], sender)


    def add_groups(self, ls_group, sender=None):
        """
        Add the user to a list of groups
        
        :param ls_group: The groups to be added to
        :type groupname: List[str]
        :param sender: the name of the user who made the action
        :type sender: str, optional
        """
        new_groups = self.get_groups() + ls_group
        # remove duplicate
        new_groups = list(set(new_groups))
        self.update(groups=new_groups, sender=sender)


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
                return verify_ldap_password(self.login, password)
            else:
                return verify_password(password, self.password)
        return False


    @classmethod
    def create(cls, **kwargs):
        """Create a user

        We intercept the create call so that we can correctly
        hash the password into an unreadable form
        
        :param login: the name of the user
        :type login: str
        :param password: The plain password to encrypt
        :type password: str
        :param sender: the name of the user who made the action
        :type sender: str, optional
        
        :return: The new created user
        :rtype: :class:`radon.model.User`
        """
        # sender is the name of the user who initiated the call, it has to
        # be removed for the Cassandra call
        if "sender" in kwargs:
            sender = kwargs["sender"]
            del kwargs["sender"]
        else:
            sender = cfg.sys_lib_user
        kwargs["password"] = encrypt_password(kwargs["password"])

        if cls.objects.filter(login=kwargs["login"]).count():
            payload = PayloadCreateFailUser.default(
                kwargs.get("login", "Unknown"), "User already exists", sender)
            create_fail_user(payload)
            return None

        user = super(User, cls).create(**kwargs)

        payload_json = {
            "obj": user.mqtt_get_state(),
            'meta' : {
                "sender": sender
            }
        }
        create_success_user(PayloadCreateSuccessUser(payload_json))
        return user

    @classmethod
    def delete_user(cls, name):
        """
        Delete a user in the database if he exists.
        :param name: the name of the user
        :type name: str
        """
        user = cls.find(name)
        if not user:
            return
        user.delete()       
        
        
        
    def delete(self, **kwargs):
        """
        Delete the user in the database.
        
        :param sender: The name of the user who made the action
        :type sender: str, optional
        """
        if "sender" in kwargs:
            sender = kwargs['sender']
            del kwargs['sender']
        else:
            sender = cfg.sys_lib_user

        payload_json = {
            "obj": {"login": self.mqtt_get_state()},
            'meta' : {"sender": sender}
        }

        super(User, self).delete()

        delete_success_user(PayloadDeleteSuccessResource(payload_json))


    @classmethod
    def find(cls, login):
        """
        Find a user from his login.
        
        :param login: the login of the user
        :type login: str
        
        :return: The user which has been found
        :rtype: :class:`radon.model.User`
        """
        return cls.objects.filter(login=login).first()

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
        payload["login"] = self.login
        payload["fullname"] = self.fullname
        payload["email"] = self.email
        payload["active"] = self.active
        payload["administrator"] = self.administrator
        payload["groups"] = [g.name for g in Group.find_all(self.groups)]
        return payload


    def rm_group(self, groupname, sender=None):
        """
        Remove the user from a group.
        
        :param groupname: The group to be removed from
        :type groupname: str
        :param sender: the name of the user who made the action
        :type sender: str, optional
        """
        self.rm_groups([groupname])


    def rm_groups(self, ls_group, sender=None):
        """
        Remove the user from a list of groups.
        
        :param groupname: The groups to be removed from
        :type groupname: List[str]
        :param sender: the name of the user who made the action
        :type sender: str, optional
        """
        new_groups = set(self.get_groups()) - set(ls_group)
        # remove duplicate
        new_groups = list(set(new_groups))
        self.update(groups=new_groups, sender=sender)


    def to_dict(self):
        """
        Return a dictionary which describes a resource for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        return {
            "uuid": self.uuid,
            "login": self.login,
            "fullname": self.fullname,
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
        
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param password: The plain password to encrypt
        :type password: str
        
        :return: The modified user
        :rtype: :class:`radon.model.User`
        """
        pre_state = self.mqtt_get_state()
        # If we want to update the password we need to encrypt it first

        if "password" in kwargs:
            kwargs["password"] = encrypt_password(kwargs["password"])

        if "login" in kwargs:
            del kwargs["login"]
        sender = cfg.sys_lib_user
        if "sender" in kwargs:
            sender = kwargs['sender']
            del kwargs['sender']

        super(User, self).update(**kwargs)
        user = User.find(self.login)
        post_state = user.mqtt_get_state()
        
        if (pre_state != post_state):
            payload_json = {
                "obj" : pre_state,
                "new": post_state,
                "meta": {"sender": sender}
            }
            update_success_user(PayloadUpdateSuccessUser(payload_json))
        return self


