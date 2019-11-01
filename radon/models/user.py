"""Copyright 2019 - 

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

from dse.cqlengine import columns
from dse.cqlengine.models import Model
import json

from radon.models.notification import Notification
from radon.models.errors import UserConflictError
from radon.models.group import Group
from radon.util import (
    datetime_serializer,
    default_uuid,
    encrypt_password,
    verify_password
)


class User(Model):
    """User Model"""
    uuid = columns.Text(default=default_uuid)
    name = columns.Text(primary_key=True, required=True)
    email = columns.Text(required=True)
    password = columns.Text(required=True)
    administrator = columns.Boolean(required=True, default=False)
    active = columns.Boolean(required=True, default=True)
    ldap = columns.Boolean(required=True, default=False)
    groups = columns.List(columns.Text, index=True)
    

    def add_group(self, groupname):
        self.add_groups([groupname])


    def add_groups(self, ls_group):
        new_groups = self.get_groups() + ls_group
        # remove duplicate
        new_groups = list(set(new_groups))
        self.update(groups=new_groups)


    def authenticate(self, password):
        """Verify if the user is authenticated"""
        return verify_password(password, self.password) and self.active


    @classmethod
    def create(cls, **kwargs):
        """Create a user

        We intercept the create call so that we can correctly
        hash the password into an unreadable form
        
        "username" param is the name of the user who initiated the call
        """
        # username is the name of the user who initiated the call, it has to
        # be removed for the Cassandra call
        if 'username' in kwargs:
            username = kwargs['username']
            del kwargs['username']
        else:
            username = None
        kwargs['password'] = encrypt_password(kwargs['password'])

        if cls.objects.filter(name=kwargs['name']).count():
            raise UserConflictError(kwargs['name'])
        
        user = super(User, cls).create(**kwargs)

        state = user.mqtt_get_state()
        payload = user.mqtt_payload({}, state)
        Notification.create_user(username, user.name, payload)
        return user


    def delete(self, username=None):
        state = self.mqtt_get_state()
        super(User, self).delete()
        payload = self.mqtt_payload(state, {})
        # username is the id of the user who did the operation
        # user.uuid is the id of the new user
        Notification.delete_user(username, self.name, payload)


    @classmethod
    def find(cls, name):
        """Find a user from his name"""
        return cls.objects.filter(name=name).first()


    def get_groups(self):
        """Return user list of group names"""
        return self.groups


    def is_active(self):
        """Check if the user is active"""
        return self.active


    def is_authenticated(self):
        """Check if the user is authenticated"""
        return True


    def mqtt_get_state(self):
        """Get the user state for the payload"""
        payload = dict()
        payload['uuid'] = self.uuid
        payload['name'] = self.name
        payload['email'] = self.email
        payload['active'] = self.active
        payload['groups'] = [g.name for g in Group.find_all(self.groups)]
        return payload


    def mqtt_payload(self, pre_state, post_state):
        """Get a string version of the payload of the message"""
        payload = dict()
        payload['pre'] = pre_state
        payload['post'] = post_state
        return json.dumps(payload, default=datetime_serializer)


    def rm_group(self, groupname):
        self.rm_groups([groupname])


    def rm_groups(self, ls_group):
        new_groups = set(self.get_groups()) - set(ls_group)
        # remove duplicate
        new_groups = list(set(new_groups))
        self.update(groups=new_groups)


    def to_dict(self):
        """Return a dictionary which describes a resource for the web ui"""
        return {
            'uuid': self.uuid,
            'name': self.name,
            'email': self.email,
            'administrator': self.administrator,
            'active': self.active,
            'ldap': self.ldap,
            'groups': [g.to_dict() for g in Group.find_all(self.groups)]
        }


    def update(self, **kwargs):
        """Update a user"""
        pre_state = self.mqtt_get_state()
        # If we want to update the password we need to encrypt it first
        
        if "password" in kwargs:
            kwargs['password'] = encrypt_password(kwargs['password'])

        if 'username' in kwargs:
            username = kwargs['username']
            del kwargs['username']
        else:
            username = None

        super(User, self).update(**kwargs)
        user = User.find(self.name)
        post_state = user.mqtt_get_state()
        payload = user.mqtt_payload(pre_state, post_state)
        Notification.update_user(username, user.name, payload)
        return self


