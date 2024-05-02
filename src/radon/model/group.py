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
from cassandra.cqlengine.models import Model
import json

from radon.model.config import cfg
from radon.model.notification import (
    create_group_fail,
    create_group_success,
    delete_group_success,
    update_group_success
)
from radon.model.payload import (
    PayloadCreateGroupFail,
    PayloadCreateGroupSuccess,
    PayloadDeleteGroupSuccess,
    PayloadUpdateGroupSuccess,
)
from radon.model.errors import GroupConflictError
from radon.util import (
    datetime_serializer,
    default_time,
    default_uuid,
    new_request_id,
)


class Group(Model):
    """
    Group Model
    
    This is used to store a Radon group
    
    :param uuid: A uuid associated to the group
    :type uuid: :class:`columns.Text`
    :param name: The group name, used as the primary key
    :type name: :class:`columns.Text`"""

    name = columns.Text(primary_key=True, required=True)
    uuid = columns.Text(default=default_uuid)
    create_ts = columns.TimeUUID(default=default_time)

    def add_user(self, name, sender=None):
        """
        Add a user to a group
        Return 3 lists:
        - added for the username which were added
        - already_there for username already in the group
        - not_added for username not found

        :param name: User name to add to the group
        :type name: str
        :param sender: the name of the user who made the action
        :type sender: str, optional
        
        :return: 3 lists to summarize what happened
        :rtype: Tuple[List[str],List[str],List[str]]
        """
        return self.add_users([name], sender)


    def add_users(self, ls_users, sender=None, req_id=None):
        """
        Add a list of users to a group
        Return 3 lists:
        - added for the username which were added
        - already_there for username already in the group
        - not_added for username not found

        :param ls_users: List of usernames to add to the group
        :type ls_users: List[str]
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param req_id: The id of the request that was made to update a group
        :type req_id: str, optional
        
        :return: 3 lists to summarize what happened
        :rtype: Tuple[List[str],List[str],List[str]]
        """
        from radon.model.user import User
 
        added = []
        not_added = []
        already_there = []
        for name in ls_users:
            user = User.find(name)
            if user:
                if self.name not in user.get_groups():
                    user.add_group(self.name, sender, req_id)
                    added.append(name)
                else:
                    already_there.append(name)
            else:
                not_added.append(name)
        return added, not_added, already_there


    @classmethod
    def create(cls, **kwargs):
        """
        Create a new group, raise an exception if the group already
        exists
        
        :param name: the name of the group
        :type name: str
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param req_id: The id of the request that was made to create a group
        :type req_id: str, optional
        
        :return: The new created group
        :rtype: :class:`radon.model.group.Group`
        """
        kwargs["name"] = kwargs["name"].strip()

        if "sender" in kwargs:
            sender = kwargs["sender"]
            del kwargs["sender"]
        else:
            sender = cfg.sys_lib_user

        if "req_id" in kwargs:
            req_id = kwargs['req_id']
            del kwargs['req_id']
        else:
            req_id = new_request_id()

        # Make sure name id not in use.
        if cls.objects.filter(name=kwargs["name"]).count():
            payload = PayloadCreateGroupFail.default(
                kwargs["name"], "Group already exists", sender)
            create_group_fail(payload)
            return None
        
        group = super(Group, cls).create(**kwargs)
        
        payload_json = {
            "obj": group.mqtt_get_state(),
            'meta' : {
                "sender": sender,
                "req_id": req_id
            }
        }
        create_group_success(PayloadCreateGroupSuccess(payload_json))

        return group


    def delete(self, **kwargs):
        """
        Delete the group in the database. (Can be improved, we need to remove 
        the group for all the users)
        
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param req_id: The id of the request that was made to create a collection
        :type req_id: str, optional
        """
        from radon.model.user import User
        
        if "sender" in kwargs:
            sender = kwargs['sender']
            del kwargs['sender']
        else:
            sender = cfg.sys_lib_user

        if "req_id" in kwargs:
            req_id = kwargs['req_id']
            del kwargs['req_id']
        else:
            req_id = new_request_id()

        payload_json = {
            "obj": {"name": self.name},
            'meta' : {
                "sender": sender,
                "req_id": req_id
            }
        }

        for u in User.objects.all():
            if self.name in u.get_groups():
                u.groups.remove(self.name)
                u.save()
        super(Group, self).delete()

        delete_group_success(PayloadDeleteGroupSuccess(payload_json))

 
    @classmethod
    def find(cls, name):
        """
        Find a group from his name.
        
        :param name: the name of the group
        :type name: str
        
        :return: The group which has been found
        :rtype: :class:`radon.model.group.Group`
        """
        return cls.objects.filter(name=name).first()


    @classmethod
    def find_all(cls, namelist):
        """
        Find all group from their names.
        
        :param namelist: the list of group names
        :type namelist: List[str]
        
        :return: The groups which has been found
        :rtype: List[:class:`radon.model.group.Group`]
        """
        return cls.objects.filter(name__in=namelist).all()


    def get_members(self):
        """
        Get a list of usernames of the group
        
        :return: The names of the users in the group
        :rtype: List[:class:`radon.model.user.User`]
        """
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from radon.model.user import User
 
        return [
            u.login for u in User.objects.all() if u.active and self.name in u.get_groups()
        ]


    def mqtt_get_state(self):
        """
        Get the group state that will be used in the payload
        
        :return: The user state as a dictionary
        :rtype: dict
        """
        payload = dict()
        payload["uuid"] = self.uuid
        payload["name"] = self.name
        payload["members"] = self.get_members()
        return payload


    def rm_user(self, name, sender=False):
        """
        Remove a user from the group.
        Return 3 lists:
        - removed for the usernames which were removed
        - not_there for usernames who weren't in the group
        - not_exist for usernames who don't exist
        
        :param name: The name of the user to remove
        :type name: str
        :param sender: the name of the user who made the action
        :type sender: str, optional
        
        :return: 3 lists to summarize what happened
        :rtype: Tuple[List[str],List[str],List[str]]
        """
        return self.rm_users([name], sender)


    def rm_users(self, ls_users, sender=False, req_id=None):
        """
        Remove a list of users from the group.
        Return 3 lists:
        - removed for the usernames which were removed
        - not_there for usernames who weren't in the group
        - not_exist for usernames who don't exist
        
        :param ls_users: The lists of user names to remove
        :type ls_users: List[str]
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param req_id: The id of the request that was made to update a group
        :type req_id: str, optional
        
        :return: 3 lists to summarize what happened
        :rtype: Tuple[List[str],List[str],List[str]]
        """
        from radon.model.user import User
 
        not_exist = []
        removed = []
        not_there = []
        for name in ls_users:
            user = User.find(name)
            if user:
                if self.name in user.get_groups():
                    user.rm_group(self.name, sender, req_id)
                    removed.append(name)
                else:
                    not_there.append(name)
            else:
                not_exist.append(name)
        return removed, not_there, not_exist


    def to_dict(self):
        """
        Return a dictionary which describes a resource for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        return {"uuid": self.uuid,
                "name": self.name,
                "members": self.get_members()}


    def update(self, **kwargs):
        """
        Update a group. 
        
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param req_id: The id of the request that was made to update a group
        :type req_id: str, optional

        :return: The modified group
        :rtype: :class:`radon.model.group.Group`
        """
        pre_state = self.mqtt_get_state()
        
        if "sender" in kwargs:
            sender = kwargs['sender']
            del kwargs['sender']
        else:
            sender = cfg.sys_lib_user

        if "req_id" in kwargs:
            req_id = kwargs['req_id']
            del kwargs['req_id']
        else:
            req_id = new_request_id()
            
        if "members" in kwargs:
            members = kwargs['members']
            del kwargs['members']
            new_members_set = set(members)
            old_members_set = set(self.get_members())
            
            to_add = new_members_set.difference(old_members_set)
            to_rm = old_members_set.difference(new_members_set)
            self.add_users(list(to_add), sender, req_id)
            self.rm_users(list(to_rm), sender, req_id)
        
        # No field to update directly for the moment
        # super(Group, self).update(**kwargs)
        
        group = Group.find(self.name)
        
        post_state = group.mqtt_get_state()
        if (pre_state != post_state):
            payload_json = {
                "obj" : pre_state,
                "new": post_state,
                "meta": {
                    "sender": sender,
                    "req_id": req_id
                }
            }
            update_group_success(PayloadUpdateGroupSuccess(payload_json))
        return self


