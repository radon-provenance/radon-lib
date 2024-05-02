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


from radon.model.config import cfg
from radon.model.payload import (
    PayloadCreateCollectionRequest,
    PayloadCreateCollectionFail,
    PayloadDeleteCollectionRequest,
    PayloadDeleteCollectionFail,
    PayloadUpdateCollectionRequest,
    PayloadUpdateCollectionFail,
    PayloadCreateGroupRequest,
    PayloadCreateGroupFail,
    PayloadDeleteGroupRequest,
    PayloadDeleteGroupFail,
    PayloadUpdateGroupRequest,
    PayloadUpdateGroupFail,
    PayloadCreateResourceRequest,
    PayloadCreateResourceFail,
    PayloadDeleteResourceRequest,
    PayloadDeleteResourceFail,
    PayloadUpdateResourceRequest,
    PayloadUpdateResourceFail,
    PayloadCreateUserRequest,
    PayloadCreateUserFail,
    PayloadDeleteUserRequest,
    PayloadDeleteUserFail,
    PayloadUpdateUserRequest,
    PayloadUpdateUserFail,
    P_META_SENDER,
    P_META_REQ_ID,
)
from radon.model.notification import (
    create_collection_fail,
    delete_collection_fail,
    update_collection_fail,
    create_group_fail,
    delete_group_fail,
    update_group_fail,
    create_resource_fail,
    delete_resource_fail,
    update_resource_fail,
    create_user_fail,
    delete_user_fail,
    update_user_fail,
)
from radon.model.collection import Collection
from radon.model.group import Group
from radon.model.resource import Resource
from radon.model.user import User
from radon.util import (
    payload_check,
    split,
    new_request_id,
)

ERR_PAYLOAD_CLASS = "Wrong payload class"

class Microservices(object):


    @classmethod
    def create_collection(cls, payload):
        """
        Create a collection
        
        :param payload: The payload with the correct information for the new 
                        object
        :type payload: :class:`radon.model.payload.PayloadCreateCollectionRequest`
        
        :return: The status of the creation, the created object and a message
        :rtype: Tuple(Bool, :class:`radon.model.collection.Collection`, str)
        """
        if not isinstance(payload, PayloadCreateCollectionRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadCreateCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            create_collection_fail(payload)
            return (False,  None, msg)
        params = payload_check("/obj", payload.get_json(), {})
        # /obj/path is already checked and valid
        path = payload_check("/obj/path", payload.get_json(), "/")
        params['container'], params['name'] = split(path)
        del params['path']
        sender = payload_check(P_META_SENDER, payload.get_json())
        if sender:
            params['sender'] = sender
        req_id = payload_check(P_META_REQ_ID, payload.get_json())
        if req_id:
            params['req_id'] = req_id
        coll = Collection.create(**params)
        if coll:
            return (True, coll, "Collection created")
        else:
            return (False, None, "Collection not created")


    @classmethod
    def create_group(cls, payload):
        """
        Create a group
        
        :param payload: The payload with the correct information for the new 
                        object
        :type payload: :class:`radon.model.payload.PayloadCreateGroupRequest`
        
        :return: The status of the creation, the created object and a message
        :rtype: Tuple(Bool, :class:`radon.model.group.Group`, str)
        """
        if not isinstance(payload, PayloadCreateGroupRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadCreateGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            create_group_fail(payload)
            return (False,  None, msg)
        params = payload_check("/obj", payload.get_json(), {})
        sender = payload_check(P_META_SENDER, payload.get_json())
        if sender:
            params['sender'] = sender
        req_id = payload_check(P_META_REQ_ID, payload.get_json())
        if req_id:
            params['req_id'] = req_id
        group = Group.create(**params)
        if group:
            return (True, group, "Group created")
        else:
            return (False, None, "Group not created")


    @classmethod
    def create_resource(cls, payload):
        """
        Create a resource
        
        :param payload: The payload with the correct information for the new 
                        object
        :type payload: :class:`radon.model.payload.PayloadCreateResourceRequest`
        
        :return: The status of the creation, the created object and a message
        :rtype: Tuple(Bool, :class:`radon.model.resource.Resource`, str)
        """
        if not isinstance(payload, PayloadCreateResourceRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadCreateResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            create_resource_fail(payload)
            return (False,  None, msg)
        params = payload_check("/obj", payload.get_json(), {})
        # /obj/path is already checked and valid
        path = payload_check("/obj/path", payload.get_json(), "/")
        params['container'], params['name'] = split(path)
        del params['path']
        
        data = payload_check("/obj/data", payload.get_json())
        if data:
            data = bytes(data, 'utf-8')
            del params['data']
        sender = payload_check(P_META_SENDER, payload.get_json())
        if sender:
            params['sender'] = sender
        req_id = payload_check(P_META_REQ_ID, payload.get_json())
        if req_id:
            params['req_id'] = req_id

        resc = Resource.create(**params)
        
        if resc:
            if data:
                resc.put(data)
            return (True, resc, "Resource created")
        else:
            return (False, None, "Resource not created")


    @classmethod
    def create_user(cls, payload):
        """
        Create a user
        
        :param payload: The payload with the correct information for the new 
                        object
        :type payload: :class:`radon.model.payload.PayloadCreateUserRequest`
        
        :return: The status of the creation, the created object and a message
        :rtype: Tuple(Bool, :class:`radon.model.user.User`, str)
        """
        if not isinstance(payload, PayloadCreateUserRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadCreateUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            create_user_fail(payload)
            return (False,  None, msg)
        params = payload_check("/obj", payload.get_json(), {})
        sender = payload_check(P_META_SENDER, payload.get_json())
        if sender:
            params['sender'] = sender
        req_id = payload_check(P_META_REQ_ID, payload.get_json())
        if req_id:
            params['req_id'] = req_id
        user = User.create(**params)
        if user:
            return (True, user, "User created")
        else:
            return (False, None, "User not created")


    @classmethod
    def delete_collection(cls, payload):
        """
        Delete a collection
        
        :param payload: The payload with the correct information for the object
                        to delete
        :type payload: :class:`radon.model.payload.PayloadDeleteCollectionRequest`
        
        :return: The status of the deletion, the deleted object and a message
        :rtype: Tuple(Bool, :class:`radon.model.collection.Collection`, str)
        """
        if not isinstance(payload, PayloadDeleteCollectionRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadDeleteCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            delete_collection_fail(payload)
            return (False, None, msg)
        coll = Collection.find(payload.get_object_key())
        if coll:
            params = {}
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            coll.delete(**params)
            return (True, coll, "Collection deleted")
        else:
            return (False, None, "Collection not found")


    @classmethod
    def delete_group(cls, payload):
        """
        Delete a group
        
        :param payload: The payload with the correct information for the object
                        to delete
        :type payload: :class:`radon.model.payload.PayloadDeleteGroupRequest`
        
        :return: The status of the deletion, the deleted object and a message
        :rtype: Tuple(Bool, :class:`radon.model.group.Group`, str)
        """
        if not isinstance(payload, PayloadDeleteGroupRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadDeleteGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            delete_group_fail(payload)
            return (False, None, msg)
        group = Group.find(payload.get_object_key())
        if group:
            params = {}
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            group.delete(**params)
            return (True, group, "Group deleted")
        else:
            return (False, None, "Group not found")


    @classmethod
    def delete_resource(cls, payload):
        """
        Delete a resource
        
        :param payload: The payload with the correct information for the object
                        to delete
        :type payload: :class:`radon.model.payload.PayloadDeleteResourceRequest`
        
        :return: The status of the deletion, the deleted object and a message
        :rtype: Tuple(Bool, :class:`radon.model.resource.Resource`, str)
        """
        if not isinstance(payload, PayloadDeleteResourceRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadDeleteResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            delete_resource_fail(payload)
            return (False, None, msg)
        resc = Resource.find(payload.get_object_key())
        if resc:
            params = {}
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            resc.delete(**params)
            return (True, resc, "Resource deleted")
        else:
            return (False, None, "Resource not found")


    @classmethod
    def delete_user(cls, payload):
        """
        Delete a user
        
        :param payload: The payload with the correct information for the object
                        to delete
        :type payload: :class:`radon.model.payload.PayloadDeleteUserRequest`
        
        :return: The status of the deletion, the deleted object and a message
        :rtype: Tuple(Bool, :class:`radon.model.user.User`, str)
        """
        if not isinstance(payload, PayloadDeleteUserRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadDeleteUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            delete_user_fail(payload)
            return (False, None, msg)
        user = User.find(payload.get_object_key())
        if user:
            params = {}
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            user.delete(**params)
            return (True, user, "User deleted")
        else:
            return (False, None, "User not found")


    @classmethod
    def update_collection(cls, payload):
        """
        Update a collection
        
        :param payload: The payload with the correct information for the object
                        to update
        :type payload: :class:`radon.model.payload.PayloadUpdateCollectionRequest`
        
        :return: The status of the update, the updated object and a message
        :rtype: Tuple(Bool, :class:`radon.model.collection.Collection`, str)
        """
        if not isinstance(payload, PayloadUpdateCollectionRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadUpdateCollectionFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            update_collection_fail(payload)
            return (False, None, msg)
        coll = Collection.find(payload.get_object_key())
        if coll:
            params = {}
            metadata = payload_check("/obj/metadata", payload.get_json(), None)
            if metadata != None:
                params['metadata'] = metadata
            read_access = payload_check("/obj/read_access", payload.get_json(), None)
            if read_access != None:
                params['read_access'] = read_access
            write_access = payload_check("/obj/write_access", payload.get_json(), None)
            if write_access != None:
                params['write_access'] = write_access
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            coll.update(**params)
            return (True, coll, "Collection updated")
        else:
            return (False, None, "Collection not found")


    @classmethod
    def update_group(cls, payload):
        """
        Update a group
        
        :param payload: The payload with the correct information for the object
                        to update
        :type payload: :class:`radon.model.payload.PayloadUpdateGroupRequest`
        
        :return: The status of the update, the updated object and a message
        :rtype: Tuple(Bool, :class:`radon.model.group.Group`, str)
        """
        if not isinstance(payload, PayloadUpdateGroupRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadUpdateGroupFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            update_group_fail(payload)
            return (False, None, msg)
        group = Group.find(payload.get_object_key())
        
        if group:
            params = {}
            params['members'] = payload_check("/obj/members", payload.get_json(), group.get_members())
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            group.update(**params)
            return (True, group, "Group updated")
        else:
            return (False, None, "Group not found")


    @classmethod
    def update_resource(cls, payload):
        """
        Update a resource
        
        :param payload: The payload with the correct information for the object
                        to update
        :type payload: :class:`radon.model.payload.PayloadUpdateResourceRequest`
        
        :return: The status of the update, the updated object and a message
        :rtype: Tuple(Bool, :class:`radon.model.resource.Resource`, str)
        """
        if not isinstance(payload, PayloadUpdateResourceRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadUpdateResourceFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            update_resource_fail(payload)
            return (False, None, msg)
        resc = Resource.find(payload.get_object_key())
        if resc:
            params = {}
            metadata = payload_check("/obj/metadata", payload.get_json(), None)
            if metadata != None:
                params['metadata'] = metadata
            read_access = payload_check("/obj/read_access", payload.get_json(), None)
            if read_access != None:
                params['read_access'] = read_access
            write_access = payload_check("/obj/write_access", payload.get_json(), None)
            if write_access != None:
                params['write_access'] = write_access
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            resc.update(**params)
            return (True, resc, "Resource updated")
        else:
            return (False, None, "Resource not found")


    @classmethod
    def update_user(cls, payload):
        """
        Update a user
        
        :param payload: The payload with the correct information for the object
                        to update
        :type payload: :class:`radon.model.payload.PayloadUpdateUserRequest`
        
        :return: The status of the update, the updated object and a message
        :rtype: Tuple(Bool, :class:`radon.model.user.User`, str)
        """
        if not isinstance(payload, PayloadUpdateUserRequest):
            return (False, None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadUpdateUserFail.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            update_user_fail(payload)
            return (False, None, msg)
        user = User.find(payload.get_object_key())
        if user:
            params = {}
            email = payload_check("/obj/email", payload.get_json())
            if email:
                params['email'] = email
            fullname = payload_check("/obj/fullname", payload.get_json())
            if fullname:
                params['fullname'] = fullname
            administrator = payload_check("/obj/administrator", payload.get_json(), None)
            if administrator != None:
                params['administrator'] = administrator
            active = payload_check("/obj/active", payload.get_json(), None)
            if active != None:
                params['active'] = active
            ldap = payload_check("/obj/ldap", payload.get_json(), None)
            if ldap != None:
                params['ldap'] = ldap
            password = payload_check("/obj/password", payload.get_json())
            if password:
                params['password'] = password
            params['sender'] = payload_check(P_META_SENDER, payload.get_json(), cfg.sys_lib_user)
            params['req_id'] = payload_check(P_META_REQ_ID, payload.get_json(), new_request_id)
            user.update(**params)
            return (True, user, "User updated")
        else:
            return (False, None, "User not found")




