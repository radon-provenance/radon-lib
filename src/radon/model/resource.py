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


from datetime import datetime
import json
import urllib
from abc import (
    ABC,
    abstractmethod
)

import radon
from radon.model import (
    DataObject,
    Notification,
    TreeNode,
)
from radon.model.acl import (
    acemask_to_str,
    acl_cdmi_to_cql,
    serialize_acl_metadata
)
from radon.model.errors import (
    NoSuchCollectionError,
    ResourceConflictError
)
from radon.util import (
    datetime_serializer,
#     decode_meta,
    default_cdmi_id,
    encode_meta,
    is_reference,
    merge,
    meta_cdmi_to_cassandra,
    meta_cassandra_to_cdmi,
    metadata_to_list,
    now,
    split,
)



class Resource(ABC):
    """Resource Model
    
    A resource represents a node in the hierarchy. It links to a TreeNode 
    object in the Cassandra database.
    
    :param node: The TreeNode row that corresponds to the resource
    :type node: :class:`radon.model.TreeNode`
    :param url: The url of the resource
    :type url: str
    :param path: The full path of the resource in the hierarchy
    :type path: str
    :param container: The name of the parent collection
    :type container: str
    :param name: The name of the resource
    :type name: str
    :param uuid: UUID of the TreeNode
    :type uuid: str
    
    
    """

    def __init__(self, node):
        """
        Create the resource object based on the TreeNode row from Cassandra
        
        :param node: The TreeNode row that corresponds to the resource
        :type node: :class:`radon.model.TreeNode`
        """
        self.node = node
        self.url = self.node.object_url
        self.path = self.node.path()
        self.container = self.node.container
        self.name = self.node.name
        self.uuid = self.node.uuid


    def __str__(self):
        return self.path

    @abstractmethod
    def chunk_content(self):
        """Get a chunk of the data object"""
        pass

 
    @classmethod
    def create(cls, container, name, url=None, metadata=None, sender=None,
               mimetype=None, size=None, read_access=None, write_access=None):
        """
        Create a new resource
        
        :param container: The name of the parent collection
        :type container: str
        :param name: The name of the resource
        :type name: str
        :param url: The url of the resource
        :type url: str, optional
        :param metadata: A Key/Value pair dictionary for user metadata
        :type metadata: dict, optional
        :param sender: The name of the user who created the resource
        :type sender: str, optional
        :param mimetype: The mimetype of the resource
        :type mimetype: str, optional
        :param size: The name of the user who created the resource
        :type size: str, optional
        
        :return: The new Resource object
        :rtype: :class:`radon.model.Resource`
        """
        from radon.model import Collection
        from radon.model import Notification
        if not container.endswith('/'):
            container += '/'
 
        # Make sure parent/name are not in use.
        path = merge(container, name)
        existing = cls.find(path)
        if existing:
            Notification.create_fail_resource(sender, 
                                              path,
                                              "Conflict with a resource")
            return None
            
        # Check if parent collection exists
        parent = Collection.find(container)
        if parent is None:
            Notification.create_fail_resource(sender, 
                                              path,
                                              "Parent container doesn't exist")
            return None

        now_date = now()
        if not metadata:
            user_meta = {}
        else:
            user_meta = {}
            for k in metadata:
                user_meta[k] = encode_meta(metadata[k])
        
        sys_meta = {
            radon.cfg.meta_create_ts: encode_meta(now_date),
            radon.cfg.meta_modify_ts: encode_meta(now_date)
        }
        
        if mimetype:
            sys_meta[radon.cfg.meta_mimetype] = mimetype
        if size:
            sys_meta[radon.cfg.meta_size] = str(size)

        if not url:
            url = "{}{}".format(radon.cfg.protocol_cassandra,
                                default_cdmi_id())
            
        resc_node = TreeNode.create(
            container=container,
            name=name,
            user_meta=user_meta,
            sys_meta=sys_meta,
            object_url=url,
            is_object=True,
        )
        
        if not sender:
            sender = radon.cfg.sys_lib_user
        
        if url.startswith(radon.cfg.protocol_cassandra):
            new = RadonResource(resc_node)
        else:
            new = UrlLibResource(resc_node)
        
        if read_access or write_access:
            new.create_acl_list(read_access, write_access)
        
        payload = {
            "obj": new.mqtt_get_state(),
            'meta' : {
                "sender": sender
            }
        }
        Notification.create_success_resource(payload)
        return new


    def create_acl_list(self, read_access, write_access):
        """
        Create ACL from lists of group names, existing ACL are replaced
        
        :param read_access: A list of group names which have read access
        :type read_access: List[str]
        :param write_access: A list of group names which have write access
        :type write_access: List[str]
        """
        self.node.create_acl_list(read_access, write_access)


    def delete(self, **kwargs):
        """
        Delete a resource and the associated row in the tree_node table
        
        :param sender: The name of the user who deleted the collection
        :type sender: str, optional
        """
        if "sender" in kwargs:
            sender = kwargs['sender']
            del kwargs['sender']
        else:
            sender = radon.cfg.sys_lib_user

        payload = {
            "obj": self.mqtt_get_state(),
            'meta' : {
                "sender": sender
            }
        }
        
        self.node.delete()
        
        Notification.delete_success_resource(payload)


    @classmethod
    def find(cls, path, version=None):
        """
        Find a resource by path, initialise the resource with the
        appropriate row in the tree_node table
        
        :param path: The full path in the hierarchy
        :type path: str
        :param version: The specific version we want to find
        :type version: int, optional
        
        :return: The Resource object which maps the TreeNode
        :rtype: :class:`radon.model.Resource`
        """
        if path == '/':
            return None
        if path.endswith("/"):
            path = path[:-1]
        
        container, name = split(path)
        if not version:
            qnodes = TreeNode.objects.filter(container=container,
                                            name=name)
            if qnodes.count() == 0:
                return None
            # Get the most recent version
            version = qnodes.first().version
        qnodes = TreeNode.objects.filter(container=container,
                                        name=name,
                                        version=version)
        if qnodes.count() == 0:
            return None
        else:
            node = qnodes.first()
            if not node.object_url:
                return NoUrlResource(node)
            if not is_reference(node.object_url):
                return RadonResource(node)
            else:
                return UrlLibResource(node)


    def full_dict(self):
        """
        Return a dictionary which describes a resource for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        data = {
            "uuid": self.uuid,
            "name": self.get_name(),
            "container": self.container,
            "path": self.path,
            "user_meta": self.get_list_user_meta(),
            "sys_meta": self.get_list_sys_meta(),
            "url": self.url,
            "is_reference": self.is_reference(),
            "mimetype": self.get_mimetype() or "application/octet-stream",
            "type": self.get_mimetype(),
            "create_ts": self.get_create_ts(),
            "modify_ts": self.get_modify_ts(),
        }
        return data


    def get_acl_dict(self):
        """
        Return ACL in a dictionary 
        
        :return: The ACL associated to the resource
        :rtype: dict
        """
        return self.node.acl


    def get_acl_list(self):
        """
        Return two lists of groups which have read and write access
        
        :return: The two lists of group names
        :rtype: Tuple[List[str],List[str]]
        """
        read_access = []
        write_access = []
        for gid, ace in self.get_acl_dict().items():
            oper = acemask_to_str(ace.acemask, True)
            if oper == "read":
                read_access.append(gid)
            elif oper == "write":
                write_access.append(gid)
            elif oper == "read/write":
                read_access.append(gid)
                write_access.append(gid)
            else:
                # Unknown combination
                radon.cfg.logger.warning(
                    "The acemask for group {0} on resource {1} is invalid".format(
                        gid,
                        self.path
                    )
                )
        return read_access, write_access


    def get_acl_metadata(self):
        """Return a dictionary of acl based on the Resource schema"""
        return serialize_acl_metadata(self)


    def get_authorized_actions(self, user):
        """"
        Get available actions for a user according to the groups it belongs
        
        :param user: The user we want to check
        :type user: :class:`radon.model.User`
        
        :return: the set of actions the user can do
        :rtype: Set[str]
        """
        # Check permission on the parent container if there's no action
        # defined at this level
        acl = self.get_acl_dict()
        if not acl:
            from radon.model import Collection
            parent_container = Collection.find(self.container)
            return parent_container.get_authorized_actions(user)
        actions = set([])
        for gid in user.groups + ["AUTHENTICATED@"]:
            if gid in acl:
                ace = acl[gid]
                level = acemask_to_str(ace.acemask, True)
                if level == "read":
                    actions.add("read")
                elif level == "write":
                    actions.add("write")
                    actions.add("delete")
                    actions.add("edit")
                elif level == "read/write":
                    actions.add("read")
                    actions.add("write")
                    actions.add("delete")
                    actions.add("edit")
        return actions


    def get_cdmi_sys_meta(self):
        """
        Return a dictionary for system metadata
        
        :return: A dictionary with decoded values, ready for cdmi
        :rtype: dict
        """
        return meta_cassandra_to_cdmi(self.node.sys_meta)


    def get_cdmi_user_meta(self):
        """
        Return a dictionary for user metadata
        
        :return: A dictionary with decoded values, ready for cdmi
        :rtype: dict
        """
        return meta_cassandra_to_cdmi(self.node.user_meta)


    def get_create_ts(self):
        """
        Get the creation timestamp
        
        :return: The timestamp stored in the system metadata
        :rtype: datetime
        """
        return self.node.sys_meta.get(radon.cfg.meta_create_ts)


    def get_list_sys_meta(self):
        """
        Transform a metadata dictionary retrieved from Cassandra to a list of
        tuples. If metadata items are lists they are split into multiple pairs in
        the result list. For system metadata.
    
        :return: a list of pairs (name, value)
        :rtype: list
        """
        return metadata_to_list(self.node.sys_meta, radon.cfg.vocab_dict)


    def get_list_user_meta(self):
        """
        Transform a metadata dictionary retrieved from Cassandra to a list of
        tuples. If metadata items are lists they are split into multiple pairs in
        the result list. For user metadata.
    
        :return: a list of pairs (name, value)
        :rtype: list
        """
        return metadata_to_list(self.node.user_meta)


    def get_modify_ts(self):
        """
        Get the modification timestamp
        
        :return: The timestamp stored in the system metadata
        :rtype: datetime
        """
        return self.node.sys_meta.get(radon.cfg.meta_modify_ts)


    def get_mimetype(self):
        """
        Return mimetype of the resource
        
        :return: The mimetype
        :rtype: str
        """
        return self.node.sys_meta.get(radon.cfg.meta_mimetype, "")
# 
#     def get_metadata(self):
#         if self.is_reference:
#             return self.entry.metadata
#         else:
#             if not self.obj:
#                 self.obj = DataObject.find(self.obj_id)
#                 if self.obj is None:
#                     return self.entry.metadata
#             return self.obj.metadata
# 
#     def get_metadata_key(self, key):
#         """Return the value of a metadata"""
#         return decode_meta(self.get_metadata().get(key, ""))


    @abstractmethod
    def get_name(self):
        """
        Return the name of a resource. If the resource is a reference we
        append a trailing '?' on the resource name
        """
        pass


    def get_path(self):
        """Return the full path of the resource"""
        return self.path


    @abstractmethod
    def get_size(self):
        """
        Return size of the resource
        
        :return: The size
        :rtype: int
        """
        pass


#     def index(self):
#         from radon.models import SearchIndex
# 
#         self.reset()
#         SearchIndex.index(self, ["name", "metadata"])


    def is_reference(self):
        """
        Return true if the resource is a reference
        
        :return: True if the resource is a reference (not stored in Cassandra)
        :rtype: bool
        """
        return is_reference(self.url)


    def mqtt_get_state(self):
        """
        Get the resource state that will be used in the payload
        
        :return: The resource state as a dictionary
        :rtype: dict
        """
        payload = dict()
        payload["uuid"] = self.uuid
        payload["url"] = self.url
        payload["container"] = self.container
        payload["name"] = self.get_name()
        payload["create_ts"] = self.get_create_ts()
        payload["modify_ts"] = self.get_modify_ts()
        payload["metadata"] = self.get_cdmi_user_meta()
        return payload


    def mqtt_payload(self, pre_state, post_state):
        """
        Get a string version of the payload of the message, with the pre and
        post states. The pre and post states are stored in a dictionary and
        dumped in a JSON string.
        
        :param pre_state: The dictionary which describes the state of the 
          resource before a modification
        :type pre_state: dict
        :param post_state: The dictionary which describes the state of the 
          resource after a modification
        :type post_state: dict
        
        :return: The payload as a JSON string
        :rtype: str
        """
        payload = dict()
        payload["pre"] = pre_state
        payload["post"] = post_state
        return json.dumps(payload, default=datetime_serializer)
# 
    
    @abstractmethod
    def put(self, data):
        """
        Store some data in a Resource stored in Cassandra
        """
        pass
# 
# 
#     def reset(self):
#         from radon.models import SearchIndex
# 
#         SearchIndex.reset(self.path)
# 
# 
#     def set_checksum(self, checksum):
#         """Set the checksum for the data object. For a reference the checksum 
#         is not stored in Cassandra. We may change the model and store is in
#         Tree Entry"""
#         if self.is_reference:
#             return
#         else:
#             if not self.obj:
#                 self.obj = DataObject.find(self.obj_id)
#                 if self.obj is None:
#                     return
#             self.obj.update(checksum=checksum)
# 
# 
    def simple_dict(self, user=None):
        """
        Return a dictionary which describes a resource for the web ui, with
        less information.
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        data = {
            "id": self.uuid,
            "name": self.get_name(),
            "container": self.container,
            "path": self.path,
            "is_reference": self.is_reference(),
            "mimetype": self.get_mimetype() or "application/octet-stream",
            "type": self.get_mimetype(),
        }
        if user:
            data["can_read"] = self.user_can(user, "read")
            data["can_write"] = self.user_can(user, "write")
            data["can_edit"] = self.user_can(user, "edit")
            data["can_delete"] = self.user_can(user, "delete")
        return data
 
 
    def to_dict(self, user=None):
        """
        Return a dictionary which describes a resource for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        return self.simple_dict(user)


    def update(self, **kwargs):
        """
        Update a resource. We intercept the call to encode the metadata if we
        modify it. Metadata passed in this method is user meta.
        
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param metadata: The plain password to encrypt
        :type metadata: dict
        :param mimetype: The mimetype of the resource
        :type mimetype: str, optional
        """
        from radon.model import Notification
 
        pre_state = self.mqtt_get_state()
        now_date = now()
        
        # Metadata given in cdmi format are transformed to be stored in Cassandra
        if 'metadata' in kwargs:
            kwargs['user_meta'] = kwargs['metadata']
            del kwargs["metadata"]
         
        if "user_meta" in kwargs:
            kwargs["user_meta"] = meta_cdmi_to_cassandra(kwargs["user_meta"])
        
        sys_meta = self.node.sys_meta
        sys_meta[radon.cfg.meta_modify_ts] = encode_meta(now_date)
        kwargs["sys_meta"] = sys_meta
        if "mimetype" in kwargs:
            sys_meta[radon.cfg.meta_mimetype] = kwargs["mimetype"]
            del kwargs["mimetype"]
            

        if "sender" in kwargs:
            sender = kwargs["sender"]
            del kwargs["sender"]
        else:
            sender = None
        
        if "read_access" in kwargs:
            read_access = kwargs["read_access"]
            del kwargs["read_access"]
        else:
            read_access = []
        if "write_access" in kwargs:
            write_access = kwargs["write_access"]
            del kwargs["write_access"]
        else:
            write_access = []

        if "url" in kwargs:
            kwargs["object_url"] = kwargs["url"]
            del kwargs["url"]
            self.url = kwargs["object_url"]
 
        self.node.update(**kwargs)
        
        if read_access or write_access:
            self.update_acl_list(read_access, write_access)
        
        resc = Resource.find(self.path)
        post_state = resc.mqtt_get_state()
        
        if (pre_state != post_state):
            payload = {
                "pre" : pre_state,
                "post": post_state,
                "meta": {
                    "sender": sender
                    }
            }
            Notification.update_success_resource(payload)
        return self


    def update_acl_cdmi(self, cdmi_acl):
        """Update ACL in the tree_node table from ACL in the cdmi format (list
        of ACE dictionary), existing ACL are replaced"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        self.node.update_acl(cql_string)


    def update_acl_list(self, read_access, write_access):
        """
        Update ACL from lists of group uuids
        
        :param read_access: A list of group names which have read access
        :type read_access: List[str]
        :param write_access: A list of group names which have write access
        :type write_access: List[str]
        """
        self.node.update_acl_list(read_access, write_access)


    def user_can(self, user, action):
        """
        User can perform the action if any of the user's group IDs
        appear in this list for 'action'_access in this object.
        
        :param user: The user to check
        :type user: :class:`radon.model.User`
        
        :return: True if the user can do the action
        :rtype: bool
        """
        if user.administrator:
            # An administrator can do anything
            return True
        actions = self.get_authorized_actions(user)
        if action in actions:
            return True
        return False



class NoUrlResource(Resource):

    def get_name(self):
        """
        Return the name of a resource. We append a trailing '#' on the 
        resource name to signify that it's broken.
        
        :return: The name, with a '#'
        :rtype: str
        """
        return "{}#".format(self.name)


    def get_size(self):
        """
        Return size of the resource. This has to be stored somewhere in the
        metadata to be more efficient
        
        :return: The size
        :rtype: int
        """
        return 0


    def put(self, data):
        """
        Storing data externally isn't implemented
        """
        raise NotImplementedError


    def chunk_content(self):
        """
        Yields the content for a generator, one chunk at a time. 
        
        :return: A chunk of data bits
        :rtype: str
        """
        return ""


class RadonResource(Resource):
    """
    A specific Resource where data bits are stored in the DataObject table in
    Cassandra.
    
    :param obj_id: The uuid of the DataObject
    :type obj_id: str
    :param obj: The DataObject object
    :type obj: :class:`radon.model.DataObject`
    """

    def __init__(self, node):
        """
        Create the resource object based on the TreeNode row from Cassandra
        
        :param node: The TreeNode row that corresponds to the resource
        :type node: :class:`radon.model.TreeNode`
        """
        Resource.__init__(self, node)
        self.obj_id = self.url.replace(radon.cfg.protocol_cassandra, "")
        self.obj = DataObject.find(self.obj_id)


    def get_name(self):
        """
        Return the name of a resource.
        
        :return: The name, as it is in the table
        :rtype: str
        """
        return self.name


    def chunk_content(self):
        """
        Yields the content for a generator, one chunk at a time. 
        
        :return: A chunk of data bits
        :rtype: str
        """
        if self.obj:
            return self.obj.chunk_content()
        return None


    def delete(self, **kwargs):
        """
        Delete a resource and the associated row in the tree_node table and all 
        the corresponding blobs
        
        :param sender: The name of the user who deleted the collection
        :type sender: str, optional
        """
        self.delete_data_objects()
        Resource.delete(self, **kwargs)


    def delete_data_objects(self):
        """
        Delete all blobs of the corresponding uuid
        """
        DataObject.delete_id(self.obj_id)
        
    
    def full_dict(self, user=None):
        """
        Return a dictionary which describes a resource for the web ui
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        data = Resource.full_dict(self)
        if self.obj:
            data["size"] = self.get_size()
        if user:
            data["can_read"] = self.user_can(user, "read")
            data["can_write"] = self.user_can(user, "write")
            data["can_edit"] = self.user_can(user, "edit")
            data["can_delete"] = self.user_can(user, "delete")
        return data


    def get_size(self):
        """
        Return size of the resource, stored in Cassandra
        
        :return: The size
        :rtype: int
        """
        if self.obj:
            return self.obj.size
        return 0


    def put(self, fh):
        """
        Store some binary data in a Resource stored in Cassandra
        """
        if not (hasattr(fh, 'read')):
            data = fh
            do = DataObject.create(data, compressed=radon.cfg.compress_do)
        else:
            chunk = fh.read(radon.cfg.chunk_size)
            do = DataObject.create(chunk, compressed=radon.cfg.compress_do)
            seq_num = 1
            while True:
                chunk = fh.read(radon.cfg.chunk_size)
                if not chunk:
                    break
                DataObject.append_chunk(do.uuid, seq_num, chunk, radon.cfg.compress_do)
                do.size += len(chunk)
                seq_num += 1
        self.obj = do
        self.obj_id = do.uuid
        self.url = do.get_url()
        self.node.object_url = self.url
        self.node.save()
        return do


class ReferenceResource(Resource):
    """
    A resource stored externally
    """
    
    pass


class UrlLibResource(ReferenceResource):
    """
    A resource stored externally, with a URL that can be processed by the 
    urllib module.
    """


    def chunk_content(self):
        """
        Yields the content for a generator, one chunk at a time. 
        
        :return: A chunk of data bits
        :rtype: str
        """
        req = urllib.request.Request(self.url)
        with urllib.request.urlopen(req) as response:
           yield response.read()


    def get_name(self):
        """
        Return the name of a resource. We append a trailing '?' on the 
        resource name to signify that it's not stored in Radon.
        
        :return: The name, with a trailing '?'
        :rtype: str
        """
        return "{}?".format(self.name)


    def get_size(self):
        """
        Return size of the resource. This has to be stored somewhere in the
        metadata to be more efficient
        
        :return: The size
        :rtype: int
        """
        return 0


    def put(self, data):
        """
        Storing data externally isn't implemented
        """
        raise NotImplementedError



