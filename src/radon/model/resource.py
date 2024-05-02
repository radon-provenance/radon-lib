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


from datetime import datetime
import json
import urllib
from abc import (
    ABC,
    abstractmethod
)

from radon.model.config import cfg
from radon.model.data_object import DataObject
from radon.model.tree_node import TreeNode
from radon.model.acl import (
    acemask_to_str,
    acl_cdmi_to_cql,
    serialize_acl_metadata
)
from radon.model.errors import (
    NoSuchCollectionError,
    ResourceConflictError
)
from radon.model.notification import (
    create_resource_fail,
    create_resource_success,
    delete_resource_success,
    update_resource_success
)
from radon.model.payload import (
    PayloadCreateResourceFail,
    PayloadCreateResourceSuccess,
    PayloadDeleteResourceSuccess,
    PayloadUpdateResourceSuccess
)
from radon.util import (
    datetime_serializer,
    decode_meta,
    default_cdmi_id,
    encode_meta,
    is_reference,
    merge,
    meta_cdmi_to_cassandra,
    meta_cassandra_to_cdmi,
    metadata_to_list,
    new_request_id,
    now,
    split,
)



class Resource(ABC):
    """Resource Model
    
    A resource represents a node in the hierarchy. It links to a TreeNode 
    object in the Cassandra database.
    
    :param node: The TreeNode row that corresponds to the resource
    :type node: :class:`radon.model.tree_node.TreeNode`
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
        :type node: :class:`radon.model.tree_node.TreeNode`
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
               mimetype=None, size=None, read_access=None, write_access=None,
               req_id=None):
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
        :param read_access: A list of groups with a read access
        :type read_access: list[str]
        :param write_access: A list of groups with a write access
        :type write_access: list[str]
        :param req_id: The id of the request that was made to create a resource
        :type req_id: str, optional
        
        :return: The new Resource object
        :rtype: :class:`radon.model.resource.Resource`
        """
        from radon.model.collection import Collection
        if not container.endswith('/'):
            container += '/'
 
        # Make sure parent/name are not in use.
        path = merge(container, name)

        if not sender:
            sender = cfg.sys_lib_user
        
        existing = cls.find(path)
        if existing:
            create_resource_fail(PayloadCreateResourceFail.default(
                path, "Conflict with a resource", sender))
            return None
        
        # Check if parent collection exists
        parent = Collection.find(container)
        if parent is None:
            create_resource_fail(PayloadCreateResourceFail.default(
                path, "Parent container doesn't exist", sender))
            return None

        now_date = now()
        cdmi_acl = None
        if not metadata:
            user_meta = {}
        else:
            if "cdmi_acl" in metadata:
                # We treat acl metadata in a specific way
                cdmi_acl = metadata["cdmi_acl"]
                del metadata["cdmi_acl"]
            user_meta = {}
            for k in metadata:
                user_meta[k] = encode_meta(metadata[k])
        
        sys_meta = {
            cfg.meta_create_ts: encode_meta(now_date),
            cfg.meta_modify_ts: encode_meta(now_date)
        }
        
        if mimetype:
            sys_meta[cfg.meta_mimetype] = mimetype
        if size:
            sys_meta[cfg.meta_size] = str(size)

        if not url:
            url = "{}{}".format(cfg.protocol_cassandra,
                                default_cdmi_id())
            
        resc_node = TreeNode.create(
            container=container,
            name=name,
            user_meta=user_meta,
            sys_meta=sys_meta,
            object_url=url,
            is_object=True,
        )

        if url.startswith(cfg.protocol_cassandra):
            new = RadonResource(resc_node)
        else:
            new = UrlLibResource(resc_node)
        
        if read_access or write_access:
            new.create_acl_list(read_access, write_access)
        if cdmi_acl:
            new.update_acl_cdmi(cdmi_acl)

        payload_json = {
            "obj": new.mqtt_get_state(),
            'meta' : {
                "sender": sender
            }
        }
        if req_id:
            payload_json['meta']['req_id'] = req_id

        create_resource_success(PayloadCreateResourceSuccess(payload_json))

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
        :param req_id: The id of the request that was made to create a collection
        :type req_id: str, optional
        """
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
            "obj": {"path": self.path},
            'meta' : {
                "sender": sender,
                "req_id": req_id
            }
        }

        self.node.delete()

        delete_resource_success(PayloadDeleteResourceSuccess(payload_json))


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
        :rtype: :class:`radon.model.resource.Resource`
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
        return self.node.get_acl()


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
                cfg.logger.warning(
                    "The acemask for group {0} on resource {1} is invalid".format(
                        gid,
                        self.path
                    )
                )
        return read_access, write_access


    def get_acl_metadata(self):
        """
        Return a dictionary of acl based on the Resource schema

        :return: The acl stored in a dict
        :rtype: dict
        """
        return serialize_acl_metadata(self)


    def get_authorized_actions(self, user):
        """"
        Get available actions for a user according to the groups it belongs
        
        :param user: The user we want to check
        :type user: :class:`radon.model.user.User`
        
        :return: the set of actions the user can do
        :rtype: Set[str]
        """
        # Check permission on the parent container if there's no action
        # defined at this level
        if not self.get_acl_dict():
            from radon.model.collection import Collection
            parent_container = Collection.find(self.container)
            return parent_container.get_authorized_actions(user)
        actions = set([])
        acl = self.node.get_acl()
        for gid in user.get_groups() + [cfg.auth_group]:
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
        return self.node.sys_meta.get(cfg.meta_create_ts)


    def get_list_sys_meta(self):
        """
        Transform a metadata dictionary retrieved from Cassandra to a list of
        tuples. If metadata items are lists they are split into multiple pairs in
        the result list. For system metadata.
    
        :return: a list of pairs (name, value)
        :rtype: list
        """
        return metadata_to_list(self.node.sys_meta, cfg.vocab_dict)


    def get_list_user_meta(self):
        """
        Transform a metadata dictionary retrieved from Cassandra to a list of
        tuples. If metadata items are lists they are split into multiple pairs in
        the result list. For user metadata.
    
        :return: a list of pairs (name, value)
        :rtype: list
        """
        return metadata_to_list(self.node.user_meta)


    def get_user_meta_key(self, key):
        """
        Return the value of a metadata
        
        :param key: The name of the metadata we are looking for
        :type key: str
        
        :return: the value of the metadata, decoded from JSON
        :rtype: object
        """
        return decode_meta(self.node.user_meta.get(key, ""))


    def get_modify_ts(self):
        """
        Get the modification timestamp
        
        :return: The timestamp stored in the system metadata
        :rtype: datetime
        """
        return self.node.sys_meta.get(cfg.meta_modify_ts)


    def get_mimetype(self):
        """
        Return mimetype of the resource
        
        :return: The mimetype
        :rtype: str
        """
        return self.node.sys_meta.get(cfg.meta_mimetype, "")
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
        
        :return: The name of the resource
        :rtype: str
        """
        pass


    def get_path(self):
        """
        Return the full path of the resource
        
        :return: The path of the resource
        :rtype: str
        """
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
        payload["path"] = self.path
        payload["create_ts"] = self.get_create_ts()
        payload["modify_ts"] = self.get_modify_ts()
        payload["metadata"] = self.get_cdmi_user_meta()
        return payload


    @abstractmethod
    def put(self, data):
        """
        Store some data in a Resource stored in Cassandra
        """
        pass


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
            "uuid": self.uuid,
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
        else:
            data["can_read"] = False
            data["can_write"] = False
            data["can_edit"] = False
            data["can_delete"] = False
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
        :param read_access: A list of groups with a read access
        :type read_access: list[str]
        :param write_access: A list of groups with a write access
        :type write_access: list[str]
        :param req_id: The id of the request that was made to create a collection
        :type req_id: str, optional
        
        :return: The updated resource
        :rtype: :class:`radon.model.resource.Resource`
        """
        pre_state = self.mqtt_get_state()
        now_date = now()
        
        # Metadata given in cdmi format are transformed to be stored in Cassandra
        if "metadata" in kwargs:
            metadata = kwargs["metadata"]
            if "cdmi_acl" in metadata:
                # We treat acl metadata in a specific way
                cdmi_acl = metadata["cdmi_acl"]
                del metadata["cdmi_acl"]
                self.update_acl_cdmi(cdmi_acl)
            # Transform the metadata in cdmi format to the format stored in
            # Cassandra
            self.node.user_meta = {}
            self.node.save()
            kwargs["user_meta"] = metadata
            del kwargs["metadata"]

        if "user_meta" in kwargs:
            kwargs["user_meta"] = meta_cdmi_to_cassandra(kwargs["user_meta"])
        
        sys_meta = self.node.sys_meta
        sys_meta[cfg.meta_modify_ts] = encode_meta(now_date)
        kwargs["sys_meta"] = sys_meta
        if "mimetype" in kwargs:
            sys_meta[cfg.meta_mimetype] = kwargs["mimetype"]
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

        if "req_id" in kwargs:
            req_id = kwargs['req_id']
            del kwargs['req_id']
        else:
            req_id = new_request_id()

        self.node.update(**kwargs)
        
        if read_access or write_access:
            self.update_acl_list(read_access, write_access)
        
        resc = Resource.find(self.path)
        post_state = resc.mqtt_get_state()
        
        if (pre_state != post_state):
            payload_json = {
                "obj" : pre_state,
                "new": post_state,
                "meta": {
                    "sender": sender,
                    "req_id": req_id
                }
            }
            update_resource_success(PayloadUpdateResourceSuccess(payload_json))
        return self


    def update_acl_cdmi(self, cdmi_acl):
        """
        Update ACL in the tree_node table from ACL in the cdmi format (list
        of ACE dictionary), existing ACL are replaced
        
        :param cdmi_acl: a cdmi string for acl
        :type cdmi_acl: List[dict]
        """
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
        :type user: :class:`radon.model.user.User`
        
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
    :type obj: :class:`radon.model.data_object.DataObject`
    """

    def __init__(self, node):
        """
        Create the resource object based on the TreeNode row from Cassandra
        
        :param node: The TreeNode row that corresponds to the resource
        :type node: :class:`radon.model.tree_node.TreeNode`
        """
        Resource.__init__(self, node)
        self.obj_id = self.url.replace(cfg.protocol_cassandra, "")
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
        
        :param fh: A file handler we can read
        :type fh: file
        
        :return: The new Data object
        :rtype: :class:`radon.model.data_object.DataObject`
        """
        if not (hasattr(fh, 'read')):
            data = fh
            do = DataObject.create(data, compressed=cfg.compress_do)
        else:
            chunk = fh.read(cfg.chunk_size)
            do = DataObject.create(chunk, compressed=cfg.compress_do)
            seq_num = 1
            while True:
                chunk = fh.read(cfg.chunk_size)
                if not chunk:
                    break
                DataObject.append_chunk(do.uuid, seq_num, chunk, cfg.compress_do)
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



