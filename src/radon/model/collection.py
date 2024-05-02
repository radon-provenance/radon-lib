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


from cassandra.cqlengine import connection
from cassandra.query import SimpleStatement
import json

from radon.model.config import cfg
from radon.model.tree_node import TreeNode
from radon.model.acl import (
    acemask_to_str,
    acl_cdmi_to_cql,
    serialize_acl_metadata
)
from radon.util import (
    decode_meta,
    encode_meta,
    meta_cassandra_to_cdmi,
    meta_cdmi_to_cassandra,
    metadata_to_list,
    merge,
    new_request_id,
    now,
    split,
)
from radon.model.errors import (
    CollectionConflictError,
    ResourceConflictError,
    NoSuchCollectionError
)
from radon.model.notification import (
    create_collection_fail,
    create_collection_success,
    delete_collection_success,
    update_collection_success,
)
from radon.model.payload import (
    PayloadCreateCollectionFail,
    PayloadCreateCollectionSuccess,
    PayloadDeleteCollectionSuccess,
    PayloadUpdateCollectionSuccess,
)



class Collection(object):
    """Collection model
    
    A collection represents a node in the hierarchy. It links to a TreeNode 
    object in the Cassandra database.
    
    :param node: The TreeNode row that corresponds to the collection
    :type node: :class:`radon.model.tree_node.TreeNode`
    :param is_root: The root collection usually needs special treatment
    :type is_root: bool
    :param name: The name of the collection, should end with '/'
    :type name: str
    :param container: The name of the parent collection
    :type container: str
    :param path: The full path of the collection in the hierarchy
    :type path: str
    :param uuid: UUID of the TreeNode
    :type uuid: str
    """

    def __init__(self, node):
        """
        Create the collection object based on the TreeNode row from Cassandra
        
        :param node: The TreeNode row that corresponds to the collection
        :type node: :class:`radon.model.tree_node.TreeNode`
        """
        self.node = node

        self.is_root = self.node.name == "." and self.node.container == "/"
        # Get name
        if self.is_root:
            self.name = "Home"
            self.path = "/"
            self.container = "/"
        else:
            # Name ends with "/" to follow CDMI standard and differentiate
            # collections and data objects
            self.name = self.node.name
            self.container = self.node.container
            self.path = merge(self.container, self.name)
        
        self.uuid = self.node.uuid

    @classmethod
    def create(cls, container, name, metadata=None, sender=None, 
               read_access=None, write_access=None, req_id=None):
        """
        Create a new collection
        
        :param container: The name of the parent collection
        :type container: str
        :param name: The name of the collection, should end with '/'
        :type name: str
        :param metadata: A Key/Value pair dictionary for user metadata
        :type metadata: dict, optional
        :param sender: The name of the user who created the collection
        :type sender: str, optional
        :param read_access: A list of groups with a read access
        :type read_access: list[str]
        :param write_access: A list of groups with a write access
        :type write_access: list[str]
        :param req_id: The id of the request that was made to create a collection
        :type req_id: str, optional
        
        :return: The new Collection object
        :rtype: :class:`radon.model.collection.Collection`
        """
        from radon.model.resource import Resource
                
        if not name.endswith("/"):
            name = name + '/'
        if not container.endswith("/"):
            container = container + '/'
            
        path = merge(container, name)

        if not sender:
            sender = cfg.sys_lib_user

        # Check if parent collection exists
        parent = Collection.find(container)
        if parent is None:
            create_collection_fail(
                PayloadCreateCollectionFail.default(
                    path, "Parent container doesn't exist", sender))
            return None
        resource = Resource.find(merge(container, name))
        if resource is not None:
            create_collection_fail(
                PayloadCreateCollectionFail.default(
                    path, "Conflict with a resource", sender))
            return None
        collection = Collection.find(path)
        if collection is not None:
            create_collection_fail(
                PayloadCreateCollectionFail.default(
                    path, "Conflict with a collection", sender))
            return None

        now_date = now()
        if not metadata:
            user_meta = {}
        else:
            user_meta = metadata
        sys_meta = {
            cfg.meta_create_ts: encode_meta(now_date),
            cfg.meta_modify_ts: encode_meta(now_date)
        }

        coll_node = TreeNode.create(
            container=container,
            name=name,
            user_meta=user_meta,
            sys_meta=sys_meta
        )
        
        new = cls(coll_node)
        
        if read_access or write_access:
            new.create_acl_list(read_access, write_access)
        
        payload_json = {
            "obj": new.mqtt_get_state(),
            "meta": {"sender": sender}
        }
        if req_id:
            payload_json['meta']['req_id'] = req_id
        create_collection_success(PayloadCreateCollectionSuccess(payload_json))

        return new


    @classmethod
    def create_root(cls):
        """
        Create the root Collection to initialise the hierarchy
        
        :return: The Collection object for the root
        :rtype: :class:`radon.model.collection.Collection`
        """
        now_date = now()
        sys_meta = {
            cfg.meta_create_ts: encode_meta(now_date),
            cfg.meta_modify_ts: encode_meta(now_date)
        }
        # If the root already exist it won't be replaced as PK will be the same
        root_node = TreeNode.create(
            container="/",
            name=".",
            sys_meta=sys_meta,
        )
        root_node.add_default_acl()
        new = cls(root_node)
        return new


    @classmethod
    def delete_all(cls, path, sender=None):
        """
        Delete recursively all sub-collections and all resources contained
        in a collection at 'path'
        
        :param path: The full path in the hierarchy
        :type path: str
        :param sender: The name of the user who deleted the collection
        :type sender: str, optional
        """ 
        parent = cls.find(path)
        if not parent:
            return
        else:
            parent.delete(sender=sender)


    @classmethod
    def find(cls, path, version=None):
        """
        Find a collection by path, initialise the collection with the
        appropriate row in the tree_node table
        
        :param path: The full path in the hierarchy
        :type path: str
        :param version: The specific version we want to find
        :type version: int, optional
        
        :return: The Collection object which maps the TreeNode
        :rtype: :class:`radon.model.collection.Collection`
        """
        # If version is not provided we need to query first to get all versions
        # and read the current version in any row (static column)
        if not path.endswith("/"):
            return None
        if path == "/":
            container = '/'
            name = '.'
        else:
            if not path.startswith("/"):
                path = '/' + path
            container, name = split(path)
        # If version is not specified we make a first query to get the current
        # version
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
            return cls(qnodes.first())


    @classmethod
    def get_root(cls):
        """
        Return the root collection, Create it if it doesn't exist
        
        :return: The Collection object for the root
        :rtype: :class:`radon.model.collection.Collection`
        """
        root = Collection.find("/")
        if not root:
            root = Collection.create_root()
        return root


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
        Delete a collection and the associated row in the tree_node table
        
        :param sender: The name of the user who deleted the collection
        :type sender: str, optional
        :param req_id: The id of the request that was made to create a collection
        :type req_id: str, optional
        """
        from radon.model.resource import Resource
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
 
        if self.is_root:
            return
        
        # We don't need the suffixes for the resources, otherwise we won't 
        # find them
        child_container, child_dataobject = self.get_child(False)
        for child_str in child_container:
            child = Collection.find(self.path + child_str)
            if child:
                child.delete(sender=sender, req_id=req_id)
        for child_str in child_dataobject:
            child = Resource.find(self.path + child_str)
            if child:
                child.delete(sender=sender, req_id=req_id)

        payload_json = {
            "obj": {"path": self.path},
            "meta": {
                "sender": sender,
                "req_id": req_id
            }
        }

        session = connection.get_session()
        keyspace = cfg.dse_keyspace
        session.set_keyspace(keyspace)
        query = SimpleStatement("""DELETE FROM tree_node WHERE container=%s and name=%s""")
        session.execute(query, (self.container, self.name, ))

        delete_collection_success(PayloadDeleteCollectionSuccess(payload_json))


    def get_acl_dict(self):
        """
        Return ACL in a dictionary 
        
        :return: The ACL associated to the collection
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
        for gid, ace in self.node.get_acl().items():
            oper = acemask_to_str(ace.acemask, False)
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
                    "The acemask for group {0} on collection {1} is invalid".format(
                        gid,
                        self.path
                    )
                )
        return read_access, write_access


    def get_acl_metadata(self):
        """
        Return a dictionary of acl based on the Collection schema

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
        if not user:
            return set([])
        if (user.administrator):
            return set(["read", "write", "delete", "edit"])
        # Check permission on the parent container if there's no action
        # defined at this level
        if not self.get_acl_dict():
            # By default root collection should have read access for all 
            # authenticated users
            if self.is_root:
                return set([])
            else:
                parent_container = Collection.find(self.container)
                return parent_container.get_authorized_actions(user)
        actions = set([])
        acl = self.node.get_acl()
        for gid in user.get_groups() + [cfg.auth_group]:
            if gid in acl:
                ace = acl[gid]
                level = acemask_to_str(ace.acemask, False)
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


    def get_create_ts(self):
        """
        Get the creation timestamp
        
        :return: The timestamp stored in the system metadata
        :rtype: datetime
        """
        return self.node.sys_meta.get(cfg.meta_create_ts)


    def get_modify_ts(self):
        """
        Get the modification timestamp
        
        :return: The timestamp stored in the system metadata
        :rtype: datetime
        """
        return self.node.sys_meta.get(cfg.meta_modify_ts)


    def get_child(self, add_ref_suffix=True):
        """
        Return two lists for child container and child dataobjects
        
        :param add_ref_suffix: if add_ref_suffix is true the data object names 
          are modified to include '?' for a reference and '#' for a missing 
          object url
        :type add_ref_suffix: bool
        
        :return: the lists of subcollection and subobjects
        :rtype: Tuple[List[str],List[str]]
        """
        nodes = TreeNode.objects.filter(container=self.path)
        child_container = []
        child_dataobject = []
        for node in list(nodes):
            # If name = '.' that's the root collection
            if node.name == ".":
                continue
            elif node.name.endswith("/"):
                subcoll_name = node.name
                # Do not add several versions of the same object (not efficient)
                if subcoll_name not in child_container:
                    child_container.append(subcoll_name)
            else:
                do_name = node.name
                if add_ref_suffix:
                    if node.object_url:
                        if not node.object_url.startswith(cfg.protocol_cassandra):
                            do_name = "{}?".format(do_name)
                    else:
                        do_name = "{}#".format(do_name)
                # Do not add several versions of the same object (not efficient)
                if do_name not in child_dataobject:
                    child_dataobject.append(do_name)
        return (child_container, child_dataobject)


    def get_child_resource_count(self):
        """
        Return the number of resources in the collection
        
        :return: the number of resources
        :rtype: int
        """
        _, child_dataobject = self.get_child()
        return len(child_dataobject)


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


    def mqtt_get_state(self):
        """
        Get the collection state that will be used in the payload
        
        :return: The collection state as a dictionary
        :rtype: dict
        """
        payload = dict()
        payload["uuid"] = self.uuid
        payload["container"] = self.container
        payload["name"] = self.name
        payload["path"] = self.path
        payload["create_ts"] = self.get_create_ts()
        payload["modify_ts"] = self.get_modify_ts()
        payload["metadata"] = self.get_cdmi_user_meta()
        return payload


    def to_dict(self, user=None):
        """
        Return a dictionary which describes a collection for the web ui
        
        :param user: If present, display the actions this specific user can do
        :type user: :class:`radon.model.user.User`
        
        :return: The dictionary with the information needed for the UI
        :rtype: dict
        """
        data = {
            "uuid": self.uuid,
            "container": self.path,
            "name": self.name,
            "path": self.path,
            "created": self.get_create_ts(),
            "user_meta": self.get_list_user_meta(),
            "sys_meta": self.get_list_sys_meta(),
        }
        if user:
            data["can_read"] = self.user_can(user, "read")
            data["can_write"] = self.user_can(user, "write")
            data["can_edit"] = self.user_can(user, "edit")
            data["can_delete"] = self.user_can(user, "delete")
        return data


    def update(self, **kwargs):
        """
        Update a collection. We intercept the call to encode the metadata if we
        modify it. Metadata passed in this method are stored as user metadata.
        
        :param sender: the name of the user who made the action
        :type sender: str, optional
        :param metadata: The plain password to encrypt
        :type metadata: dict
        :param read_access: A list of groups with a read access
        :type read_access: list[str]
        :param write_access: A list of groups with a write access
        :type write_access: list[str]
        :param req_id: The id of the request that was made to create a collection
        :type req_id: str, optional
        
        :return: The updated collection
        :rtype: :class:`radon.model.collection.Collection`
        """
        pre_state = self.mqtt_get_state()
        now_date = now()
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

        if "req_id" in kwargs:
            req_id = kwargs['req_id']
            del kwargs['req_id']
        else:
            req_id = new_request_id()

        self.node.update(**kwargs)

        if read_access or write_access:
            self.update_acl_list(read_access, write_access)
        
        coll = Collection.find(self.path)
        post_state = coll.mqtt_get_state()
        
        if (pre_state != post_state):
            payload_json = {
                "obj" : pre_state,
                "new": post_state,
                "meta": {
                    "sender": sender,
                    "req_id": req_id
                }
            }
            update_collection_success(PayloadUpdateCollectionSuccess(payload_json))
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


