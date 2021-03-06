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

from datetime import datetime
import logging
import json


from radon.models import DataObject, TreeEntry, User
from radon.models.acl import (
    acemask_to_str,
    acl_cdmi_to_cql,
    serialize_acl_metadata
)
from radon.models.errors import NoSuchCollectionError, ResourceConflictError
from radon.util import (
    datetime_serializer,
    decode_meta,
    default_cdmi_id,
    merge,
    meta_cdmi_to_cassandra,
    meta_cassandra_to_cdmi,
    metadata_to_list,
    split,
)

PROTOCOL_CASSANDRA = "cassandra://"


def is_reference(url):
    return not url.startswith(PROTOCOL_CASSANDRA)


class Resource(object):
    """Resource Model"""

    def __init__(self, entry, obj=None):
        self.entry = entry
        self.url = self.entry.url
        self.path = self.entry.path()
        self.container = self.entry.container
        self.name = self.entry.name
        self.is_reference = is_reference(self.url)
        self.uuid = self.entry.uuid
        if not self.is_reference:
            self.obj_id = self.url.replace(PROTOCOL_CASSANDRA, "")
            self.obj = DataObject.find(self.obj_id)
        else:
            self.obj = None

    def __str__(self):
        return self.path

    def chunk_content(self):
        """Get a chunk of the data object"""
        if self.obj:
            return self.obj.chunk_content()
        else:
            return None

    @classmethod
    def create(
        cls,
        container,
        name,
        uuid=None,
        metadata=None,
        url=None,
        mimetype=None,
        username=None,
        size=None,
        checksum=None
    ):
        """Create a new resource in the tree_entry table"""
        from radon.models import Collection
        from radon.models import Notification

        # Check if parent collection exists
        parent = Collection.find(container)
        if parent is None:
            raise NoSuchCollectionError(container)
        if uuid is None:
            uuid = default_cdmi_id()
        create_ts = datetime.now()
        modified_ts = create_ts
        path = merge(container, name)
        if metadata:
            metadata_cass = meta_cdmi_to_cassandra(metadata)
        # Make sure parent/name are not in use.
        existing = cls.find(path)
        if existing:
            raise ResourceConflictError(path)
        kwargs = {
            "container": container,
            "name": name,
            "url": url,
            "uuid": uuid,
        }
        if is_reference(url):
            kwargs["create_ts"] = create_ts
            kwargs["modified_ts"] = modified_ts
            kwargs["mimetype"] = mimetype
            if metadata:
                kwargs["metadata"] = metadata_cass
        else:
            obj_id = url.replace(PROTOCOL_CASSANDRA, "")
            data_obj = DataObject.find(obj_id)
            if checksum:
                data_obj.update(checksum=checksum)
            if metadata:
                data_obj.update(mimetype=mimetype, metadata=metadata_cass)
            else:
                if mimetype:
                    data_obj.update(mimetype=mimetype)
                if size:
                    data_obj.update(size=size)

        data_entry = TreeEntry.create(**kwargs)
        new = Resource(data_entry)

        state = new.mqtt_get_state()
        payload = new.mqtt_payload({}, state)
        Notification.create_resource(username, path, payload)
        # Index the resource
        new.index()
        return new

    def create_acl_list(self, read_access, write_access):
        """Add the ACL from lists of group ids, ACL are replaced"""
        if self.is_reference:
            self.entry.create_entry_acl_list(read_access, write_access)
        else:
            if self.obj:
                self.obj.create_acl_list(read_access, write_access)

    def delete(self, username=None):
        """Delete the resource in the tree_entry table and all the corresponding
        blobs"""
        from radon.models import Notification

        self.delete_blobs()
        self.entry.delete()

        state = self.mqtt_get_state()
        payload = self.mqtt_payload(state, {})
        Notification.delete_resource(username, self.path, payload)
        self.reset()

    def delete_blobs(self):
        """Delete all blobs of the corresponding uuid"""
        if not self.is_reference:
            DataObject.delete_id(self.obj_id)

    @classmethod
    def find(cls, path):
        """Return a resource from a path"""
        coll_name, resc_name = split(path)
        entries = TreeEntry.objects.filter(container=coll_name, name=resc_name)
        if not entries:
            return None
        else:
            return cls(entries.first())

    def full_dict(self, user=None):
        """Return a dictionary which describes a resource for the web ui"""
        data = {
            "uuid": self.uuid,
            "name": self.get_name(),
            "container": self.container,
            "path": self.path,
            "metadata": self.get_list_metadata(),
            "url": self.url,
            "is_reference": self.is_reference,
            "mimetype": self.get_mimetype() or "application/octet-stream",
            "type": self.get_mimetype(),
            "create_ts": self.get_create_ts(),
            "modified_ts": self.get_modified_ts(),
        }
        # Add fields when the object isn't a reference
        if self.obj:
            data["checksum"] = self.get_checksum()
            data["size"] = self.get_size()
        if user:
            data["can_read"] = self.user_can(user, "read")
            data["can_write"] = self.user_can(user, "write")
            data["can_edit"] = self.user_can(user, "edit")
            data["can_delete"] = self.user_can(user, "delete")
        return data

    def get_acl(self):
        if self.is_reference:
            return self.entry.acl
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return self.entry.acl
            return self.obj.acl

    def get_acl_metadata(self):
        """Return a dictionary of acl based on the Resource schema"""
        return serialize_acl_metadata(self)

    def get_authorized_actions(self, user):
        """"Get available actions for user according to a group"""
        # Check permission on the parent container if there's no action
        # defined at this level
        acl = self.get_acl()
        if not acl:
            from radon.models import Collection

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

    def get_cdmi_metadata(self):
        """Return the metadata associated to the object as a CDMI dictionary
        """
        return meta_cassandra_to_cdmi(self.get_metadata())

    def get_checksum(self):
        if self.is_reference:
            return None
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return None
            return self.obj.checksum

    def get_create_ts(self):
        if self.is_reference:
            return self.entry.create_ts
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return self.entry.create_ts
            return self.obj.create_ts

    def get_list_metadata(self):
        """Transform metadata to a list of couples for web ui"""
        return metadata_to_list(self.get_metadata())

    def get_metadata(self):
        if self.is_reference:
            return self.entry.metadata
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return self.entry.metadata
            return self.obj.metadata

    def get_metadata_key(self, key):
        """Return the value of a metadata"""
        return decode_meta(self.get_metadata().get(key, ""))

    def get_mimetype(self):
        #         if self.resource.get_mimetype():
        #             return self.resource.get_mimetype()
        #         mimetype = self.resource.get_metadata_key('cdmi_mimetype')
        #         if mimetype:
        #             return mimetype
        if self.is_reference:
            return self.entry.mimetype
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return self.entry.mimetype
            return self.obj.mimetype

    def get_modified_ts(self):
        if self.is_reference:
            return self.entry.modified_ts
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return self.entry.modified_ts
            return self.obj.modified_ts

    def get_name(self):
        """Return the name of a resource. If the resource is a reference we
        append a trailing '?' on the resource name"""
        # References are object whose url doesn't start with 'cassandra://'
        if self.is_reference:
            return u"{}?".format(self.name)
        else:
            return self.name

    def get_path(self):
        """Return the full path of the resource"""
        return self.path

    def get_size(self):
        if self.is_reference:
            return 0
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return 0
            return self.obj.size

    def index(self):
        from radon.models import SearchIndex

        self.reset()
        SearchIndex.index(self, ["name", "metadata"])

    def mqtt_get_state(self):
        """Get the resource state for the payload"""
        payload = dict()
        payload["uuid"] = self.uuid
        payload["url"] = self.url
        payload["container"] = self.container
        payload["name"] = self.get_name()
        payload["create_ts"] = self.get_create_ts()
        payload["modified_ts"] = self.get_modified_ts()
        payload["metadata"] = self.get_cdmi_metadata()
        return payload

    def mqtt_payload(self, pre_state, post_state):
        """Get a string version of the payload of the message"""
        payload = dict()
        payload["pre"] = pre_state
        payload["post"] = post_state
        return json.dumps(payload, default=datetime_serializer)

    def get_acl_list(self):
        """Return two list of groups id which have read and write access"""
        read_access = []
        write_access = []
        for gid, ace in self.get_acl().items():
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
                pass
        return read_access, write_access


    def reset(self):
        from radon.models import SearchIndex

        SearchIndex.reset(self.path)


    def set_checksum(self, checksum):
        """Set the checksum for the data object. For a reference the checksum 
        is not stored in Cassandra. We may change the model and store is in
        Tree Entry"""
        if self.is_reference:
            return
        else:
            if not self.obj:
                self.obj = DataObject.find(self.obj_id)
                if self.obj is None:
                    return
            self.obj.update(checksum=checksum)


    def simple_dict(self, user=None):
        """Return a dictionary which describes a resource for the web ui"""
        data = {
            "id": self.uuid,
            "name": self.get_name(),
            "container": self.container,
            "path": self.path,
            "is_reference": self.is_reference,
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
        return self.simple_dict(user)

    def update(self, **kwargs):
        """Update a resource"""
        from radon.models import Notification

        pre_state = self.mqtt_get_state()
        kwargs["modified_ts"] = datetime.now()

        # user_uuid used for Notification
        if "username" in kwargs:
            username = kwargs["username"]
            del kwargs["username"]
        else:
            username = None

        # Metadata given in cdmi format are transformed to be stored in Cassandra
        if "metadata" in kwargs:
            kwargs["metadata"] = meta_cdmi_to_cassandra(kwargs["metadata"])

        if self.is_reference:
            self.entry.update(**kwargs)
        else:
            if "url" in kwargs:
                self.entry.update(url=kwargs["url"])
                del kwargs["url"]
            if self.obj:
                self.obj.update(**kwargs)

        resc = Resource.find(self.path)
        post_state = resc.mqtt_get_state()
        payload = resc.mqtt_payload(pre_state, post_state)
        Notification.update_resource(username, resc.path, payload)

        # Index the resource
        resc.index()

    def update_acl_cdmi(self, cdmi_acl):
        """Update ACL in the tree entry table from ACL in the cdmi format (list
        of ACE dictionary), existing ACL are replaced"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        if self.is_reference:
            self.entry.update_entry_acl(cql_string)
        else:
            if self.obj:
                self.obj.update_acl(cql_string)

    def update_acl_list(self, read_access, write_access):
        """Update the ACL from a cdmi list of ACE"""
        if self.is_reference:
            self.entry.update_entry_acl_list(read_access, write_access)
        else:
            if self.obj:
                self.obj.update_acl_list(read_access, write_access)

    def user_can(self, user, action):
        """
        User can perform the action if any of the user's group IDs
        appear in this list for 'action'_access in this object.
        """
        if user.administrator:
            # An administrator can do anything
            return True
        actions = self.get_authorized_actions(user)
        if action in actions:
            return True
        return False
