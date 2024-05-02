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


from cassandra.cqlengine.models import Model
from cassandra.query import SimpleStatement
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection

from radon.model.config import cfg
from radon.util import (
    default_cdmi_id,
    merge
)
from radon.model.acl import (
    Ace,
    acl_list_to_cql
)


class TreeNode(Model):
    """TreeNode model
    
    This is used to store the hierarchy in Cassandra, Collections or Data 
    Objects.
    
    (container, name) is the partition key, it's the path of the element in the
    hierarchy. Collections ends with a '/' like in the CDMI standard. That way
    subcollections are stored closely in Cassandra nodes.
    version is the last part of the primary key so we can keep several versions
    of the hierarchy.
    
    
    :param container: The parent path of the object/collection
    :type container: :class:`columns.Text`
    :param name: The name of the object/collection. Collections ends with '/'
    :type name: :class:`columns.Text`
    :param version: The version of the object/collection
    :type version: :class:`columns.Integer`
    :param uuid: A CDMI uuid
    :type uuid: :class:`columns.Text`
    :param is_object: A boolean to simplify the test
    :type is_object: :class:`columns.Boolean`
    :param object_url: For data object the url to the content of the object. It
       can starts with 'cassandra:// if data is stored in Radon (See 
       :class:`radon.model.data_object.DataObject`
    :type object_url: :class:`columns.Text()`
    :param sys_meta: A Key/Value pair dictionary for system metadata
    :type sys_meta: :class:`columns.Map(columns.Text, columns.Text)`
    :param user_meta: A Key/Value pair dictionary for user metadata. Values are
       stored in JSON
    :type user_meta: :class:`columns.Map(columns.Text, columns.Text)`
    :param acl:  A Key/Value pair dictionary for ACL, a group name and the 
      associated ACE
    :type acl: :class:`columns.Map(columns.Text, columns.UserDefinedType(Ace))`
    """

    # Partitioned by container, clustered by name, so all files for a directory
    # are in the same partition
    container = columns.Text(partition_key=True)
    name = columns.Text(primary_key=True, partition_key=False)
    version = columns.Integer(primary_key=True, partition_key=False, default=0,
                              clustering_order="DESC")
    # UUID are not indexed
    uuid = columns.Text(default=default_cdmi_id)
    is_object = columns.Boolean(default=False)
    
    # URL to a data object if the Tree node is not a container
    # (radon:// for internal objects or anything else for a reference, we do not
    # restrict the syntax of the URL yet, it's up to the client to manage the
    # different URL stored in Cassandra)
    object_url = columns.Text()
    
    sys_meta = columns.Map(columns.Text, columns.Text)
    user_meta = columns.Map(columns.Text, columns.Text)
    acl = columns.Map(columns.Text, columns.UserDefinedType(Ace))

    def add_default_acl(self):
        """Add read access to all authenticated users"""
        self.create_acl_list([cfg.auth_group], [])


    def create_acl(self, acl_cql):
        """
        Replace the acl with the given cql string
        
        :param acl_cql: The acl string to put in Cassandra, can be easily
          generated in :meth:`radon.model.acl.acl_list_to_cql`
        :type acl_cql: str
        """
        session = connection.get_session()
        keyspace = cfg.dse_keyspace
        session.set_keyspace(keyspace)
        query = SimpleStatement(
            u"""UPDATE tree_node SET acl={} 
            WHERE container=%s and name=%s and version=%s""".format(
                acl_cql
            )
        )
        session.execute(query, (self.container, self.name, self.version))


    def create_acl_list(self, read_access, write_access):
        """
        Create ACL from lists of group uuids
        
        :param read_access: A list of group names which have read access
        :type read_access: List[str]
        :param write_access: A list of group names which have write access
        :type write_access: List[str]
        """
        cql_string = acl_list_to_cql(read_access, write_access)
        self.create_acl(cql_string)


    def get_acl(self):
        """
        Get ACL from the table
        
        :return: The ACL stored in Cassandra
        :rtype: dict
        """
        session = connection.get_session()
        keyspace = cfg.dse_keyspace
        session.set_keyspace(keyspace)
        query = SimpleStatement(
            u"""SELECT acl FROM tree_node
            WHERE container=%s and name=%s and version=%s""")
        rows = session.execute(query, (self.container, self.name, self.version))
        if rows:
            acl = rows.one().get("acl")
            # if no acl it would return None instead of {}
            if not acl:
                return {}
            else:
                return acl
        else:
            return {}


    def path(self):
        """
        Get the full path of the element. See :meth:`radon.util.merge`
    
        :return: The merged path
        :rtype: str
        """
        return merge(self.container, self.name)


    def update_acl(self, acl_cql):
        """
        Update the acl with the given cql string that will be added
        
        :param acl_cql: The acl string to put in Cassandra, can be easily
          generated in :meth:`radon.model.acl.acl_list_to_cql`
        :type acl_cql: str
        """
        session = connection.get_session()
        keyspace = cfg.dse_keyspace
        session.set_keyspace(keyspace)
        query = SimpleStatement(
            u"""UPDATE tree_node SET acl=acl+{} 
            WHERE container=%s and name=%s and version=%s""".format(
                acl_cql
            )
        )
        session.execute(query, (self.container, self.name, self.version))



    def update_acl_list(self, read_access, write_access):
        """
        Update ACL from lists of group uuids
        
        :param read_access: A list of group names which have read access
        :type read_access: List[str]
        :param write_access: A list of group names which have write access
        :type write_access: List[str]
        """
        cql_string = acl_list_to_cql(read_access, write_access)
        self.update_acl(cql_string)



