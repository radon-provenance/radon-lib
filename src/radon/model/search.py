# Radon Copyright 2022, University of Oxford
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
from cassandra import InvalidRequest

from radon.model.config import cfg
from radon.model.collection import Collection
from radon.model.resource import Resource
from radon.util import merge

class Search(object):
    """Search functionalities
    """


    @classmethod
    def search(cls, solr_query, user=None):
        """
        Search an object in the database
        
        :param solr_query: The solr query that will be sent
        :type solr_query: str
        :param user: A user to check ACL
        :type user: :class:`radon.model.user.User`
        
        :return: A list of rows which match the query
        :rtype: list of dict
        """
        query = """SELECT * FROM tree_node where {}""".format(solr_query)
        
        cluster = connection.get_cluster()
        session = cluster.connect(cfg.dse_keyspace)
        try:
            rows = session.execute(query)
        except InvalidRequest:
            return [] 
        
        
        results = []
        for node_row in rows:
            if node_row.get("is_object") == True:
                path = merge(node_row.get("container", '/'),
                             node_row.get("name", '/'))
                resc = Resource.find(path)
                r_dict = resc.to_dict(user)
                r_dict['result_type'] = 'Resource'
                results.append(r_dict)
            else:
                path = merge(node_row.get("container", '/'),
                             node_row.get("name", '/'))
                coll = Collection.find(path)
                c_dict = coll.to_dict(user)
                c_dict['result_type'] = 'Collection'
                results.append(c_dict)

        return results


