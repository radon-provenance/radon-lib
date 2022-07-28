# Copyright 2022
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


from dse.cqlengine import connection

import radon
from radon.model import (
    Collection
)
from radon.util import (
    merge
)

class Search(object):
    """Search functionalities
    """


    @classmethod
    def search(cls, terms, user):
        """
        search
        """
        termstrings = ",".join(terms)
        query = """SELECT * FROM tree_node where solr_query='path:{}'""".format(
            termstrings)
        cluster = connection.get_cluster()
        session = cluster.connect(radon.cfg.dse_keyspace)
        rows = session.execute(query)
        
        results = []
        for node_row in rows:
            if not "is_object" in node_row:
                break
            
                #c_dict['result_type'] = 'Resource'
                
            if node_row.get("is_object") == False:
                path = merge(node_row.get("container", '/'),
                             node_row.get("name", '/'))
                coll = Collection.find(path)
                c_dict = coll.to_dict(user)
                c_dict['result_type'] = 'Collection'
                
                results.append(c_dict)

        return results



