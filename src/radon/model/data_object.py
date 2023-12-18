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


from io import BytesIO
import zipfile
from dse.cqlengine import columns, connection
from dse.query import SimpleStatement
from dse.cqlengine.models import Model

from radon.model.config import cfg
from radon.util import default_cdmi_id


static_fields = [
    "checksum",
    "size"
]


class DataObject(Model):
    """ The DataObject represents actual data objects, the hierarchy
    references it.

    Each partition key gathers together all the data under one partition (the
    CDMI ID ) and the object properties are represented using static columns
    (one instance per partition)
    It has a similar effect to a join to a properties table, except the
    properties are stored with the rest of the partition

    This is an 'efficient' model optimised for Cassandra's quirks.
    
    
    :param uuid: A CDMI uuid (partition key)
    :type uuid: :class:`columns.Text`
    :param sequence_number: This is the 'clustering' key, a data is split in
      several blobs, with the same id qnd different sequence number.
    :type sequence_number: :class:`columns.Integer`
    :param checksum: A checksum to verify the integrity od the data
    :type checksum: :class:`columns.Text`
    :param size: Total size of the data
    :type size: :class:`columns.Integer`
    :param blob: The binary bits to store
    :type blob: :class:`columns.Blob`
    :param compressed: An option to compress the data bits
    :type compressed: :class:`columns.Boolean`
    """

    # The 'name' of the object
    uuid = columns.Text(default=default_cdmi_id, required=True, partition_key=True)
    # This is the 'clustering' key
    sequence_number = columns.Integer(primary_key=True, partition_key=False)
    # These columns are shared between all entries with same id (static attributes)
    checksum = columns.Text(static=True)
    size = columns.BigInt(default=0, static=True)
    blob = columns.Blob(required=False)
    compressed = columns.Boolean(default=False)


    @classmethod
    def append_chunk(cls, uuid, sequence_number, raw_data, compressed=False):
        """
        Create a new blob for an existing data_object
        
        :param uuid: A CDMI uuid
        :type uuid: str
        :param sequence_number: The sequence number, this has to be different
        :type sequence_number: int
        :param raw_data: the binary bits
        :type raw_data: str
        :param compressed: An option to compress the data bits
        :type compressed: bool, optional
        """
        if compressed:
            f = BytesIO()
            z = zipfile.ZipFile(f, "w", zipfile.ZIP_DEFLATED)
            z.writestr("data", raw_data)
            z.close()
            data = f.getvalue()
            f.close()
        else:
            data = raw_data
        data_object = cls(
            uuid=uuid, sequence_number=sequence_number, blob=data,
            compressed=compressed
        )
        data_object.save()
        return data_object


    def chunk_content(self):
        """
        Yields the content for a generator, one chunk at a time. 
        
        :return: A chunk of data bits
        :rtype: str
        """
        entries = DataObject.objects.filter(uuid=self.uuid)
        for entry in entries:
            if entry.compressed:
                data = BytesIO(entry.blob)
                z = zipfile.ZipFile(data, "r")
                content = z.read("data")
                data.close()
                z.close()
                yield content
            else:
                yield entry.blob


    @classmethod
    def create(cls, raw_data, compressed=False):
        """
        Create a Data Object blob with the content passed in parameter
        
        :param raw_data: The binary bits to store
        :type raw_data: str
        :param compressed: An option to compress the data bits
        :type compressed: bool, optional
        
        :return: The new Data Object
        :rtype: :class:`radon.model.DataObject`
        """
        new_id = default_cdmi_id()
        if compressed:
            f = BytesIO()
            z = zipfile.ZipFile(f, "w", zipfile.ZIP_DEFLATED)
            z.writestr("data", raw_data)
            z.close()
            data = f.getvalue()
            f.close()
        else:
            data = raw_data
 
        kwargs = {
            "uuid": new_id,
            "sequence_number": 0,
            "blob": data,
            "compressed": compressed,
            "size": len(data)
        }
        new = super(DataObject, cls).create(**kwargs)
        return new


    @classmethod
    def delete_id(cls, uuid):
        """
        Delete all blobs for the specified uuid
        
        :param uuid: A CDMI uuid
        :type uuid: str
        """
        session = connection.get_session()
        keyspace = cfg.dse_keyspace
        session.set_keyspace(keyspace)
        query = SimpleStatement("""DELETE FROM data_object WHERE uuid=%s""")
        session.execute(query, (uuid,))


    @classmethod
    def find(cls, uuid):
        """
        Find an object by uuid
        
        :param uuid: A CDMI uuid
        :type uuid: str
        
        :return: The first DataObject of the partition corresponding to the 
          UUID
        :rtype: :class:`radon.model.DataObject`
        """
        entries = cls.objects.filter(uuid=uuid)
        if not entries:
            return None
        else:
            return entries.first()


    def get_url(self):
        """
        Get the URL of the Data Object that we use as reference in the 
        hierarchy
        
        :return: An URL that informs that the data is in Cassandra + the UUID
        :rtype: str
        """
        return cfg.protocol_cassandra + self.uuid




