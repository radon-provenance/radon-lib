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


import base64
from crcmod.predefined import mkPredefinedCrcFun
from datetime import (
    date,
    datetime,
    timedelta,
    timezone
)
from dse.util import uuid_from_time
import json
import mimetypes
import os
import random
import string
from passlib.hash import pbkdf2_sha256
import struct
import uuid
import ldap

import radon


IDENT_PEN = 42223
# CDMI ObjectId Length: 8 bits header + 16bits uuid
IDENT_LEN = 24
 
 
def _calculate_crc16(id_):
    """Calculate and return the CRC-16 for the given identifier. Return the 
    CRC-16 integer value.
 
    :param id_: The id being created
    :type id_: bytearray (bytestring in Python 2)
    
    :return: the CRC-16 integer value
    :rtype: int
    """
    # Coerce to bytearray. If already a bytearray this will create a copy
    # so as to avoid side-effects of manipulation for CRC calculation
    id_ = bytearray(id_)
    # Reset CRC bytes in copy to 0 for calculation
    id_[6] = 0
    id_[7] = 0
    # Need to generate CRC func
    crc16fun = mkPredefinedCrcFun("crc-16")
    crc16 = crc16fun(id_)
    # Return a 2 byte string representation of the resulting integer
    # in network byte order (big-endian)
    return crc16
 
 
def _get_blank_id():
    """Return a blank CDMI compliant ID.
 
    Return a blank CDMI compliant ID with enterprise number etc.
    pre-initialized.
 
    Enterprise Number:
 
        The Enterprise Number field shall be the SNMP enterprise number of
        the offering organization that created the object ID, in network
        byte order. See RFC 2578 and
        http://www.iana.org/assignments/enterprise-numbers.
        0 is a reserved value.
 
     :return: Return a blank CDMI compliant ID 
     :rtype: bytearray
    """
    id_length = IDENT_LEN
    # TODO: add exceptions back
    #     if id_length < 9 or id_length > 40:
    #         raise InvalidOptionConfigException(
    #             'identifiers',
    #             'length',
    #             id_length,
    #             "Identifier length must be at least 9 and no more than 40"
    #         )
    id_ = bytearray([0] * id_length)
    # Set IANA Private Enterprise Number
    #
    # CDMI Spec: Enterprise Number should be in network byte order
    # (big-endian) for 3 bytes starting at byte 1
    # struct cannot pack an integer into 3 bytes, instead
    # pack the PEN into 2 bytes starting at byte 2
    pen = IDENT_PEN
    struct.pack_into("!H", id_, 2, pen)
    # Set ID length
    #
    # CDMI Spec: Length should be 1 bytes starting at byte 5
    # struct cannot pack an integer into 1 byte, instead
    # pack the PEN into 2 bytes starting at byte 4
    # Byte 4 is reserved (zero) but length will not exceed 256 so will
    # only occupy byte 5 when back in network byte order (big-endian)
    struct.pack_into("!H", id_, 4, id_length)
    return id_
 
 
def _insert_crc16(id_):
    """Calculate and insert the CRC-16 for the identifier.
 
    Calculate the CRC-16 value for the given identifier, insert it
    into the given identifier and return the resulting identifier.
    :return: the bytearray where CRC-16 has been inserted
    :rtype: bytearray
    """
    crc16 = _calculate_crc16(id_)
    struct.pack_into("!H", id_, 6, crc16)
    return id_
 
 
def datetime_serializer(obj):
    """Convert a datetime object to its string representation for JSON serialization.
    
    :param obj: the datetime object we want to serialize
    :type obj: datetime
    
    :return: the datetime serialized in a string
    :rtype: str
    """
    if isinstance(obj, datetime):
        return obj.isoformat()


def datetime_unserializer(d_str):
    """Convert a string representation to a datetime object for JSON unserialization.
    
    :param d_str: the datetime string we want to unserialize
    :type d_str: str
    
    :return: the datetime unserialized to a datetime
    :rtype: datetime
    """
    return datetime.fromisoformat(d_str)


def decode_datetime(value):
    """Decode a datetime value stored in a JSON string
    
    :param value: the datetime stored in a JSON string
    :type value: str
    
    :return: the decoded datetime
    :rtype: datetime    
    """
    str_v = decode_meta(value)
    return datetime_unserializer(str_v)


def decode_meta(value):
    """Decode a specific metadata value, metadata are stored as JSON
    
    :param value: the metadata stored in a JSON string
    :type value: str
    
    :return: the decoded metadata
    :rtype: depends on the value, can be str, list, dict, ...
    """
    try:
        # Values are stored as json strings
        val = json.loads(value)
    except ValueError:
        val = value
    return val


def default_cdmi_id():
    """Generate a new uuid with add the CDMI header
    
    :return: the uuid in a string
    :rtype: str
    """
    # Get a blank CDMI ID
    id_ = _get_blank_id()
    # Pack after first 8 bytes of identifier in network byte order
    # (big-endian)
    uid = uuid.uuid4()
    struct.pack_into("!16s", id_, 8, uid.bytes)
    # Calculate and insert the CRC-16
    id_ = _insert_crc16(id_)
 
    bytes_id = base64.b16encode(id_)
    return bytes_id.decode()


def default_date():
    """Return a string representing the current date
    
    >>> default_date()
    '20210729'
    
    :return: A date in a string
    :rtype: str
    """
    return datetime.now().strftime("%Y%m%d")


def default_time():
    """Generate a TimeUUID from the current local date and time.  uuid 
    generator is provided by DSE, to be stored in Cassandra
    
    :return: A uuid
    :rtype: uuid.UUID
    
    """
    return uuid_from_time(datetime.now())


def default_uuid():
    """Generate a new uuid, to be stored as strings in Cassandra
    
    :return: A uuid
    :rtype: str
    """
    return str(uuid.uuid4())


def encode_meta(meta):
    """Encode a metadata value to be stored as json in Cassandra", datetime has 
    its own serializer.
    
    :param metadata: the metadata to be encoded as JSON string
    :type value: any value that can be dump by the JSON module
    
    :return: A JSON dump of the metadata
    :rtype: str
    """
    return json.dumps(meta, ensure_ascii=False, sort_keys=True,
                      default=datetime_serializer)


def encrypt_password(plain):
    """Encrypt a password using sha256
    
    :param plain: a string containing the password to encode
    :type plain: str
     
     :return: a password hash
     :rtype: str
    """
    return pbkdf2_sha256.encrypt(plain)


def guess_mimetype(filepath):
    """Try to guess the mimetype of a file given its filename
    
    :param filepath: The name of the file we try to guess
    :type filepath: str
     
     :return: a mimetype
     :rtype: str
    """
    type_, enc_ = mimetypes.guess_type(filepath)
    if not type_:
        if enc_ == "bzip2":
            mimetype = "application/x-bzip2"
        elif enc_ == "gzip":
            mimetype = "application/gzip"
        else:
            mimetype = "application/octet-stream"
    else:
        if enc_ in ["gzip", "bzip2"] and type_ == "application/x-tar":
            mimetype = "application/x-gtar"
        else:
            mimetype = type_
    return mimetype


def is_collection(path):
    """Check if the collection exists
    
    :param path: The path of the collection in Radon
    :type path: str
     
     :return: a boolean
     :rtype: bool
     """
    from radon.model import Collection
    return Collection.find(path) is not None


def is_reference(url):
    """Check if the url stored for a resource links to an external object or
    links to a data object stored in Radon
    
    :param path: The path of the resource in Radon
    :type path: str
     
     :return: a boolean
     :rtype: bool
    """
    if url:
        return not url.startswith(radon.cfg.protocol_cassandra)
    else:
        return False


def is_resource(path):
    """Check if the resource exists
    
    :param path: The path of the resource in Radon
    :type path: str
     
     :return: a boolean
     :rtype: bool
    """
    from radon.model import Resource
    return Resource.find(path) is not None


def last_x_days(days=50):
    """Return the last X days as string names YYYYMMDD in a list
    
    >>> last_x_days(2)
    ['20210729', '20210728']
    
    :param days: Number of days we want to retrieve in the list
    :type days: int
    
    :return: a list of days strings
    :rtype: list
    """
    dt = datetime.now()
    dates = [dt] + [dt + timedelta(days=-x) for x in range(1, days)]
    return [d.strftime("%Y%m%d") for d in dates]
 
 
def merge(coll_name, resc_name):
    """Create a full path from a collection name and a resource name
    
    >>> merge('/a','b')
    '/a/b'
    >>> merge('/a/','b')
    '/a/b'
    
    :param coll_name: The path of the collection
    :type coll_name: str
    
    :param resc_name: The name of the resource/collection
    :type resc_name: str
    
    :return: The merged path
    :rtype: str
    """
    if coll_name.endswith("/"):
        # We don't add an extra '/' if it's already there
        return u"{}{}".format(coll_name, resc_name)
    else:
        return u"{}/{}".format(coll_name, resc_name)


def meta_cassandra_to_cdmi(metadata):
    """Transform a metadata dictionary retrieved from Cassandra to a CDMI
    metadata dictionary
    metadata values in Cassandra are stored as JSON
    metadata values in CDMI are stored as str

    :param metadata: metadata values dict from Cassandra 
    :type metadata: dict
    
    :return: a dictionary with decoded JSON strings
    :rtype: dict
    """
    md = {}
    for k, v in metadata.items():
        # Values are stored as json strings
        val = decode_meta(v)
        # meta with no values are deleted (not easy to delete them with
        # cqlengine)
        if val:
            md[k] = val
    return md
 
 
def meta_cdmi_to_cassandra(metadata):
    """Transform a metadata dictionary from CDMI request to a metadata
    dictionary that can be stored in a Cassandra Model.
    metadata values in Cassandra are stored as JSON
    metadata values in CDMI are stored as str

    :param metadata: metadata values dict
    :type metadata: dict
    
    :return: a dictionary with values encoded in JSON strings
    :rtype: dict
    """
    d = {}
    for key, value in metadata.items():
        # Don't store metadata without value
        if not value:
            continue
        d[key] = encode_meta(value)
    return d


def metadata_to_list(metadata, vocab_dict=None):
    """Transform a metadata dictionary retrieved from Cassandra to a list of
    tuples. If metadata items are lists they are split into multiple pairs in
    the result list
    
    :param metadata: A dictionary of metadata with values encoded as JSON
    :type metadata: dict
    :param vocab_dict: a dictionary we can use to change the names of the 
    metadata for 'prettyprinting' the output
    :type vocab_dict: dict
    
    :return: a list of pairs (name, value)
    :rtype: list
    """
    res = []
    for k, v in metadata.items():
        if vocab_dict:
            # If we use vocab_dict to pretty print display we also
            # deserialize date times
            str_v = decode_meta(v)
            if k in radon.cfg.meta_datetimes:
                try:
                    d = datetime.strptime(str_v, "%Y-%m-%dT%H:%M:%S.%f%z")
                    str_v = d.strftime("%A %d %B %Y - %H:%M:%S (%z)")
                except ValueError:
                    pass
            
            res.append((vocab_dict.get(k, k), str_v))
        else:
            res.append((k, decode_meta(v)))
    return res

def now():
    """Return the current UTC date/time
    
    :return: the current datetime
    :rtype: datetime"""
    return datetime.now(timezone.utc)


def path_exists(path):
    """Check if the path is already in use
    
    :param path: a path in the collection
    :type path: str
    
    :return: A boolean to say if the path already exists
    :rtype: bool
    """
    return is_resource(path) or is_collection(path)


def random_password(length=10):
    """Generate a random string of fixed length
    
    :param length: The size of the password
    :type param: int
    
    :return: A random password of size 'length'
    :rtpe; str
    """
    letters = string.ascii_letters + string.digits + string.punctuation
    return "".join(random.choice(letters) for _ in range(length))


def split(path):
    """Parse a full path and return the collection and the resource name
    If the path ends by a '/' that's a collection

    :param path: a path in the collection
    :type path: str
    
    :return: a pair with the collection name and the resource/collection name
    :type: tuple
    """
    if path == '/':
        return tuple(('/', '.'))
    if path.endswith('/'):
        pi = path[:-1].rfind('/')
    else:
        pi = path.rfind('/')

    coll_name = path[:pi+1]
    resc_name = path[pi+1:]
    
    # root collection
    if coll_name == '':
        coll_name = '/'
    
    return tuple((coll_name, resc_name))


def verify_ldap_password(username, password):
    """Try to authenticate against an existing ldap server
    
    :param username: the username we want to test
    :type username: str
    :param password: the plain password to test
    :type password: str
    
    :return: a boolean which indicate if the password has been accepted by the 
    ldap server
    :rtype: bool
    """
    server_uri = radon.cfg.auth_ldap_server_uri
    dn_template = radon.cfg.auth_ldap_user_dn_template
    
    if server_uri is None:
        return False 
    if dn_template is None:
        return False
    try:
        connection = ldap.initialize(server_uri)
        connection.protocol_version = ldap.VERSION3
        user_dn = dn_template % {"user": username}
        connection.simple_bind_s(user_dn, password)
        return True
    except ldap.INVALID_CREDENTIALS:
        return False
    except ldap.SERVER_DOWN:
        # TODO: Return error instead of none
        return False


def verify_password(password, hash):
    """Check user password against an existing hash (hash)
    
    :param password: the password we want to test
    :type password: str
    :param hash: the hash we test against
    :type hash: str
    
    :return: a boolean which indicate if the password is correct
    :rtype: bool
    """
    return pbkdf2_sha256.verify(password, hash)




