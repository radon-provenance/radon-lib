"""Copyright 2021

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

from dse.util import uuid_from_time
from datetime import (
    date,
    datetime
)
import uuid
import ldap

from radon import cfg
from radon.util import(
    datetime_serializer,
    datetime_unserializer,
    decode_meta,
    decode_datetime,
    default_cdmi_id,
    default_date,
    default_time,
    default_uuid,
    encode_meta,
    encrypt_password,
    guess_mimetype,
    is_collection,
    is_reference,
    is_resource,
    last_x_days,
    merge,
    meta_cassandra_to_cdmi,
    meta_cdmi_to_cassandra,
    metadata_to_list,
    now,
    path_exists,
    random_password,
    split,
    verify_ldap_password,
    verify_password
)
from radon.model import (
    Collection,
    Resource
)
from radon.database import (
    connect,
    create_root,
    create_tables,
    destroy,
    initialise,
)


TEST_KEYSPACE = "test_keyspace"
TEST_URL = "http://www.google.fr"


def setup_module():
    cfg.dse_keyspace = TEST_KEYSPACE
    initialise()
    connect()
    create_tables()
    create_root()
    try:
        coll = Collection.create("/", "coll1")
        ref1 = Resource.create("/", "test.url", url=TEST_URL)
        resc = Resource.create("/coll1", "test.txt")
        ref2 = Resource.create("/coll1", "test.url", url=TEST_URL)
    except: # If collections or resources already exist
        pass


def teardown_module(module):
    destroy()


def test_default_cdmi_id():
    cdmi_id_1 = default_cdmi_id()
    cdmi_id_2 = default_cdmi_id()
    assert cdmi_id_1 != cdmi_id_2


def test_datetime_serializer():
    # a datetime is serialized to a string
    assert isinstance(datetime_serializer(datetime.today()), str)
    # serializing something else returns None
    assert datetime_serializer("2020-04-29 20:18:21") == None
    # serialize/unserialize should be identity
    now = datetime.now()
    assert datetime_unserializer(datetime_serializer(now)) == now


def test_decode_encode_meta():
    values = [
        "test",
        ["t", "e", "s", "t"],
        12,
        {"a":"val"}
        ]
    
    for val in values:
        v = decode_meta(encode_meta(val))
        assert decode_meta(encode_meta(val)) == val
    # Specific decoding for datetime
    val = datetime.today()
    val_v = encode_meta(val)
    assert decode_datetime(val_v) == val
    # Test json decode error
    assert(decode_meta("test")) == "test"


def test_default_date():
    today = date.today()
    assert default_date() == today.strftime("%Y%m%d")


def test_default_time():
    assert isinstance(default_time(), uuid.UUID)


def test_default_uuid():
    uuid1 = default_uuid()
    uuid2 = default_uuid()
    assert uuid1 != uuid2


def test_encrypt_password():
    pwd_plain = "password"
    pwd_crypted = encrypt_password(pwd_plain)
    assert pwd_plain != pwd_crypted


def test_guess_mimetype():
    import mimetypes
    fps = [
        ("test.ar", "application/octet-stream"),
        ("test.cpio", "application/x-cpio"),
        ("test.iso", "application/x-iso9660-image"),
        ("test.tar", "application/x-tar"),
        ("test.bz2", "application/x-bzip2"),
        ("test.gz", "application/gzip"),
        ("test.tar.gz", "application/x-gtar"),
        ("test.tar.bz2", "application/x-gtar"),
        ("test.tgz", "application/x-gtar"),
        ("test.zip", "application/zip"),
    ]
    
    for fp, valid in fps:
        assert guess_mimetype(fp) == valid


def test_is_collection():
    assert is_collection("/")
    assert is_collection("/coll1/")
    assert not is_collection("/coll1")
    assert not is_collection("/undefined_coll")


def test_is_reference():
    assert not is_reference(cfg.protocol_cassandra +"0ADASEDSDECDEEF")
    assert is_reference(TEST_URL)
    assert not is_reference(None)


def test_is_resource():
    #assert is_resource("/test.url")
    #assert is_reference("/test.url")
    assert is_resource("/coll1/test.txt")
    #assert not is_resource("/undefined_coll/test.txt")


def test_last_x_days():
    today = date.today()
    l = last_x_days(2)
    assert l[0] == today.strftime("%Y%m%d")
    assert len(l) == 2
    l = last_x_days(5)
    assert l[0] == today.strftime("%Y%m%d")
    assert len(l) == 5


def test_merge():
    assert merge("", "a") == "/a"
    assert merge("/A", "a") == "/A/a"
    assert merge("/", "a") == "/a"
    assert merge("/", "") == "/"
    assert merge("/A/", "a") == "/A/a"
    assert merge("/A/", "B/") == "/A/B/"


def test_meta_cdmi_to_cassandra():
    metas= [
        {"test": "val"},
        {"test": ["val1", "val2"]},
        ]
    
    for meta_cdmi in metas:
        meta_cass = meta_cdmi_to_cassandra(meta_cdmi)
        assert meta_cassandra_to_cdmi(meta_cass) == meta_cdmi

    assert meta_cdmi_to_cassandra({"test" : None}) == {}


def test_metadata_to_list():
    meta = {"test": encode_meta("val")}
    assert metadata_to_list(meta) == [("test", "val")]
    meta = {"test": encode_meta(["val1", "val2"])}
    assert metadata_to_list(meta) == [("test", ["val1", "val2"])]
    meta = {cfg.meta_create_ts: encode_meta("Today")}
    assert metadata_to_list(meta, cfg.vocab_dict) == [(cfg.vocab_dict[cfg.meta_create_ts], "Today")]
    now_date = now()
    now_str = now_date.strftime("%A %d %B %Y - %H:%M:%S (%z)")
    meta = {cfg.meta_modify_ts: encode_meta(now_date)}
    assert metadata_to_list(meta, cfg.vocab_dict) == [(cfg.vocab_dict[cfg.meta_modify_ts], now_str)]


def test_path_exists():
    assert path_exists("/")
    assert path_exists("/coll1/")
    assert not path_exists("/undefined_coll/")


def test_random_password():
    assert random_password() != random_password()
    assert random_password(5) != random_password(5)
    assert len(random_password(15)) == 15


def test_split():
    assert split("/collection/resource.txt") == ("/collection/", "resource.txt")
    assert split("/collection/rÃ©source.txt") == ("/collection/", "rÃ©source.txt")
    assert split("/resource/") == ("/", "resource/")
    assert split("resource/") == ("/", "resource/")
    assert split("/resource.txt") == ("/", "resource.txt")
    assert split("/") ==  ('/','.')
    assert split("/a") == ('/','a')
    assert split("/a/") == ('/','a/')
    assert split("/a/b") == ('/a/','b')
    assert split("/a/b/") == ('/a/','b/')
    assert split("/a/b/c") == ('/a/b/','c')
    assert split("/a/b/c/") == ('/a/b/','c/')



def test_verify_ldap_password(mocker):
    pw = "password"
    cfg.auth_ldap_server_uri = None
    assert verify_ldap_password("username", pw) == False
    
    cfg.auth_ldap_server_uri = "ldap://ldap.example.com"
    cfg.auth_ldap_user_dn_template = None
    assert verify_ldap_password("username", pw) == False
    
    cfg.auth_ldap_user_dn_template = "uid=%(user)s,ou=users,dc=example,dc=com"

    ## Exception ldap.SERVER_DOWN
    assert verify_ldap_password("username", pw) == False
    
    ## Exception ldap.INVALID_CREDENTIALS
    mocker.patch('ldap.ldapobject.SimpleLDAPObject.simple_bind_s',
                 return_value=False,
                 side_effect=ldap.INVALID_CREDENTIALS)
    assert verify_ldap_password("username", pw) == False
    
    # Connection OK
    mocker.patch('ldap.ldapobject.SimpleLDAPObject.simple_bind_s', return_value=True)
    assert verify_ldap_password("username", pw) == True


def test_verify_password():
    pw1 = "password"
    pw2 = encrypt_password(pw1)
    assert verify_password(pw1, pw2) == True
    assert verify_password("wrong_password", pw2) == False




