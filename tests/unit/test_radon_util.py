"""Copyright 2020 - 

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

from radon.util import(
    datetime_serializer,
    decode_meta,
    default_cdmi_id,
    default_date,
    default_time,
    default_uuid,
    encrypt_password,
    last_x_days,
    merge,
    meta_cassandra_to_cdmi,
    meta_cdmi_to_cassandra,
    metadata_to_list,
    split,
    verify_password
)


def test_default_cdmi_id():
    cdmi_id_1 = default_cdmi_id()
    cdmi_id_2 = default_cdmi_id()
    assert cdmi_id_1 != cdmi_id_2


def test_datetime_serializer():
    assert isinstance(datetime_serializer(datetime.today()), str)
    assert datetime_serializer("2020-04-29 20:18:21") == None


def test_decode_meta():
    assert decode_meta('{"json": "test"}') == "test"
    assert decode_meta('{"json": ["t", "e", "s", "t"]}') == ["t", "e", "s", "t"]
    assert decode_meta('{"json": 12}') == 12
    assert decode_meta('{"wrong": 12}') == ''
    # ValueError for json.loads -> return the value parameter
    assert decode_meta('{"json": {{}') == '{"json": {{}'


def test_default_date():
    today = date.today()
    assert default_date() == today.strftime("%y%m%d")


def test_default_time():
    assert isinstance(default_time(), uuid.UUID)


def test_default_uuid():
    assert default_uuid() != default_uuid()


def test_encrypt_password():
    pwd_plain = "password"
    pwd_crypted = encrypt_password(pwd_plain)
    assert pwd_plain != pwd_crypted


def test_last_x_days():
    today = date.today()
    l = last_x_days(2)
    assert l[-1] == today.strftime("%y%m%d")
    assert len(l) == 2
    l = last_x_days(5)
    assert l[-1] == today.strftime("%y%m%d")
    assert len(l) == 5


def test_merge():
    assert merge("", "test") == "/test"
    assert merge("/test", "test") == "/test/test"
    assert merge("/", "test") == "/test"
    assert merge("/", "") == "/"
    assert merge("/test/", "test") == "/test/test"


def test_meta_cassandra_to_cdmi():
    meta = {"meta": "val"}
    assert meta_cassandra_to_cdmi(meta) == {"meta": "val"}
    meta = {"meta": '{"json": "val"}'}
    assert meta_cassandra_to_cdmi(meta) == {"meta": "val"}
    meta = {"meta": '{"json": ["val1", "val2"]}'}
    assert meta_cassandra_to_cdmi(meta) == {"meta": ["val1", "val2"]}


def test_meta_cdmi_to_cassandra():
    meta = {"test": "val"}
    assert meta_cdmi_to_cassandra(meta) == {'test': '{"json": "val"}'}
    meta = {"test": ["val1", "val2"]}
    assert meta_cdmi_to_cassandra(meta) == {'test': '{"json": ["val1", "val2"]}'}
    meta = {"test": ""}
    assert meta_cdmi_to_cassandra(meta) == {}


def test_metadata_to_list():
    meta = {"test": "val"}
    assert metadata_to_list(meta) == [("test", "val")]
    meta = {"test": '{"json": "val1"}'}
    assert metadata_to_list(meta) == [("test", "val1")]
    meta = {"test": '{"json": ["val1", "val2"]}'}
    assert metadata_to_list(meta) == [("test", "val1"), ("test", "val2")]


def test_split():
    assert split("/collection/resource.txt") == ("/collection", "resource.txt")
    assert split("/collection/résource.txt") == ("/collection", "résource.txt")
    assert split("/resource") == ("/", "resource")
    assert split("resource.txt") == ("", "resource.txt")


def test_verify_password():
    pw1 = "password"
    pw2 = encrypt_password(pw1)
    assert verify_password(pw1, pw2) == True
    assert verify_password("wrong_password", pw2) == False


