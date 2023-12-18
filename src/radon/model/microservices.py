# Copyright 2023
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


from radon.model.payload import (
    PayloadCreateRequestUser,
    PayloadCreateFailUser
)
from radon.model.notification import (
    create_fail_user
)
from radon.model.user import User
from radon.util import (
    payload_check,
)

ERR_PAYLOAD_CLASS = "Wrong payload class"

class Microservices(object):


    @classmethod
    def create_user(cls, payload):
        if not isinstance(payload, PayloadCreateRequestUser):
            return (None, ERR_PAYLOAD_CLASS)
        (is_valid, msg) = payload.validate()
        if not is_valid:
            payload = PayloadCreateFailUser.default(
                payload.get_object_key(),
                msg,
                payload.get_sender())
            create_fail_user(payload)
            return (None, msg)
        obj = payload_check("/obj", payload.get_json())
        if not obj:
            return (None, "Object definition not defined in payload")
        user = User.create(**obj)
        print(user)
        return (user, "User created")


