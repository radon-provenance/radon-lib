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



class BaseError(Exception):
    """Base Exception."""

    pass


class ModelError(BaseError):
    """Base Class for storage Exceptions

    Abstract Base Class from which more specific Exceptions are derived.
    
    :param obj_str: A string that will be displayed in the error message to
      indicate which object is failing
    :type obj_str: str
    """

    def __init__(self, obj_str):
        self.obj_str = obj_str


class ResourceConflictError(ModelError):
    """Resource already exists Exception"""

    def __str__(self):
        return "Resource already exists at '{}'".format(self.obj_str)


class NoSuchResourceError(ModelError):
    """No such data object Exception"""

    def __str__(self):
        return "Resource '{}' does not exist".format(self.obj_str)


class CollectionConflictError(ModelError):
    """Container already exists Exception"""

    def __str__(self):
        return "Container already exists at '{}'".format(self.obj_str)


class NoSuchCollectionError(ModelError):
    """No such container Exception """

    def __str__(self):
        return "Container '{}' does not exist".format(self.obj_str)


class GroupConflictError(ModelError):
    """Group already exists Exception"""

    def __str__(self):
        return "Group '{}' already exists".format(self.obj_str)


class UserConflictError(ModelError):
    """Username already used"""

    def __str__(self):
        return "Username '{}' already in use".format(self.obj_str)




