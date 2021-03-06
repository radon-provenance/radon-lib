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

from radon.models import (
    Collection,
    Resource,
)


def is_collection(path):
    """Check if the collection exists"""
    return Collection.find(path) is not None


def is_resource(path):
    """Check if the resource exists"""
    return Resource.find(path) is not None


def path_exists(path):
    """Check if the path is already in use"""
    return is_resource(path) or is_collection(path)
