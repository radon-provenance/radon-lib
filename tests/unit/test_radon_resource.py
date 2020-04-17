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

from radon.models.resource import(
    is_reference
)



def test_is_reference():
    assert is_reference("cassandra://0000A4EF001849A5BB37DC79AB07483296F36FC34141950D") == False
    assert is_reference("http://radon.radon.org/Test") == True