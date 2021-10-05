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


from setuptools import find_packages, setup
from glob import glob
import os
from os.path import (
    basename,
    splitext
)
import codecs
import re

here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name="radon",
    version=find_version("src/radon", "__init__.py"),
    description="Radon core library",
    extras_require={},
    long_description="Radon library for Radon development",
    author="Jerome Fuselier",
    maintainer_email="",
    license="Apache License, Version 2.0",
    url="https://github.com/radon-provenance/radon-lib",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    entry_points={"console_scripts": ["radmin = radon.cli:main"],},
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Environment :: Console",
        "Operating System :: POSIX :: Linux",
    ],
)
