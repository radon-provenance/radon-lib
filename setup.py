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

from setuptools import find_packages, setup

import radon


setup(
    name="radon",
    version=radon.__version__,
    description="Radon core library",
    extras_require={},
    long_description="Radon library for Radon development",
    author="Jerome Fuselier",
    maintainer_email="",
    license="Apache License, Version 2.0",
    url="",
    packages=find_packages(),
    entry_points={"console_scripts": ["radmin = radon.cli:main"],},
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Environment :: Console",
        "Operating System :: POSIX :: Linux",
    ],
)
