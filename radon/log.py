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

import logging



class LogFormatter(logging.Formatter):
    """Uses the ISO8601 date format, with the optional 'T' character, and a 
    '.' as the decimal separator."""
    default_format = '%(name)-12s %(asctime)s.%(msecs)03dZ %(levelname)-8s%(message)s'
    debug_format = '%(name)-12s %(asctime)s.%(msecs)03dZ %(levelname)-8s' \
                   '[%(pathname)s:%(funcName)s:%(lineno)s] %(message)s'

    def __init__(self, fmt="%(levelno)s: %(msg)s"):
        logging.Formatter.__init__(self, fmt)

    def format(self, record):
        orig_fmt = self._fmt
        self._fmt = self.default_format
        self.datefmt = '%Y-%m-%dT%H:%M:%S'

        if record.levelno == logging.DEBUG:
            self._fmt = self.debug_format

        result = logging.Formatter.format(self, record)

        self._fmt = orig_fmt

        return result


def init_log(name):
    """Initialise logging"""
    logging.basicConfig(level=logging.INFO)

    for handler in logging.root.handlers:
        handler.setFormatter(LogFormatter())

    return logging.getLogger(name)
