# Radon Copyright 2021, University of Oxford
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


import logging


def init_logger(name, cfg):
    """Initialise logging
    
    :param name: Name of the logger
    :type name: str
    :param cfg: The global Config object
    :type cfg: :class:`radon.model.config.LocalConfig`

    :return: the logger
    :rtype: :class:`logging.Logger`
    """
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()

    default_fmt = logging.Formatter(
        "%(name)-10s %(asctime)s %(levelname)-9s%(message)s"
    )

    debug_fmt = logging.Formatter(
        "%(name)-10s %(asctime)s %(levelname)-9s"
        "[%(pathname)s:%(funcName)s:%(lineno)s] %(message)s"
    )
    if cfg.debug:
        logger.setLevel(logging.DEBUG)
        handler.setFormatter(debug_fmt)
    else:
        logger.setLevel(logging.WARNING)
        handler.setFormatter(default_fmt)

    logger.addHandler(handler)

    return logger




