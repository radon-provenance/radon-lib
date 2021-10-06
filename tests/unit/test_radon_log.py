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

import pytest
import logging

from radon import cfg
from radon.log import init_logger


def test_debug_logger(caplog):
    cfg.debug = True
    logger = init_logger("test_debug", cfg)

    with caplog.at_level(logging.DEBUG):
        logger.debug("debug")
        logger.critical("error")
    assert "debug" in caplog.text
    assert "error" in caplog.text


def test_default_logger(caplog):
    cfg.debug = False
    logger = init_logger("test_default", cfg)

    with caplog.at_level(logging.DEBUG):
        logger.debug("debug")
        logger.critical("error")
    assert "debug" not in caplog.text
    assert "error" in caplog.text



