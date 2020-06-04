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


import pytest
import os

from cli_test_helpers import ArgvContext, EnvironContext
from unittest.mock import patch
  
import radon.cli


def test_runas_module():
    """Can this package be run as a Python module"""
    exit_status = os.system('python -m radon.cli --help')
    assert exit_status == 0


def test_entrypoint():
    """Is entrypoint script installed? (setup.py)"""
    exit_status = os.system('radmin --help')
    assert exit_status == 0


def test_create_command():
    """Is command available?"""
    exit_status = os.system('radmin create --help')
    assert exit_status == 0


def test_cli():
    """Does CLI stop execution w/o a command argument?"""
    with pytest.raises(SystemExit):
        radon.cli.main()
        pytest.fail("CLI doesn't abort asking for a command argument")


@patch('radon.cli.RadonApplication.add_to_group')
def test_cli_command_add_to_group(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'atg', 'grp1', 'test1'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.create')
def test_cli_command_create(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'create'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.list_groups')
def test_cli_command_list_groups(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'lg'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.list_users')
def test_cli_command_list_users(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'lu'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mk_group')
def test_cli_command_mk_group(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'mkgroup', 'grp4'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mk_ldap_user')
def test_cli_command_mk_ldapuser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'mkldapuser'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mk_user')
def test_cli_command_mkuser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'mkuser'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mod_user')
def test_cli_command_moduser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'moduser', 'user1', 'email'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.rm_from_group')
def test_cli_command_rfg(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'rfg', 'grp1', 'user1'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.rm_group')
def test_cli_command_rmgroup(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'rmgroup', 'grp1'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.rm_user')
def test_cli_command_rmuser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'rmuser'):
        radon.cli.main()
    assert mock_command.called




