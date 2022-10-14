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


import pytest
import os
import shutil
import tempfile

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


def test_cli():
    """Does CLI stop execution w/o a command argument?"""
    with pytest.raises(SystemExit):
        radon.cli.main()
        pytest.fail("CLI doesn't abort asking for a command argument")


@patch('radon.cli.RadonApplication.init')
def test_cli_command_init(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'init'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.drop')
def test_cli_command_drop(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'drop'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.ls')
def test_cli_command_ls(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'ls'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.change_dir')
def test_cli_command_change_dir(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'cd'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mkdir')
def test_cli_command_mkdir(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'mkdir', '/'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.get')
def test_cli_command_get(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'get', '/'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.put')
def test_cli_command_put(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'put', '/'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.pwd')
def test_cli_command_pwd(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'pwd'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.rm')
def test_cli_command_rm(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'rm', '/'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.list_users')
def test_cli_command_list_users(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'lu'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.list_groups')
def test_cli_command_list_groups(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'lg'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mk_user')
def test_cli_command_mkuser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'mkuser'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mk_ldap_user')
def test_cli_command_mk_ldapuser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'mkldapuser'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mod_user')
def test_cli_command_moduser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'moduser', 'user1', 'email'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.rm_user')
def test_cli_command_rmuser(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'rmuser'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.mk_group')
def test_cli_command_mk_group(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'mkgroup', 'grp4'):
        radon.cli.main()
    assert mock_command.called


@patch('radon.cli.RadonApplication.add_to_group')
def test_cli_command_add_to_group(mock_command):
    """Is the correct code called when invoked via the CLI?"""
    with ArgvContext('radmin', 'atg', 'grp1', 'test1'):
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


def test_commands(mocker):
    # Remove the existing user folder for the session
    if os.path.exists("~/.radon"):
        shutil.rmtree(os.path.expanduser("~/.radon"))
    
    
    #####################
    ## Create Database ##
    #####################
    
    radon.cfg.dse_keyspace = "temp_radon"
    
    with ArgvContext('radmin', 'init'):
        radon.cli.main()
    
    
    
    ###########
    ## Users ##
    ###########
    
    # Mock input methods to automate the test (values will be incorrect so we
    # modify users afterward
    mocker.patch('radon.cli.input', return_value="test")
    mocker.patch('radon.cli.getpass', return_value="test")
    
    
    ## Create 1st user, admin user
    with ArgvContext('radmin', 'mkuser', 'user1'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user1', 'email', 'user1@radon.com'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user1', 'administrator', 'y'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user1', 'active', 'y'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user1', 'password', 'test'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user1', 'ldap', 'n'):
        radon.cli.main()
    
    ## Create 2nd user, non admin user
    with ArgvContext('radmin', 'mkuser', 'user2'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user2', 'email', 'user2@radon.com'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user2', 'administrator', 'n'):
        radon.cli.main()
    # Test to deactivate/activate user
    with ArgvContext('radmin', 'moduser', 'user2', 'active', 'n'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user2', 'active', 'y'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user2', 'password', 'test'):
        radon.cli.main()
    # Switch to a ldap user
    with ArgvContext('radmin', 'moduser', 'user2', 'ldap', 'y'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user2', 'ldap', 'n'):
        radon.cli.main()
    
    ## User already exists 
    mocker.patch('radon.cli.input', return_value="user1")
    with ArgvContext('radmin', 'mkuser'):
        radon.cli.main()
    
    ## Create 3rd user, ldap user
    mocker.patch('radon.cli.input', return_value="user3")
    with ArgvContext('radmin', 'mkldapuser'):
        radon.cli.main()
    
    ## Ldap User already exists 
    with ArgvContext('radmin', 'mkldapuser', 'user3'):
        radon.cli.main()
    
    ## Test moduser
    
    # User doesn't exist
    with ArgvContext('radmin', 'moduser', 'user4', 'email', 'test4@radon.com'):
        radon.cli.main()
    # Arg not passed in parameter, mock input or get_pass
    with ArgvContext('radmin', 'moduser', 'user2', 'email'):
        radon.cli.main()
    with ArgvContext('radmin', 'moduser', 'user2', 'password'):
        radon.cli.main()
    
    # List users
    with ArgvContext('radmin', 'lu'):
        radon.cli.main()
    # List specific user
    with ArgvContext('radmin', 'lu', 'user1'):
        radon.cli.main()
    # List specific LDAP user
    with ArgvContext('radmin', 'lu', 'user3'):
        radon.cli.main()
    # List unknown user
    with ArgvContext('radmin', 'lu', 'unk_usr'):
        radon.cli.main()
    
    
    # ############
    # ## Groups ##
    # ############
    #
    # ## Create 1st group
    # with ArgvContext('radmin', 'mkgroup', 'grp1'):
    #     radon.cli.main()
    #
    # ## Group already exists
    # with ArgvContext('radmin', 'mkgroup', 'grp1'):
    #     radon.cli.main()
    #
    # mocker.patch('radon.cli.input', return_value="grp2")
    # ## Create 2nd group
    # with ArgvContext('radmin', 'mkgroup'):
    #     radon.cli.main()
    #
    # ## Add to group that doesn't exist
    # with ArgvContext('radmin', 'atg', 'grp3', 'user1'):
    #     radon.cli.main()
    # # Add user
    # with ArgvContext('radmin', 'atg', 'grp1', 'user1', 'user4'):
    #     radon.cli.main()
    # with ArgvContext('radmin', 'atg', 'grp1', 'user1', 'user2', 'user4', 'user5'):
    #     radon.cli.main()
    # with ArgvContext('radmin', 'atg', 'grp1', 'user1', 'user2'):
    #     radon.cli.main()
    #
    # # List groups
    # with ArgvContext('radmin', 'lg'):
    #     radon.cli.main()
    # # List specific group
    # with ArgvContext('radmin', 'lg', 'grp1'):
    #     radon.cli.main()
    # # List unknown group
    # with ArgvContext('radmin', 'lg', 'unk_grp'):
    #     radon.cli.main()
    #
    #
    # #################
    # ## Collections ##
    # #################
    #
    #
    # # Test mkdir
    # with ArgvContext('radmin', 'mkdir', 'test'):
    #     radon.cli.main()
    # # Collection already exists
    # with ArgvContext('radmin', 'mkdir', 'test'):
    #     radon.cli.main()
    # # not allowed name
    # with ArgvContext('radmin', 'mkdir', 'cdmi_test'):
    #     radon.cli.main()
    # # subcollection doesn't exist
    # with ArgvContext('radmin', 'mkdir', 'test1/test'):
    #     radon.cli.main()
    # with ArgvContext('radmin', 'mkdir', 'test/test'):
    #     radon.cli.main()
    #
    #
    # ################
    # ## Change dir ##
    # ################
    #
    # with ArgvContext('radmin', 'cd', 'test'):
    #     radon.cli.main()
    # with ArgvContext('radmin', 'cd'):
    #     radon.cli.main()
    # with ArgvContext('radmin', 'cd', 'test1'):
    #     radon.cli.main()
    #
    #
    # #########
    # ## Put ##
    # #########
    #
    # tmpfilepath = os.path.join(tempfile.gettempdir(), "testfile")
    # with open(tmpfilepath, "w") as f:
    #     f.write("Delete the file after the test")
    #
    # # Put a file without specifying the name
    # with ArgvContext('radmin', 'put', tmpfilepath):
    #     radon.cli.main()
    # # Put a file on an existing one
    # with ArgvContext('radmin', 'put', tmpfilepath):
    #     radon.cli.main()
    # # Put a file with a name
    # with ArgvContext('radmin', 'put', tmpfilepath, 'test.txt'):
    #     radon.cli.main()
    # # Try to put an unknown file
    # with ArgvContext('radmin', 'put', "unknown_file.unk", 'test.txt'):
    #     radon.cli.main()
    # # Try to put in an unknown collection
    # with ArgvContext('radmin', 'put', tmpfilepath, "/unk_test/"):
    #     radon.cli.main()
    # # Put a file in a collection
    # # Put a file without specifying the name
    # with ArgvContext('radmin', 'put', tmpfilepath, 'test/'):
    #     radon.cli.main()
    # # Put a reference
    # with ArgvContext('radmin', 'put', "--ref", "http://www.google.fr", 'test_ref.txt'):
    #     radon.cli.main()
    #
    #
    # #########
    # ## Get ##
    # #########
    #
    # tmpfilepath2 = os.path.join(tempfile.gettempdir(), "testfileget")
    # tmpfilepath3 = os.path.join(tempfile.gettempdir(), "testfileget2.fifo")
    # os.mkfifo(tmpfilepath3)
    # tmpdir = os.path.join(tempfile.gettempdir(), "testgetdir/")
    # tmpfilepath4 = os.path.join(tmpdir, "test.txt")
    #
    #
    # # Get a file without specifying the destination file
    # with ArgvContext('radmin', 'get', 'test.txt'):
    #     radon.cli.main()
    # # Get a file with specifying the destination file
    # with ArgvContext('radmin', 'get', "test.txt", tmpfilepath2):
    #     radon.cli.main()
    # # Get a file with an existing destination file without --force
    # with ArgvContext('radmin', 'get', "test.txt", tmpfilepath2):
    #     radon.cli.main()
    # # Get a file with an existing destination file without --force
    # with ArgvContext('radmin', 'get', "test.txt", tmpfilepath2, "--force"):
    #     radon.cli.main()
    # # Get a file with a directory as the destination file
    # with ArgvContext('radmin', 'get', "test.txt", "/tmp"):
    #     radon.cli.main()
    # # Get a file ant put it on an exsting object which is not a file nor a 
    # # directory
    # with ArgvContext('radmin', 'get', "test.txt", tmpfilepath3):
    #     radon.cli.main()
    # # Resource doesn't exist
    # with ArgvContext('radmin', 'get', "unk.txt"):
    #     radon.cli.main()
    # # Get in a directory
    # with ArgvContext('radmin', 'get', "test.txt", tmpfilepath4):
    #     radon.cli.main()
    #
    # os.remove("test.txt")
    #
    #
    # #########
    # ## Ls ##
    # #########
    #
    # # Ls without parameter
    # with ArgvContext('radmin', "ls"):
    #     radon.cli.main()
    # # Ls with full path
    # with ArgvContext('radmin', "ls", "/test/"):
    #     radon.cli.main()
    # # Ls with non existing path
    # with ArgvContext('radmin', "ls", "/unknown/"):
    #     radon.cli.main()
    # # Ls with relative path
    # with ArgvContext('radmin', "ls", "test/"):
    #     radon.cli.main()
    # # Ls with version
    # with ArgvContext('radmin', "ls", "--v", "0"):
    #     radon.cli.main()
    # # Ls with acl
    # with ArgvContext('radmin', "ls", "-a"):
    #     radon.cli.main()
    # # Ls with full path
    # with ArgvContext('radmin', "ls", "-a", "/test/"):
    #     radon.cli.main()
    #
    #
    # ##################
    # ## Remove files ##
    # ##################
    #
    # # Remove resource
    # with ArgvContext('radmin', "rm", "/test.txt"):
    #     radon.cli.main()
    # # Remove collection
    # with ArgvContext('radmin', "rm", "/test/"):
    #     radon.cli.main()
    # # Remove unknown object
    # with ArgvContext('radmin', "rm", "/unk_coll/"):
    #     radon.cli.main()
    #
    # # Remove local files
    # os.remove(tmpfilepath)
    # os.remove(tmpfilepath2)
    # os.remove(tmpfilepath3)
    # shutil.rmtree(os.path.join(tempfile.gettempdir(), "testgetdir"))
    #
    #
    # #######################
    # ## Remove from group ##
    # #######################
    #
    # ## Remove from group that doesn't exist
    # with ArgvContext('radmin', 'rfg', 'grp3', 'user1'):
    #     radon.cli.main()
    # # Remove users
    # with ArgvContext('radmin', 'rfg', 'grp1', 'user1', 'user4'):
    #     radon.cli.main()
    # with ArgvContext('radmin', 'rfg', 'grp1', 'user1', 'user2', 'user4', 'user5'):
    #     radon.cli.main()
    # with ArgvContext('radmin', 'rfg', 'grp1', 'user1', 'user2'):
    #     radon.cli.main()
    #
    # ###################
    # ## Remove groups ##
    # ###################
    #
    # # Name in the parameter
    # with ArgvContext('radmin', 'rmgroup', 'grp1'):
    #     radon.cli.main()
    # # Name with input
    # mocker.patch('radon.cli.input', return_value="grp2")
    # with ArgvContext('radmin', 'rmgroup'):
    #     radon.cli.main()
    # # Group doesn't exist
    # with ArgvContext('radmin', 'rmgroup', 'grp3'):
    #     radon.cli.main()
    #
    #
    # ##################
    # ## Remove users ##
    # ##################
    #
    # # Name in the parameter
    # with ArgvContext('radmin', 'rmuser', 'user1'):
    #     radon.cli.main()
    # # Name with input
    # mocker.patch('radon.cli.input', return_value="user2")
    # with ArgvContext('radmin', 'rmuser'):
    #     radon.cli.main()
    # # User doesn't exist
    # with ArgvContext('radmin', 'rmuser', 'user4'):
    #     radon.cli.main()
    #
    # #################
    # ## Session Mgt ##
    # #################
    #
    # # Test get_session when there's a loading problem
    # mocker.patch('pickle.load', side_effect=IOError)
    # with ArgvContext('radmin', 'pwd'):
    #     radon.cli.main()
    #



    ####################
    ## Clean Database ##
    ####################
    
    # Simulate an input of a 'y' to drop the keyspace automatically
    mocker.patch('radon.cli.input', return_value="y")
    
    with ArgvContext('radmin', 'drop'):
        radon.cli.main()

