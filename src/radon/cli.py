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

__doc_opt__ = """
Radon Admin Command Line Interface.

Usage:
  radmin init
  radmin drop
  radmin ls [<path>] [-a] [--v=<VERSION>]
  radmin cd [<path>]
  radmin mkdir <path>
  radmin get <src> [<dest>] [--force]
  radmin put <src> [<dest>] [--mimetype=<MIME>]
  radmin put --ref <url> <dest> [--mimetype=<MIME>]
  radmin pwd
  radmin rm <path>
  radmin lu [<name>]
  radmin lg [<name>]
  radmin mkuser [<name>]
  radmin mkldapuser [<name>]
  radmin moduser <name> (email | administrator | active | password | ldap) [<value>]
  radmin rmuser [<name>]
  radmin mkgroup [<name>]
  radmin atg <name> <userlist> ...
  radmin rfg <name> <userlist> ...
  radmin rmgroup [<name>]


Options:
  -h --help      Show this screen.
  --version      Show version.
"""

import errno
from getpass import getpass
import logging
import blessings
from operator import methodcaller
import docopt
import os
import pickle
 
import radon
from radon.database import (
    create_default_users,
    create_root,
    create_tables,
    destroy,
    initialise
)
from radon.model import (
    Collection,
    Group,
    Resource,
    User
)
from radon.model.errors import (
    NoSuchCollectionError
)
from radon.util import (
    guess_mimetype,
    random_password,
    split
)


SESSION_PATH = os.path.join(os.path.expanduser("~/.radon"), "session.pickle")

ARG_NAME = "<name>"
ARG_PATH = "<path>"
ARG_USERLIST = "<userlist>"
 
MSG_GROUP_EXIST = "Group {} already exists"
MSG_GROUP_NOT_EXIST = "Group {} doesn't exist"
MSG_GROUP_CREATED = "Group {} has been created"
MSG_GROUP_DELETED = "Group {} has been deleted"
MSG_ADD_USER = "Added {} to the group {}"
MSG_USER_IN_GROUP = "{} {} already in the group {}"
MSG_USER_NOT_EXIST = "User {} doesn't exist"
MSG_USERS_NOT_EXIST = "Users {} don't exist"
MSG_USER_CREATED = "User {} has been created"
MSG_USER_MODIFIED = "User {} has been modified"
MSG_USER_DELETED = "User {} has been deleted"
MSG_PROMPT_USER = "Please enter the username: "
MSG_PROMPT_GROUP = "Please enter the group name: "
MSG_COLL_EXIST = "Collection {} already exists"
MSG_COLL_NOT_EXIST = "Collection {} doesn't exist"
MSG_COLL_WRONG_NAME = "cdmi_ prefix is not a valid prefix for the name of a container"
MSG_RESC_EXIST = "Resource {} already exists"
MSG_RESC_NOT_EXIST = "Resource {} doesn't exists"
MSG_NO_OBJECT = "No object found at path {}"


class RadonApplication():
    """Methods for the CLI"""

    def __init__(self, session_path):
        self.terminal = blessings.Terminal()
        self.session_path = session_path
        initialise()

    def add_to_group(self, args):
        """Add user(s) to a group."""
        groupname = args[ARG_NAME]
        ls_users = args[ARG_USERLIST]
        group = Group.find(groupname)
        if not group:
            self.print_error(MSG_GROUP_NOT_EXIST.format(groupname))
            return
        added, not_added, already_there = group.add_users(ls_users)
 
        if added:
            self.print_success(
                MSG_ADD_USER.format(", ".join(added), group.name)
            )
        if already_there:
            if len(already_there) == 1:
                verb = "is"
            else:
                verb = "are"
            self.print_error(
                MSG_USER_IN_GROUP.format(
                    ", ".join(already_there), verb, group.name
                )
            )
        if not_added:
            if len(not_added) == 1:
                msg = MSG_USER_NOT_EXIST
            else:
                msg = MSG_USERS_NOT_EXIST
            self.print_error(msg.format(", ".join(not_added)))

    def init(self):
        """Create the tables"""
        create_tables()
        create_root()
        create_default_users()

        session = self.create_session()
        self.save_session(session)

    def drop(self):
        """Remove the keyspace"""
        print("*********************************")
        print("**           WARNING           **")
        print("*********************************")
        print("This will remove every data stored in the database.")
        confirm = input("Are you sure you want to continue ? [y/N] ")
        
        if confirm.lower() in ["true", "y", "yes"]:
            destroy()
            session = self.create_session()
            self.save_session(session)


    def change_dir(self, args):
        "Move into a different container."
        session = self.get_session()
        cwd = session.get('cwd', '/')

        if args[ARG_PATH]:
            path = args[ARG_PATH]
        else:
            path = "/"

        if not path.startswith("/"):
            # relative path
            path = "{}{}".format(cwd, path)

        if not path.endswith("/"):
            path = path + '/'

        col = Collection.find(path)
        if not col:
            self.print_error(MSG_COLL_NOT_EXIST.format(path))
            return
        
        session['cwd'] = path
        # Save the client for persistent use
        self.save_session(session)
        return 0
 
    def create_session(self):
        """Return a new session"""
        # The session is a dictionary that stores the current status
        return {"cwd" : "/"}


    def get(self, args):
        "Fetch a data object from the archive to a local file."
        src = args["<src>"]
        # Determine local filename
        if args["<dest>"]:
            localpath = args["<dest>"]
        else:
            localpath = src.rsplit("/")[-1]
        # Get the full destination path of the new resource
        src = self.get_full_path(src)
        
        # Check for overwrite of existing file, directory, link
        if os.path.isfile(localpath):
            if not args["--force"]:
                self.print_error(
                    "File '{0}' exists, --force option not used" "".format(localpath)
                )
                return errno.EEXIST
        elif os.path.isdir(localpath):
            self.print_error("'{0}' is a directory".format(localpath))
            return errno.EISDIR
        elif os.path.exists(localpath):
            self.print_error("'{0}'exists but not a file".format(localpath))
            return errno.EEXIST
        
        resc = Resource.find(src)
        if not resc:
            self.print_error(MSG_RESC_NOT_EXIST.format(src))
            return
        
        dest_folder = os.path.dirname(localpath)
        if dest_folder and not os.path.isdir(dest_folder):
            os.makedirs(dest_folder)
        lfh = open(localpath, "wb")
        for chunk in resc.chunk_content():
            lfh.write(chunk)
        lfh.close()
        return 0


    def get_full_path(self, path):
        """Return the full path in Radon"""
        session = self.get_session()
        cwd = session.get('cwd', '/')

        if not path.startswith("/"):
            # relative path
            path = "{}{}".format(cwd, path)
        return path
 
    def get_session(self):
        """Return the persistent session stored in the session_path file"""
        try:
            # Load existing session, so as to keep current dir etc.
            with open(self.session_path, "rb") as fhandle:
                session = pickle.load(fhandle)
        except (IOError, pickle.PickleError):
            # Create a new session
            session = self.create_session()
        return session
 
    def ls(self, args):
        """List a container."""
        session = self.get_session()
        cwd = session.get('cwd', '/')
        if args[ARG_PATH]:
            path = args[ARG_PATH]
            if not path.startswith("/"):
                # relative path
                path = "{}{}".format(cwd, path)
        else:
            # Get the current working dir from the session file
            path = cwd
        # --v option specify the version we want to display
        if args["--v"]:
            version = int(args["--v"])
            col = Collection.find(path, version)
        else:
            col = Collection.find(path)
        if not col:
            self.print_error(MSG_COLL_NOT_EXIST.format(path))
            return
        # Display name of the collection
        if path == "/":
            print("Root:")
        else:
            print("{}:".format(col.path))
        # Display Acl
        if args["-a"]:
            acl = col.get_acl_dict()
            if acl:
                for gid in acl:
                    print("  ACL - {}: {}".format(
                        gid, acl[gid]))
            else:
                print("  ACL: No ACE defined")
        # Display child
        c_colls, c_objs = col.get_child()
        for child in sorted(c_colls, key=methodcaller("lower")):
            print(self.terminal.blue(child))
        for child in sorted(c_objs, key=methodcaller("lower")):
            print(child)


    def list_groups(self, args):
        """List all groups or a specific group if the name is specified"""
        if args[ARG_NAME]:
            name = args[ARG_NAME]
            group = Group.find(name)
            if group:
                group_info = group.to_dict()
                members = ", ".join(group_info.get("members", []))
                print(
                    "{0.bold}Group name{0.normal}: {1}".format(
                        self.terminal, group_info.get("name", name)
                    )
                )
                print(
                    "{0.bold}Group id{0.normal}: {1}".format(
                        self.terminal, group_info.get("uuid", "")
                    )
                )
                print("{0.bold}Members{0.normal}: {1}".format(self.terminal, members))
            else:
                self.print_error(MSG_GROUP_NOT_EXIST.format(name))
        else:
            for group in Group.objects.all():
                print(group.name)


    def list_users(self, args):
        """List all users or a specific user if the name is specified"""
        if args[ARG_NAME]:
            name = args[ARG_NAME]
            user = User.find(name)
            if user:
                user_info = user.to_dict()
                groups = ", ".join([el["name"] for el in user_info.get("groups", [])])
                if not user_info.get("ldap"):
                    print(
                        "{0.bold}User name{0.normal}: {1}".format(
                            self.terminal, user_info.get("username", name)
                        )
                    )
                    print(
                        "{0.bold}Email{0.normal}: {1}".format(
                            self.terminal, user_info.get("email", "")
                        )
                    )
                    print(
                        "{0.bold}User id{0.normal}: {1}".format(
                            self.terminal, user_info.get("uuid", "")
                        )
                    )
                    print(
                        "{0.bold}Administrator{0.normal}: {1}".format(
                            self.terminal, user_info.get("administrator", False)
                        )
                    )
                    print(
                        "{0.bold}Active{0.normal}: {1}".format(
                            self.terminal, user_info.get("active", False)
                        )
                    )
                    print("{0.bold}Groups{0.normal}: {1}".format(self.terminal, groups))
                else:
                    print(
                        "{0.bold}User name (ldap){0.normal}: {1}".format(
                            self.terminal, user_info.get("username", name)
                        )
                    )
                    print(
                        "{0.bold}Administrator{0.normal}: {1}".format(
                            self.terminal, user_info.get("administrator", False)
                        )
                    )
                    print(
                        "{0.bold}Active{0.normal}: {1}".format(
                            self.terminal, user_info.get("active", False)
                        )
                    )
                    print("{0.bold}Groups{0.normal}: {1}".format(self.terminal, groups))
            else:
                self.print_error(MSG_USER_NOT_EXIST.format(name))
        else:
            for user in User.objects.all():
                print(user.name)


    def mkdir(self, args):
        "Create a new container."
        session = self.get_session()
        cwd = session.get('cwd', '/')
 
        path = args[ARG_PATH]
        # Collections names should end with a '/'
        if not path.endswith("/"):
            path += '/'
        
        if not path.startswith("/"):
            # relative path
            path = "{}{}".format(cwd, path)

        col = Collection.find(path)
        if col:
            self.print_error(MSG_COLL_EXIST.format(path))
            return

        parent, name = split(path)
        if name.startswith("cdmi_"):
            self.print_error(MSG_COLL_WRONG_NAME.format(name))
            return
        
        p_coll = Collection.find(parent)
        if not p_coll:
            self.print_error(MSG_COLL_NOT_EXIST.format(path))
            return
         
        Collection.create(name=name, container=parent)


    def mk_group(self, args):
        """Create a new group. Ask in the terminal for mandatory fields"""
        if not args[ARG_NAME]:
            name = input("Please enter the group name: ")
        else:
            name = args[ARG_NAME]
 
        group = Group.find(name)
        if group:
            self.print_error(MSG_GROUP_EXIST.format(name))
            return
        group = Group.create(name=name)
        print(MSG_GROUP_CREATED.format(group.name))
 
    def mk_ldap_user(self, args):
        """Create a new ldap user. Ask in the terminal for mandatory fields"""
        if not args[ARG_NAME]:
            name = input("Please enter the user's username: ")
        else:
            name = args[ARG_NAME]
        if User.find(name):
            self.print_error("Username {} already exists".format(name))
            return
        admin = input("Is this an administrator? [y/N] ")
        pwd = random_password(20)
        User.create(
            name=name,
            password=pwd,
            email="STORED_IN_LDAP",
            ldap=True,
            administrator=(admin.lower() in ["true", "y", "yes"]),
        )
        print(MSG_USER_CREATED.format(name))
 
    def mk_user(self, args):
        """Create a new user. Ask in the terminal for mandatory fields"""
        if not args[ARG_NAME]:
            name = input("Please enter the user's username: ")
        else:
            name = args[ARG_NAME]
        if User.find(name):
            self.print_error("Username {} already exists".format(name))
            return
        admin = input("Is this an administrator? [y/N] ")
        email = ""
        while not email:
            email = input("Please enter the user's email address: ")
        pwd = ""
        while not pwd:
            pwd = getpass("Please enter the user's password: ")
        User.create(
            name=name,
            password=pwd,
            email=email,
            ldap=False,
            administrator=(admin.lower() in ["true", "y", "yes"]),
        )
        print(MSG_USER_CREATED.format(name))

    def mod_user(self, args):
        """Modify a user. Ask in the terminal if the value isn't provided"""
        name = args[ARG_NAME]
        user = User.find(name)
        if not user:
            self.print_error("User {} doesn't exist".format(name))
            return
        value = args["<value>"]
        if not value:
            if args["password"]:
                while not value:
                    value = getpass("Please enter the new password: ")
            else:
                while not value:
                    value = input("Please enter the new value: ")
        if args["email"]:
            user.update(email=value)
        elif args["administrator"]:
            user.update(administrator=value.lower() in ["true", "y", "yes"])
        elif args["active"]:
            user.update(active=value.lower() in ["true", "y", "yes"])
        elif args["ldap"]:
            user.update(ldap=value.lower() in ["true", "y", "yes"])
        elif args["password"]:
            user.update(password=value)
        print(MSG_USER_MODIFIED.format(name))
 
    def print_error(self, msg):
        """Display an error message."""
        print("{0.bold_red}Error{0.normal} - {1}".format(self.terminal, msg))


    def print_success(self, msg):
        """Display a success message."""
        print("{0.bold_green}Success{0.normal} - {1}".format(self.terminal, msg))


    def put(self, args):
        "Put a file to a path."
        is_reference = args["--ref"]

        if is_reference:
            url = args["<url>"]
            dest_path = args["<dest>"]
            # Get the full destination path of the new resource
            dest_path = self.get_full_path(dest_path)
        else:
            src = args["<src>"]
            # Absolutize local path
            local_path = os.path.abspath(src)

            # Check that local file exists
            if not os.path.exists(local_path):
                self.print_error("File '{}' doesn't exist".format(local_path))
                return errno.ENOENT

            if args["<dest>"]:
                dest_path = args["<dest>"]
                
                # We try to put the new file in a subcollection
                if dest_path.endswith('/'):
                    dest_path = "{}{}".format(dest_path,
                                              os.path.basename(local_path))
            else:
                # PUT to same name in pwd on server
                dest_path = os.path.basename(local_path)

            # Get the full destination path of the new resource
            dest_path = self.get_full_path(dest_path)

        # Check resource objects on the database
        resc = Resource.find(dest_path)
        if resc:
            self.print_error(MSG_RESC_EXIST.format(dest_path))
            return
        
        parent, name = split(dest_path)
        try:
            if is_reference:
                resc = Resource.create(parent, name, url=url)
            else:
                resc = Resource.create(parent, name)
                with open(local_path, "rb") as fh:
                    resc.put(fh)
            print(resc)
        except NoSuchCollectionError:
            self.print_error(MSG_COLL_NOT_EXIST.format(os.path.dirname(dest_path)))


    def pwd(self, args):
        """Print working directory"""
        session = self.get_session()
        print(session.get('cwd', '/'))


    def rm(self, args):
        """Remove a data object or a collection.
        """
        path = args["<path>"]
         # Get the full path of the object to delete
        path = self.get_full_path(path)
        
        resc = Resource.find(path)
        if resc:
            resc.delete()
            return
        
        coll = Collection.find(path)
        if coll:
            coll.delete()
            return
            
        self.print_error(MSG_NO_OBJECT.format(path))


    def rm_from_group(self, args):
        """Remove user(s) from a group."""
        groupname = args[ARG_NAME]
        group = Group.find(groupname)
        if not group:
            self.print_error(MSG_GROUP_NOT_EXIST.format(groupname))
            return
        ls_users = args[ARG_USERLIST]
        removed, not_there, not_exist = group.rm_users(ls_users)
        if removed:
            self.print_success(
                "Removed {} from the group {}".format(", ".join(removed), group.name)
            )
        if not_there:
            if len(not_there) == 1:
                verb = "isn't"
            else:
                verb = "aren't"
            self.print_error(
                "{} {} in the group {}".format(", ".join(not_there), verb, group.name)
            )
        if not_exist:
            if len(not_exist) == 1:
                msg = "{} doesn't exist"
            else:
                msg = "{} don't exist"
            self.print_error(msg.format(", ".join(not_exist)))

    def rm_group(self, args):
        """Remove a group."""
        if not args[ARG_NAME]:
            name = input(MSG_PROMPT_GROUP)
        else:
            name = args[ARG_NAME]
        group = Group.find(name)
        if not group:
            self.print_error(MSG_GROUP_NOT_EXIST.format(name))
            return
        group.delete()
        print(MSG_GROUP_DELETED.format(name))

    def rm_user(self, args):
        """Remove a user."""
        if not args[ARG_NAME]:
            name = input(MSG_PROMPT_USER)
        else:
            name = args[ARG_NAME]
        user = User.find(name)
        if not user:
            self.print_error(MSG_USER_NOT_EXIST.format(name))
            return
        user.delete()
        print(MSG_USER_DELETED.format(name))
# 
    def save_session(self, session):
        """Save the status of the session for subsequent use."""
        if not os.path.exists(os.path.dirname(self.session_path)):
            os.makedirs(os.path.dirname(self.session_path))
        # Save existing session, so as to keep current dir etc.
        with open(self.session_path, "wb") as fh:
            pickle.dump(session, fh, pickle.HIGHEST_PROTOCOL)


def main():
    """Main function"""
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("dse.policies").setLevel(logging.WARNING)
    logging.getLogger("dse.cluster").setLevel(logging.WARNING)
    logging.getLogger("dse.cqlengine.management").setLevel(logging.WARNING)
    import sys
    arguments = docopt.docopt(
        __doc_opt__, version="Radon Admin CLI {}".format(radon.__version__)
    )
    app = RadonApplication(SESSION_PATH)

    if arguments["init"]:
        return app.init()
    if arguments["drop"]:
        return app.drop()

    elif arguments["ls"]:
        return app.ls(arguments)
    elif arguments["mkdir"]:
        return app.mkdir(arguments)
    elif arguments["pwd"]:
        return app.pwd(arguments)
    elif arguments["cd"]:
        return app.change_dir(arguments)
    elif arguments["put"]:
        return app.put(arguments)
    elif arguments["get"]:
        return app.get(arguments)
    elif arguments["rm"]:
        return app.rm(arguments)

    elif arguments["lg"]:
        return app.list_groups(arguments)
    elif arguments["lu"]:
        return app.list_users(arguments)
    elif arguments["atg"]:
        return app.add_to_group(arguments)
    elif arguments["mkgroup"]:
        return app.mk_group(arguments)
    elif arguments["mkldapuser"]:
        return app.mk_ldap_user(arguments)
    elif arguments["mkuser"]:
        return app.mk_user(arguments)
    elif arguments["moduser"]:
        return app.mod_user(arguments)
    elif arguments["rfg"]:
        return app.rm_from_group(arguments)
    elif arguments["rmgroup"]:
        return app.rm_group(arguments)
    elif arguments["rmuser"]:
        return app.rm_user(arguments)


if __name__ == "__main__":
    main()


