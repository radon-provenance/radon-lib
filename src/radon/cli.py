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

__doc_opt__ = """
Radon Admin Command Line Interface.

Usage:
  radmin create
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

from getpass import getpass
import logging
import string
import random
import blessings
import docopt

import radon
from radon.models import Group, initialise, sync, User

ARG_NAME = "<name>"
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

def random_password(length=10):
    """Generate a random string of fixed length """
    letters = string.ascii_letters + string.digits + string.punctuation
    return "".join(random.choice(letters) for _ in range(length))


class RadonApplication():
    """Methods for the CLI"""

    def __init__(self):
        self.terminal = blessings.Terminal()
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

    def create(self):
        """Create the keyspace and the tables"""
        sync()

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
    app = RadonApplication()

    if arguments["atg"]:
        return app.add_to_group(arguments)
    elif arguments["create"]:
        return app.create()
    elif arguments["lg"]:
        return app.list_groups(arguments)
    elif arguments["lu"]:
        return app.list_users(arguments)
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
