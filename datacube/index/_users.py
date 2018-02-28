# coding=utf-8


class UserResource(object):
    def __init__(self, db):
        """
        :type db: datacube.drivers.postgres._connections.PostgresDb
        """
        self._db = db

    def grant_role(self, role, *usernames):
        """
        Grant a role to users
        """
        with self._db.connect() as connection:
            connection.grant_role(role, usernames)

    def create_user(self, username, password, role, description=None):
        """
        Create a new user.
        """
        with self._db.connect() as connection:
            connection.create_user(username, password, role, description=description)

    def delete_user(self, *usernames):
        """
        Delete a user
        """
        with self._db.connect() as connection:
            connection.drop_users(usernames)

    def list_users(self):
        """
        :return: list of (role, user, description)
        :rtype: list[(str, str, str)]
        """
        with self._db.connect() as connection:
            for role, user, description in connection.list_users():
                yield role, user, description
