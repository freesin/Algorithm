# -*- coding: utf-8 -*-

import psycopg2


class Psycopg2Connection:

    def __init__(self):
        self.__host = None
        self.__port = None
        self.__user = None
        self.__password = None
        self.__database = None
        self.__schema_name = None

        self.__conn = None
        self.__cursor = None

    def __get__(self, instance, owner):
        return self.__conn

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, value):
        self.__host = value

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, value):
        self.__port = value

    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, value):
        self.__user = value

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, value):
        self.__password = value

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, value):
        self.__database = value

    @property
    def schema_name(self):
        return self.__schema_name

    @schema_name.setter
    def schema_name(self, value):
        self.__schema_name = value

    @property
    def cursor(self):
        if self.__cursor:
            return self.__cursor
        else:
            return None

    def connect(self):
        try:
            if self.__host and self.__port and self.__user and self.__password and self.__database:
                self.__conn = psycopg2.connect(host=self.__host,
                                               port=self.__port,
                                               user=self.__user,
                                               password=self.__password,
                                               database=self.__database)
                self.__cursor = self.__conn.cursor()
                return True

            return False
        except Exception as pgException:
            raise pgException

    def connect_with_schema(self):
        try:
            if self.__host and self.__port and self.__user and self.__password and self.__database and self.__schema_name:
                self.__conn = psycopg2.connect(host=self.__host,
                                               port=self.__port,
                                               user=self.__user,
                                               password=self.__password,
                                               database=self.__database)
                self.__cursor = self.__conn.cursor()
                self.__cursor.execute("""set search_path to {schema_name}""".format(schema_name=self.__schema_name))
                return True

            return False
        except Exception as pgException:
            raise pgException

    def commit(self):
        try:
            self.__conn.commit()
            return True
        except Exception as pgException:
            raise pgException

    def close(self):
        try:
            self.__conn.close()
            return True
        except Exception as pgException:
            raise pgException
