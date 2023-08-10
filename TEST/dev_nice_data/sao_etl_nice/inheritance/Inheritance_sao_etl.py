# -*- coding: utf-8 -*-

from abc import *


class InheritanceSaoEtl(metaclass=ABCMeta):

    def __init__(self, sao_db_connection, schema_name, com_info, default_date_version, base_da_standard_begin):
        self._sao_db_connection = sao_db_connection
        self._schema_name = schema_name
        self._default_date_version = default_date_version
        self._base_da_standard_begin = base_da_standard_begin
        self._com_info = com_info

        self._sao_db_connection.cursor.execute("set search_path to {schema_name};".format(schema_name=self._schema_name))

    @classmethod
    def sao_etl_start(cls):
        pass
