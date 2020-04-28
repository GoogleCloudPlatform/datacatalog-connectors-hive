#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

import unittest

from google.datacatalog_connectors.hive import entities


class EntitiesTestCase(unittest.TestCase):

    def test_entities_dump_should_create_json_object(self):
        databases = EntitiesTestCase.__create_databases_object()
        databases_json = json.dumps(
            ([database.dump() for database in databases]))
        self.assertIsNotNone(databases_json)

    @staticmethod
    def __create_databases_object():
        database = entities.Database()
        database.id = 1
        database.name = 'database'
        database.desc = 'database desc'
        database.uri = 'database uri'

        table = entities.Table()
        table.id = 1
        table.name = 'table'
        table.type = 'table'
        table.create_time = 12312421
        table.database_id = 1
        table.sd_id = 1

        table_storage = entities.TableStorage()
        table_storage.sd_id = 1
        table_storage.location = 'storage location'
        table_storage.cd_id = 1

        column = entities.Column()

        column.id = 1
        column.name = 'column'
        column.type = 'int'
        column.comment = 'column comment'

        columns = [column]

        table_storage.columns = columns
        table_storages = [table_storage]

        table_param = entities.TableParams()
        table_param.id = 1
        table_param.param_key = 'key'
        table_param.param_value = 'value'
        table_params = [table_param]

        table.table_storages = table_storages
        table.table_params = table_params

        tables = [table]
        database.tables = tables
        return [database]
