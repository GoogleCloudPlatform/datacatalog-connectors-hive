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
import os
import unittest

from unittest.mock import patch

from google.datacatalog_connectors.hive.prepare import \
    assembled_entry_factory


@patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.entry_path')
class AssembledEntryFactoryTestCase(unittest.TestCase):
    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __ENTRY_GROUP_ID = 'entry_group_id'
    __MOCKED_ENTRY_PATH = 'mocked_entry_path'

    __METADATA_SERVER_HOST = 'metadata_host'

    def test_database_metadata_should_be_converted_to_assembled_entries(
            self, entry_path):  # noqa

        entry_path.return_value = \
            AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH

        factory = assembled_entry_factory.AssembledEntryFactory(
            AssembledEntryFactoryTestCase.__PROJECT_ID,
            AssembledEntryFactoryTestCase.__LOCATION_ID,
            AssembledEntryFactoryTestCase.__METADATA_SERVER_HOST,
            AssembledEntryFactoryTestCase.__ENTRY_GROUP_ID)

        database_metadata = convert_json_to_metadata_object(
            retrieve_json_file('databases.json'))

        assembled_entries = \
            factory.make_entries_from_database_metadata(
                database_metadata)

        databases = database_metadata['databases']

        # This is the size of the combination of tables and databases metadata
        # The returned object contains a list of database +
        # a list of tables for each database
        expected_created_entries_len = \
            sum([len(database_item['tables']) for database_item in databases],
                len(databases))

        # This is the size of the entries created based on the metadata
        # The returned tuple contains 1 database +
        # a list of tables related to the database on
        # each iteration
        created_entries_len = sum(
            [len(tables) for (database, tables) in assembled_entries],
            len(assembled_entries))

        self.assertEqual(expected_created_entries_len, created_entries_len)
        self.__assert_created_entry_fields(assembled_entries)

    def test_database_metadata_should_be_converted_to_assembled_entries_verify_all_fields(  # noqa: E501
            self, entry_path):
        entry_path.return_value = \
            AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH
        factory = assembled_entry_factory.AssembledEntryFactory(
            AssembledEntryFactoryTestCase.__PROJECT_ID,
            AssembledEntryFactoryTestCase.__LOCATION_ID,
            AssembledEntryFactoryTestCase.__METADATA_SERVER_HOST,
            AssembledEntryFactoryTestCase.__ENTRY_GROUP_ID)

        database_metadata = convert_json_to_metadata_object(
            retrieve_json_file('databases_with_one_table.json'))

        assembled_entries = \
            factory.make_entries_from_database_metadata(
                database_metadata)

        for assembled_database, assembled_tables in assembled_entries:
            database_entry = assembled_database.entry

            self.assertEqual('default', assembled_database.entry_id)
            self.assertEqual('database', database_entry.user_specified_type)
            self.assertEqual(AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH,
                             database_entry.name)
            self.assertEqual('Default Hive database',
                             database_entry.description)
            self.assertEqual(
                '//metadata_host//hdfs://namenode:8020/user/hive/warehouse',
                database_entry.linked_resource)
            self.assertEqual('hive', database_entry.user_specified_system)

            for assembled_table in assembled_tables:
                table_entry = assembled_table.entry
                self.assertEqual('default__funds', assembled_table.entry_id)
                self.assertEqual(
                    1567519048,
                    table_entry.source_system_timestamps.create_time.seconds)
                self.assertEqual(
                    1567519078,
                    table_entry.source_system_timestamps.update_time.seconds)
                # Assert specific fields for table
                self.assertEqual('table', table_entry.user_specified_type)
                self.assertEqual('hive', table_entry.user_specified_system)
                self.assertEqual(
                    AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH,
                    table_entry.name)
                self.assertEqual(
                    '//metadata_host//hdfs://'
                    'namenode:8020/user/hive/warehouse/funds',
                    table_entry.linked_resource)
                self.assertGreater(len(table_entry.schema.columns), 0)
                first_column = table_entry.schema.columns[0]
                self.assertEqual('string', first_column.type)
                self.assertEqual('addr', first_column.column)
                self.assertEqual('a new addr column', first_column.description)

                second_column = table_entry.schema.columns[1]
                self.assertEqual('string', second_column.type)
                self.assertEqual('bar', second_column.column)

                third_column = table_entry.schema.columns[2]
                self.assertEqual('int', third_column.type)
                self.assertEqual('foo', third_column.column)

    def test_database_metadata_no_tables_should_be_converted_to_datacatalog_entries(  # noqa: E501
            self, entry_path):
        entry_path.return_value = \
            AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH
        factory = assembled_entry_factory.AssembledEntryFactory(
            AssembledEntryFactoryTestCase.__PROJECT_ID,
            AssembledEntryFactoryTestCase.__LOCATION_ID,
            AssembledEntryFactoryTestCase.__METADATA_SERVER_HOST,
            AssembledEntryFactoryTestCase.__ENTRY_GROUP_ID)

        database_metadata = convert_json_to_metadata_object(
            retrieve_json_file('databases_no_tables.json'))

        assembled_entries = \
            factory.make_entries_from_database_metadata(
                database_metadata)

        databases = database_metadata['databases']

        # In this case we should not have tables
        expected_created_tables_len = sum(
            [len(database_item['tables']) for database_item in databases])
        self.assertEqual(0, expected_created_tables_len)
        expected_created_entries_len = \
            expected_created_tables_len + len(database_metadata)

        # This is the size of the entries created based on the metadata
        # The returned tuple contains 1 database + a list of tables
        # related to the database on
        # each iteration
        created_tables_len = sum(
            [len(tables) for (database, tables) in assembled_entries])
        self.assertEqual(0, created_tables_len)
        created_entries_len = created_tables_len + len(assembled_entries)

        self.assertEqual(expected_created_entries_len, created_entries_len)
        self.__assert_created_entry_fields(assembled_entries)

    def __assert_created_entry_fields(self, assembled_entries):
        for assembled_database, assembled_tables in assembled_entries:
            database_entry = assembled_database.entry
            self.__assert_required(assembled_database.entry_id,
                                   assembled_database.entry)

            # Assert specific fields for database
            self.assertEqual('database', database_entry.user_specified_type)
            self.assertEqual('hive', database_entry.user_specified_system)
            self.assertEqual(AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH,
                             database_entry.name)
            self.assertIn(AssembledEntryFactoryTestCase.__METADATA_SERVER_HOST,
                          database_entry.linked_resource)

            for user_defined_table in assembled_tables:
                table_entry = user_defined_table.entry
                self.__assert_required(user_defined_table.entry_id,
                                       table_entry)
                self.assertIsNotNone(
                    table_entry.source_system_timestamps.create_time.seconds)
                self.assertIsNotNone(
                    table_entry.source_system_timestamps.update_time.seconds)
                # Assert specific fields for table
                self.assertEqual('table', table_entry.user_specified_type)
                self.assertEqual('hive', table_entry.user_specified_system)
                self.assertEqual(
                    AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH,
                    table_entry.name)
                self.assertIn(
                    AssembledEntryFactoryTestCase.__METADATA_SERVER_HOST,
                    table_entry.linked_resource)
                self.assertGreater(len(table_entry.schema.columns), 0)
                for column in table_entry.schema.columns:
                    self.assertIsNotNone(column.type)
                    self.assertIsNotNone(column.column)

    def __assert_required(self, entry_id, entry):
        self.assertIsNotNone(entry_id)
        self.assertIsNotNone(entry.user_specified_type)
        self.assertIsNotNone(entry.name)
        self.assertIsNotNone(entry.description)
        self.assertIsNotNone(entry.linked_resource)


def retrieve_json_file(name):
    resolved_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '../test_data/{}'.format(name))

    with open(resolved_name) as json_file:
        return json_file.read()


def convert_json_to_metadata_object(parsed_json):
    return {
        'databases':
            json.loads(
                parsed_json,
                object_hook=lambda dict_obj: DictWithAttributeAccess(dict_obj))
    }


class DictWithAttributeAccess(dict):

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value
