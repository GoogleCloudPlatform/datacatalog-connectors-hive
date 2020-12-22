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

from google.datacatalog_connectors.hive.prepare import \
    datacatalog_entry_factory


class DataCatalogEntryFactoryTestCase(unittest.TestCase):
    __MODULE_PATH = '{}'.format(os.path.dirname(os.path.abspath(__file__)))

    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __METADATA_SERVER_HOST = 'metadata_host'
    __ENTRY_GROUP_ID = 'sql_database'

    def test_make_entry_for_database_should_set_all_available_fields(self):
        databases = convert_json_to_metadata_object(
            retrieve_json_file('databases_with_one_table.json'))['databases']

        factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)

        entry_id, entry = \
            factory.make_entries_for_database(databases[0])

        self.assertEqual('default', entry_id)
        self.assertEqual('default', entry.display_name)
        self.assertEqual(
            'projects/test_project/locations/location_id/'
            'entryGroups/sql_database/entries/default', entry.name)
        self.assertEqual('hive', entry.user_specified_system)
        self.assertEqual('database', entry.user_specified_type)
        self.assertEqual(
            '//metadata_host//hdfs://'
            'namenode:8020/user/hive/warehouse', entry.linked_resource)
        self.assertIsNone(entry.source_system_timestamps.create_time)
        self.assertIsNone(entry.source_system_timestamps.update_time)

    def test_make_entry_for_table_should_set_all_available_fields(self):
        databases = convert_json_to_metadata_object(
            retrieve_json_file('databases_with_one_table.json'))['databases']

        table = databases[0].tables[0]

        factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)

        entry_id, entry = \
            factory.make_entry_for_table(table, 'my_datawarehouse')

        self.assertEqual('my_datawarehouse__funds', entry_id)
        self.assertEqual('funds', entry.display_name)
        self.assertEqual(
            'projects/test_project/locations/location_id/'
            'entryGroups/sql_database/entries/'
            'my_datawarehouse__funds', entry.name)
        self.assertEqual('hive', entry.user_specified_system)
        self.assertEqual('table', entry.user_specified_type)
        self.assertEqual(
            '//metadata_host//'
            'hdfs://namenode:8020/user/hive/warehouse/funds',
            entry.linked_resource)
        self.assertEqual('2019-09-03 13:57:28+00:00',
                         str(entry.source_system_timestamps.create_time))
        self.assertEqual('2019-09-03 13:57:58+00:00',
                         str(entry.source_system_timestamps.update_time))

    def test_make_entry_for_table_invalid_database_name_should_set_all_available_fields(  # noqa:E501
            self):
        databases = convert_json_to_metadata_object(
            retrieve_json_file('databases_with_one_table.json'))['databases']

        table = databases[0].tables[0]

        factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)

        invalid_database_name = 'my::::????)()()____invalid_huge_' \
                                'database_name_!!!!!!_@@@@@@_with' \
                                '_chars_not_supported_by_dc_and_a' \
                                'length_that_is_too_long_and_needs' \
                                '_to_be_truncated'

        entry_id, entry = \
            factory.make_entry_for_table(table, invalid_database_name)

        self.assertEqual(
            'my_____invalid_huge_database_name'
            '_____with_chars_not_supecec4307', entry_id)
        self.assertEqual('funds', entry.display_name)
        self.assertEqual(
            'projects/test_project/locations/location_id/entryGroups/'
            'sql_database/entries/my_____invalid_huge_database_name'
            '_____with_chars_not_supecec4307', entry.name)
        self.assertEqual('hive', entry.user_specified_system)
        self.assertEqual('table', entry.user_specified_type)
        self.assertEqual(
            '//metadata_host//'
            'hdfs://namenode:8020/user/hive/warehouse/funds',
            entry.linked_resource)
        self.assertEqual('2019-09-03 13:57:28+00:00',
                         str(entry.source_system_timestamps.create_time))
        self.assertEqual('2019-09-03 13:57:58+00:00',
                         str(entry.source_system_timestamps.update_time))

    def test_make_entry_for_table_invalid_table_name_should_set_all_available_fields(  # noqa:E501
            self):
        databases = convert_json_to_metadata_object(
            retrieve_json_file('databases_with_one_table.json'))['databases']

        table = databases[0].tables[0]

        factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)

        invalid_table_name = 'my::::????)()()____invalid_huge_' \
                             'table_name_!!!!!!_@@@@@@_with' \
                             '_chars_not_supported_by_dc_and_a' \
                             'length_that_is_too_long_and_needs' \
                             '_to_be_truncated'

        table.name = invalid_table_name

        entry_id, entry = \
            factory.make_entry_for_table(table, 'my_datawarehouse')

        self.assertEqual(
            'my_datawarehouse__my_____'
            'invalid_huge_table_name_____wit0d3e0443', entry_id)
        self.assertEqual(
            'my_____invalid_huge_table_name_____'
            'with_chars_not_supported_by_dc_and_'
            'alength_that_is_too_long_and_needs_'
            'to_be_truncated', entry.display_name)
        self.assertEqual(
            'projects/test_project/locations/location_id/entryGroups/'
            'sql_database/entries/my_datawarehouse__my_____'
            'invalid_huge_table_name_____wit0d3e0443', entry.name)
        self.assertEqual('hive', entry.user_specified_system)
        self.assertEqual('table', entry.user_specified_type)
        self.assertEqual(
            '//metadata_host//'
            'hdfs://namenode:8020/user/hive/warehouse/funds',
            entry.linked_resource)
        self.assertEqual('2019-09-03 13:57:28+00:00',
                         str(entry.source_system_timestamps.create_time))
        self.assertEqual('2019-09-03 13:57:58+00:00',
                         str(entry.source_system_timestamps.update_time))

    def test_make_entry_for_table_long_linked_resource_should_set_all_available_fields(  # noqa:E501
            self):
        databases = convert_json_to_metadata_object(
            retrieve_json_file('databases_with_one_table.json'))['databases']

        table = databases[0].tables[0]

        table.table_storages[0].location = \
            'my::::????)()()____invalid_huge_table_name_!!!!!!_@@@@@@_with' \
            '_chars_not_supported_by_dc_and_a_length_that_is_' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated' \
            'too_long_and_needs_to_be_truncated'

        factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)

        entry_id, entry = \
            factory.make_entry_for_table(table, 'my_datawarehouse')

        self.assertEqual('my_datawarehouse__funds', entry_id)
        self.assertEqual('funds', entry.display_name)
        self.assertEqual(
            'projects/test_project/locations/location_id/'
            'entryGroups/sql_database/entries/'
            'my_datawarehouse__funds', entry.name)
        self.assertEqual('hive', entry.user_specified_system)
        self.assertEqual('table', entry.user_specified_type)
        self.assertEqual(
            '//metadata_host//my::::_____invalid_huge_table_name'
            '_____with_chars_not_supported_by_dc_and_a_length_that'
            '_is_too_long_and_needs_to_be_truncatedtoo_long_and'
            '_needs_to_be_truncatedtoo_long_and_needs_to...',
            entry.linked_resource)
        self.assertEqual('2019-09-03 13:57:28+00:00',
                         str(entry.source_system_timestamps.create_time))
        self.assertEqual('2019-09-03 13:57:58+00:00',
                         str(entry.source_system_timestamps.update_time))


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
