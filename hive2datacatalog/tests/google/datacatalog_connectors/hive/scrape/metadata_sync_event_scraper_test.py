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

from google.datacatalog_connectors.hive import scrape


class MetadataScraperTestCase(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.hive.scrape'

    def test_scrape_message_no_event_should_raise_exception(self):
        self.assertRaises(
            Exception, scrape.MetadataSyncEventScraper.get_database_metadata,
            {})

    def test_scrape_message_unknown_event_should_raise_exception(self):
        self.assertRaises(
            Exception, scrape.MetadataSyncEventScraper.get_database_metadata,
            {'event': 'unknown'})

    def test_scrape_create_table_message_metadata_should_return_objects(self):
        databases_metadata = \
            scrape.MetadataSyncEventScraper.get_database_metadata(
                retrieve_json_file('hooks/message_create_table.json'))
        self.assertIsNotNone(databases_metadata)

        database_metadata = databases_metadata['databases'][0]

        self.assertEqual('default', database_metadata.name)
        table = database_metadata.tables[0]
        self.assertEqual('company_funds', table.name)
        self.assertEqual(1575043266, table.create_time)
        self.assertEqual('table', table.type)
        table_storage = table.table_storages[0]
        self.assertEqual(
            'hdfs://namenode:8020/user/hive/warehouse/company_funds',
            table_storage.location)
        columns = table_storage.columns
        column_code = columns[0]
        self.assertEqual('code', column_code.name)
        self.assertEqual('int', column_code.type)
        self.assertEqual(None, column_code.comment)

        column_desc = columns[1]
        self.assertEqual('desc', column_desc.name)
        self.assertEqual('string', column_desc.type)
        self.assertEqual(None, column_desc.comment)

    def test_scrape_update_table_message_metadata_should_return_objects(self):
        databases_metadata =\
            scrape.MetadataSyncEventScraper.get_database_metadata(
                retrieve_json_file('hooks/message_update_table.json'))
        self.assertIsNotNone(databases_metadata)

        database_metadata = databases_metadata['databases'][0]

        self.assertEqual('default', database_metadata.name)
        table = database_metadata.tables[0]
        self.assertEqual('company_funds', table.name)
        self.assertEqual(1575043266, table.create_time)
        self.assertEqual('table', table.type)
        self.assertEqual('1575043287', table.table_params[0].param_value)
        table_storage = table.table_storages[0]
        self.assertEqual(
            'hdfs://namenode:8020/user/hive/warehouse/company_funds',
            table_storage.location)
        columns = table_storage.columns
        column_code = columns[0]
        self.assertEqual('code', column_code.name)
        self.assertEqual('int', column_code.type)
        self.assertEqual(None, column_code.comment)

        column_desc = columns[1]
        self.assertEqual('desc', column_desc.name)
        self.assertEqual('string', column_desc.type)
        self.assertEqual(None, column_desc.comment)

    def test_scrape_drop_table_message_metadata_should_return_objects(self):
        databases_metadata =\
            scrape.MetadataSyncEventScraper.get_database_metadata(
                retrieve_json_file('hooks/message_drop_table.json'))
        self.assertIsNotNone(databases_metadata)

        database_metadata = databases_metadata['databases'][0]

        self.assertEqual('default', database_metadata.name)
        table = database_metadata.tables[0]
        self.assertEqual('company_funds', table.name)
        self.assertEqual(1575043266, table.create_time)
        self.assertEqual('table', table.type)
        table_storage = table.table_storages[0]
        self.assertEqual(
            'hdfs://namenode:8020/user/hive/warehouse/company_funds',
            table_storage.location)
        columns = table_storage.columns
        column_code = columns[0]
        self.assertEqual('code', column_code.name)
        self.assertEqual('int', column_code.type)
        self.assertEqual(None, column_code.comment)

        column_desc = columns[1]
        self.assertEqual('desc', column_desc.name)
        self.assertEqual('string', column_desc.type)
        self.assertEqual(None, column_desc.comment)

    def test_scrape_create_database_message_metadata_should_return_objects(
            self):  # noqa
        databases_metadata =\
            scrape.MetadataSyncEventScraper.get_database_metadata(
                retrieve_json_file('hooks/message_create_database.json'))
        self.assertIsNotNone(databases_metadata)

        database_metadata = databases_metadata['databases'][0]

        self.assertEqual('HR', database_metadata.name)
        self.assertEqual('hdfs://namenode:8020/user/hive/warehouse/hr.db',
                         database_metadata.uri)

    def test_scrape_drop_database_message_metadata_should_return_objects(self):
        databases_metadata =\
            scrape.MetadataSyncEventScraper.get_database_metadata(
                retrieve_json_file('hooks/message_drop_database.json'))
        self.assertIsNotNone(databases_metadata)

        database_metadata = databases_metadata['databases'][0]

        self.assertEqual('HR', database_metadata.name)
        self.assertEqual('hdfs://namenode:8020/user/hive/warehouse/hr.db',
                         database_metadata.uri)


def retrieve_json_file(name):
    resolved_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '../test_data/{}'.format(name))

    with open(resolved_name) as json_file:
        return json.load(json_file)
