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

from sqlalchemy import exc
from pytest import raises

from google.datacatalog_connectors.hive import scrape


class MetadataScraperTestCase(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.hive.scrape'

    @classmethod
    def setUpClass(cls):
        MetadataScraperTestCase.hive_metastore_db_host = 'localhost'
        MetadataScraperTestCase.hive_metastore_db_user = 'dbs'
        MetadataScraperTestCase.hive_metastore_db_pass = 'dbs'
        MetadataScraperTestCase.hive_metastore_db_name = 'metastore'
        MetadataScraperTestCase.hive_metastore_db_type = 'postgresql'

    @patch(f'{__SCRAPE_PACKAGE}.metadata_database_scraper.sessionmaker')
    def test_scrape_databases_metadata_should_return_objects(
            self, sessionmaker):  # noqa

        sessionmaker.return_value = MockedSession

        metadata_scraper = scrape.MetadataDatabaseScraper(
            MetadataScraperTestCase.hive_metastore_db_host,
            MetadataScraperTestCase.hive_metastore_db_user,
            MetadataScraperTestCase.hive_metastore_db_pass,
            MetadataScraperTestCase.hive_metastore_db_name,
            MetadataScraperTestCase.hive_metastore_db_type)

        databases_metadata = metadata_scraper.get_database_metadata()

        self.assertEqual(6, len(databases_metadata['databases']))

    @patch(f'{__SCRAPE_PACKAGE}.metadata_database_scraper.sessionmaker')
    def test_scrape_databases_metadata_on_operational_error_should_reraise(
            self, sessionmaker):  # noqa

        sessionmaker.return_value = MockedErrorSession

        with raises(exc.OperationalError):
            metadata_scraper = scrape.MetadataDatabaseScraper(
                MetadataScraperTestCase.hive_metastore_db_host,
                MetadataScraperTestCase.hive_metastore_db_user,
                MetadataScraperTestCase.hive_metastore_db_pass,
                MetadataScraperTestCase.hive_metastore_db_name,
                MetadataScraperTestCase.hive_metastore_db_type)
            with patch('logging.error') as mocked_method:
                metadata_scraper.get_database_metadata()
                mocked_method.assert_called_once()


def retrieve_json_file(name):
    resolved_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '../test_data/{}'.format(name))

    with open(resolved_name) as json_file:
        return json.load(json_file)


class MockedSession(object):

    def __init__(self):
        self.__dict__ = {}
        self.__setitem__('query', lambda *args: MockedSession())
        self.__setitem__('options', lambda *args: MockedSession())
        self.__setitem__('all',
                         lambda *args: retrieve_json_file('databases.json'))

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]


def raise_operational_error():
    raise exc.OperationalError(statement='SQL QUERY', params=[], orig=1)


class MockedErrorSession(object):

    def __init__(self):
        self.__dict__ = {}
        self.__setitem__('query', lambda *args: MockedErrorSession())
        self.__setitem__('options', lambda *args: MockedErrorSession())
        self.__setitem__('all', lambda *args: raise_operational_error())

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
