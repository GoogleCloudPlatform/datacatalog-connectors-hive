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
from unittest import mock

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

    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_database_scraper.sessionmaker')
    def test_scrape_databases_metadata_should_return_objects(
            self, sessionmaker):  # noqa

        mocked_session = mock.MagicMock()

        # Return the same instance, since session object
        # uses a builder pattern.
        mocked_session.return_value = mocked_session
        mocked_session.query.return_value = mocked_session
        mocked_session.limit.return_value = mocked_session
        mocked_session.options.return_value = mocked_session
        mocked_session.offset.return_value = mocked_session

        # Mocks with 2 pages, and third page is empty.
        mocked_session.all.side_effect = [
            retrieve_json_file('databases.json'),
            retrieve_json_file('databases.json'), []
        ]

        sessionmaker.return_value = mocked_session

        metadata_scraper = scrape.MetadataDatabaseScraper(
            MetadataScraperTestCase.hive_metastore_db_host,
            MetadataScraperTestCase.hive_metastore_db_user,
            MetadataScraperTestCase.hive_metastore_db_pass,
            MetadataScraperTestCase.hive_metastore_db_name,
            MetadataScraperTestCase.hive_metastore_db_type)

        databases_metadata = metadata_scraper.get_database_metadata()

        self.assertEqual(12, len(databases_metadata['databases']))

    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_database_scraper.sessionmaker')
    def test_scrape_databases_metadata_when_no_pages_should_return_empty(
            self, sessionmaker):  # noqa

        mocked_session = mock.MagicMock()

        # Return the same instance, since session object
        # uses a builder pattern.
        mocked_session.return_value = mocked_session
        mocked_session.query.return_value = mocked_session
        mocked_session.limit.return_value = mocked_session
        mocked_session.options.return_value = mocked_session
        mocked_session.offset.return_value = mocked_session

        mocked_session.all.return_value = []

        sessionmaker.return_value = mocked_session

        metadata_scraper = scrape.MetadataDatabaseScraper(
            MetadataScraperTestCase.hive_metastore_db_host,
            MetadataScraperTestCase.hive_metastore_db_user,
            MetadataScraperTestCase.hive_metastore_db_pass,
            MetadataScraperTestCase.hive_metastore_db_name,
            MetadataScraperTestCase.hive_metastore_db_type)

        databases_metadata = metadata_scraper.get_database_metadata()

        self.assertEqual(0, len(databases_metadata['databases']))

    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_database_scraper.sessionmaker')
    def test_scrape_databases_metadata_on_operational_error_should_reraise(
            self, sessionmaker):  # noqa

        mocked_session = mock.MagicMock()

        # Return the same instance, since session object
        # uses a builder pattern.
        mocked_session.return_value = mocked_session
        mocked_session.query.return_value = mocked_session
        mocked_session.limit.return_value = mocked_session
        mocked_session.options.return_value = mocked_session
        mocked_session.offset.return_value = mocked_session

        mocked_session.all.side_effect = exc.OperationalError(
            statement='SQL QUERY', params=[], orig=1)

        sessionmaker.return_value = mocked_session

        with raises(exc.OperationalError):
            metadata_scraper = scrape.MetadataDatabaseScraper(
                MetadataScraperTestCase.hive_metastore_db_host,
                MetadataScraperTestCase.hive_metastore_db_user,
                MetadataScraperTestCase.hive_metastore_db_pass,
                MetadataScraperTestCase.hive_metastore_db_name,
                MetadataScraperTestCase.hive_metastore_db_type)
            with mock.patch('logging.error') as mocked_method:
                metadata_scraper.get_database_metadata()
                mocked_method.assert_called_once()


def retrieve_json_file(name):
    resolved_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '../test_data/{}'.format(name))

    with open(resolved_name) as json_file:
        return json.load(json_file)
