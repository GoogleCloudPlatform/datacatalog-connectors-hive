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

import os
import unittest
from unittest import mock

from google.datacatalog_connectors.apache_atlas import sync
from google.datacatalog_connectors.commons_test import utils

_PREPARE_PACKAGE = 'google.datacatalog_connectors.apache_atlas.prepare'
__SYNC_PACKAGE = 'google.datacatalog_connectors.apache_atlas.sync'
_SYNC_MODULE = '{}.metadata_synchronizer'.format(__SYNC_PACKAGE)


@mock.patch('{}.ingest.DataCatalogMetadataIngestor'.format(_SYNC_MODULE))
@mock.patch('{}.cleanup.DataCatalogMetadataCleaner'.format(_SYNC_MODULE))
@mock.patch('{}.EntryRelationshipMapper'.format(_PREPARE_PACKAGE))
class MetadataSynchronizerTest(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    @mock.patch('{}.prepare.AssembledEntryFactory'.format(_SYNC_MODULE))
    @mock.patch('{}.scrape.MetadataScraper'.format(_SYNC_MODULE))
    def setUp(self, mock_scraper, mock_assembled_entry_factory):
        self.__synchronizer = sync.MetadataSynchronizer(
            'test-project', 'test-location', {
                'host': 'my_host',
                'port': 'my_port',
                'user': 'my_user',
                'pass': 'my_pass',
            })

    def test_constructor_should_set_instance_attributes(self, *_):
        attrs = self.__synchronizer.__dict__

        self.assertEqual('test-project', attrs['_project_id'])
        self.assertEqual('test-location', attrs['_location_id'])
        self.assertIsNotNone(attrs['_metadata_scraper'])
        self.assertIsNotNone(attrs['_tag_template_factory'])
        self.assertEqual('my_host', attrs['_instance_url'])
        self.assertIsNotNone(attrs['_assembled_entry_factory'])

    def test_run_no_metadata_should_succeed(self, mock_mapper, mock_cleaner,
                                            mock_ingestor):
        scraper = self.__synchronizer.__dict__['_metadata_scraper']

        scraper.get_metadata.return_value = {
            'classifications': {},
            'entity_types': {},
            'enum_types': {}
        }, None

        self.__synchronizer.run()

        scraper.get_metadata.assert_called_once()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_metadata_should_succeed(self, mock_mapper, mock_cleaner,
                                         mock_ingestor):
        scraper = self.__synchronizer.__dict__['_metadata_scraper']

        entity_types_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'entity_types_metadata_for_tag_templates.json')

        classifications_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH,
            'classifications_metadata_for_tag_templates.json')

        scraper.get_metadata.return_value = {
            'classifications': classifications_dict,
            'entity_types': entity_types_dict,
            'enum_types': {}
        }, None

        self.__synchronizer.run()

        scraper.get_metadata.assert_called_once()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()
