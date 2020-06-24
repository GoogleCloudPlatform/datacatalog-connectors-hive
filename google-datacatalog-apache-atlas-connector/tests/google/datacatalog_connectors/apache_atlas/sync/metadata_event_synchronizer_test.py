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

from google.datacatalog_connectors.commons_test import utils

from google.datacatalog_connectors.apache_atlas import sync

_PREPARE_PACKAGE = 'google.datacatalog_connectors.apache_atlas.prepare'
__SYNC_PACKAGE = 'google.datacatalog_connectors.apache_atlas.sync'
_SYNC_MODULE = '{}.metadata_synchronizer'.format(__SYNC_PACKAGE)
_EVENT_SYNC_MODULE = '{}.metadata_event_synchronizer'.format(__SYNC_PACKAGE)


@mock.patch('{}.ingest.DataCatalogMetadataIngestor'.format(_SYNC_MODULE))
@mock.patch('{}.cleanup.DataCatalogMetadataCleaner'.format(_SYNC_MODULE))
@mock.patch('{}.EntryRelationshipMapper'.format(_PREPARE_PACKAGE))
class MetadataEventSynchronizerTest(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    @mock.patch('{}.prepare.AssembledEntryFactory'.format(_SYNC_MODULE))
    @mock.patch('{}.scrape.MetadataEventScraper'.format(_EVENT_SYNC_MODULE))
    @mock.patch('{}.scrape.MetadataScraper'.format(_EVENT_SYNC_MODULE))
    @mock.patch('google.datacatalog_connectors.commons.'
                'datacatalog_facade.DataCatalogFacade')
    def setUp(self, mock_datacatalog_facade, mock_scraper, mock_event_scraper,
              mock_assembled_entry_factory):
        self.__synchronizer = sync.MetadataEventSynchronizer(
            'test-project', 'test-location', {
                'host': 'my_host',
                'port': 'my_port',
                'user': 'my_user',
                'pass': 'my_pass',
                'event_servers': 'my_host:port',
                'event_consumer_group_id': 'my_consumer_group'
            })

    def test_constructor_should_set_instance_attributes(self, *_):
        attrs = self.__synchronizer.__dict__

        self.assertEqual('test-project', attrs['_project_id'])
        self.assertEqual('test-location', attrs['_location_id'])
        self.assertIsNotNone(attrs['_metadata_scraper'])
        self.assertIsNotNone(attrs['_tag_template_factory'])
        self.assertEqual('my_host', attrs['_instance_url'])
        self.assertIsNotNone(attrs['_assembled_entry_factory'])
        self.assertIsNotNone(
            attrs['_MetadataEventSynchronizer__datacatalog_facade'])

    @mock.patch('{}.sleep'.format(_EVENT_SYNC_MODULE),
                side_effect=InterruptedError)
    def test_run_no_metadata_should_succeed(self, mock_sleep, mock_mapper,
                                            mock_cleaner, mock_ingestor):
        scraper = self.__synchronizer.__dict__['_metadata_scraper']

        event_consumer = mock.MagicMock()

        scraper.get_metadata.return_value = {
            'classifications': {},
            'entity_types': {},
            'enum_types': {},
            'entity_events': {}
        }, event_consumer
        # We force an InterruptedError to stop the event_consumer poll loop
        with self.assertRaises(InterruptedError):
            self.__synchronizer.run()
            mock_sleep.assert_called_once()
            scraper.get_metadata.assert_called_once()

            mapper = mock_mapper.return_value
            mapper.fulfill_tag_fields.assert_called_once()

            cleaner = mock_cleaner.return_value
            cleaner.delete_obsolete_metadata.assert_called_once()

            ingestor = mock_ingestor.return_value
            ingestor.ingest_metadata.assert_called_once()

            event_consumer.commit.assert_called_once()
            event_consumer.close.assert_called_once()

    @mock.patch('{}.sleep'.format(_EVENT_SYNC_MODULE),
                side_effect=InterruptedError)
    def test_run_metadata_should_succeed(self, mock_sleep, mock_mapper,
                                         mock_cleaner, mock_ingestor):
        scraper = self.__synchronizer.__dict__['_metadata_scraper']

        event_consumer = mock.MagicMock()

        entity_types_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'entity_types_metadata_for_tag_templates.json')

        classifications_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH,
            'classifications_metadata_for_tag_templates.json')

        scraper.get_metadata.return_value = {
            'classifications': classifications_dict,
            'entity_types': entity_types_dict,
            'enum_types': {},
            'entity_events': {}
        }, event_consumer

        with self.assertRaises(InterruptedError):
            self.__synchronizer.run()
            mock_sleep.assert_called_once()
            scraper.get_metadata.assert_called_once()

            mapper = mock_mapper.return_value
            mapper.fulfill_tag_fields.assert_called_once()

            cleaner = mock_cleaner.return_value
            cleaner.delete_obsolete_metadata.assert_called_once()

            ingestor = mock_ingestor.return_value
            ingestor.ingest_metadata.assert_called_once()

            event_consumer.commit.assert_called_once()
            event_consumer.close.assert_called_once()

    @mock.patch('{}.scrape.MetadataEnricher'.format(_EVENT_SYNC_MODULE))
    @mock.patch('{}.sleep'.format(_EVENT_SYNC_MODULE),
                side_effect=InterruptedError)
    def test_run_metadata_columns_events_should_succeed(
            self, mock_sleep, mock_metadata_enricher, mock_mapper,
            mock_cleaner, mock_ingestor):
        scraper = self.__synchronizer.__dict__['_metadata_scraper']

        datacatalog_facade = self.__synchronizer.__dict__[
            '_MetadataEventSynchronizer__datacatalog_facade']

        event_consumer = mock.MagicMock()

        entity_types_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'entity_types_metadata_for_tag_templates.json')

        classifications_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH,
            'classifications_metadata_for_tag_templates.json')

        scraper.get_metadata.return_value = {
            'classifications':
                classifications_dict,
            'entity_types':
                entity_types_dict,
            'enum_types': {},
            'entity_events': [{
                'entity': {
                    'guid': '123',
                    'typeName': 'Column'
                },
                'operationType': 'CLASSIFICATION_ADD'
            }]
        }, event_consumer

        datacatalog_facade.get_tag_field_values_for_search_results.\
            return_value = ['234', '567']

        with self.assertRaises(InterruptedError):
            self.__synchronizer.run()
            mock_sleep.assert_called_once()
            scraper.get_metadata.assert_called_once()

            mapper = mock_mapper.return_value
            mapper.fulfill_tag_fields.assert_called_once()

            cleaner = mock_cleaner.return_value
            cleaner.delete_obsolete_metadata.assert_called_once()

            ingestor = mock_ingestor.return_value
            ingestor.ingest_metadata.assert_called_once()

            mock_metadata_enricher.MetadatEnricher.\
                enrich_entity_relationships.assert_called_once()

            event_consumer.commit.assert_called_once()
            event_consumer.close.assert_called_once()

    @mock.patch('{}.scrape.MetadataEnricher'.format(_EVENT_SYNC_MODULE))
    @mock.patch('{}.sleep'.format(_EVENT_SYNC_MODULE),
                side_effect=InterruptedError)
    def test_run_metadata_delete_events_should_succeed(self, mock_sleep,
                                                       mock_metadata_enricher,
                                                       mock_mapper,
                                                       mock_cleaner,
                                                       mock_ingestor):
        scraper = self.__synchronizer.__dict__['_metadata_scraper']

        datacatalog_facade = self.__synchronizer.__dict__[
            '_MetadataEventSynchronizer__datacatalog_facade']

        event_consumer = mock.MagicMock()

        entity_types_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'entity_types_metadata_for_tag_templates.json')

        classifications_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH,
            'classifications_metadata_for_tag_templates.json')

        scraper.get_metadata.return_value = {
            'classifications':
                classifications_dict,
            'entity_types':
                entity_types_dict,
            'enum_types': {},
            'entity_events': [{
                'type': 'ENTITY_NOTIFICATION_V2',
                'entity': {
                    'typeName': 'Table',
                    'attributes': {
                        'owner': 'John Doe',
                        'createTime': 1589983847369,
                        'qualifiedName': 'personal_info50bfc1d7',
                        'name': 'personal_info50bfc1d7',
                        'description': 'personal_info50bfc1d7'
                    },
                    'guid': '15539516-c6de-46bd-8b0f-2ac68b811789',
                    'status': 'DELETED',
                    'displayText': 'personal_info50bfc1d7'
                },
                'operationType': 'ENTITY_DELETE'
            }]
        }, event_consumer

        datacatalog_facade.get_tag_field_values_for_search_results.\
            return_value = ['234', '567']

        datacatalog_facade. \
            search_catalog_relative_resource_name.return_value = ['entry_1',
                                                                  'entry_2']

        with self.assertRaises(InterruptedError):
            self.__synchronizer.run()
            mock_sleep.assert_called_once()
            scraper.get_metadata.assert_called_once()

            mapper = mock_mapper.return_value
            mapper.fulfill_tag_fields.assert_called_once()

            cleaner = mock_cleaner.return_value
            cleaner.delete_obsolete_metadata.assert_called_once()

            ingestor = mock_ingestor.return_value
            ingestor.ingest_metadata.assert_called_once()

            mock_metadata_enricher.MetadatEnricher.\
                enrich_entity_relationships.assert_called_once()

            event_consumer.commit.assert_called_once()
            event_consumer.close.assert_called_once()

            self.assertEqual(2, datacatalog_facade.delete_entry.call_count)
