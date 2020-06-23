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

from google.datacatalog_connectors.commons_test import utils

from google.datacatalog_connectors.apache_atlas import scrape


class MetadataEventScraperTest(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.apache_atlas.scrape'

    @mock.patch('{}.apache_atlas_event_facade.ApacheAtlasEventFacade'.format(
        __SCRAPE_PACKAGE))
    @mock.patch(
        '{}.apache_atlas_facade.ApacheAtlasFacade'.format(__SCRAPE_PACKAGE))
    def setUp(self, apache_atlas_facade, apache_atlas_event_facade):
        self.__scrape = scrape.MetadataEventScraper({
            'host': 'my_host',
            'port': 'my_port',
            'user': 'my_user',
            'pass': 'my_pass',
            'event_servers': 'my_host:port',
            'event_consumer_group_id': 'my_consumer_group'
        })
        # Shortcut for the object assigned
        # to self.__scrape.__apache_atlas_event_facade
        self.__apache_atlas_event_facade = \
            apache_atlas_event_facade.return_value
        self.__apache_atlas_facade = apache_atlas_facade.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__scrape.__dict__
        self.assertIsNotNone(
            attrs['_MetadataEventScraper__apache_atlas_event_facade'])
        self.assertIsNotNone(attrs['_apache_atlas_facade'])

    @mock.patch(
        '{}.metadata_enricher.MetadataEnricher.enrich_entity_relationships'.
        format(__SCRAPE_PACKAGE))
    def test_scrape_delete_classification_should_return_metadata(
            self, enrich_entity_relationships):

        metadata = self.__create_scrape_event_scenario(
            'delete_classification_events')
        types_count = 51

        self.assertEqual(4, len(metadata))
        entity_events = metadata['entity_events']
        entity_event = entity_events[0]

        self.assertEqual('CLASSIFICATION_DELETE',
                         entity_event['operationType'])
        self.assertEqual(types_count, len(metadata['entity_types']))
        self.assertEqual(8, len(metadata['entity_types']['Table']['entities']))

        expected_table_metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'expected_table_metadata.json')

        self.assertDictEqual(expected_table_metadata,
                             metadata['entity_types']['Table'])

        enrich_entity_relationships.assert_called_once()
        self.__apache_atlas_facade.get_typedefs.assert_called_once()
        self.assertEqual(
            0, self.__apache_atlas_facade.search_entities_from_entity_type.
            call_count)

    @mock.patch(
        '{}.metadata_enricher.MetadataEnricher.enrich_entity_relationships'.
        format(__SCRAPE_PACKAGE))
    def test_scrape_add_classification_should_return_metadata(
            self, enrich_entity_relationships):

        metadata = self.__create_scrape_event_scenario(
            'add_classification_events')
        types_count = 51

        self.assertEqual(4, len(metadata))
        entity_events = metadata['entity_events']
        entity_event = entity_events[0]

        self.assertEqual('CLASSIFICATION_ADD', entity_event['operationType'])
        self.assertEqual(types_count, len(metadata['entity_types']))
        self.assertEqual(0, len(metadata['entity_types']['Table']['entities']))

        enrich_entity_relationships.assert_called_once()
        self.__apache_atlas_facade.get_typedefs.assert_called_once()
        self.assertEqual(
            0, self.__apache_atlas_facade.search_entities_from_entity_type.
            call_count)

    def __create_scrape_event_scenario(self, event_name):
        # Step 1 - Set up event message
        event_msgs = self.__make_delete_classification_events_object(
            event_name)
        self.__apache_atlas_event_facade.create_event_consumer.return_value =\
            event_msgs
        # Step 2 - create the lazy object returned by Apache Atlas facade
        typedef = utils.MockedObject()
        typedefs = [typedef]
        self.__apache_atlas_facade.get_typedefs.return_value = typedefs
        # Step 3 - create the return for Atlas classifications
        classifications_defs = self.__make_classification_object()
        typedef.classificationDefs = classifications_defs
        # Step 4 - create the return for Atlas Enum rypes
        typedef.enumDefs = self.__make_enum_types_object()
        # Step 5 - create the return for Atlas Entity types
        entity_types = self.__make_entity_type_object()
        typedef.entityDefs = entity_types
        # Following steps are executed for each entity type.
        # Step 6 - create the return for search results
        search_results = self.__make_search_results_object()
        self.__apache_atlas_facade.search_entities_from_entity_type.\
            return_value = search_results
        # Step 7 - create the return for fetched entities
        fetched_entities_dict =\
            utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                               'fetched_entities_dict.json')
        self.__apache_atlas_facade.fetch_entities.return_value =\
            fetched_entities_dict
        # Step 8 - create the return for entities classifications
        entity_classifications =\
            utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                               'entity_classifications.json')
        self.__apache_atlas_facade.fetch_entity_classifications.return_value =\
            entity_classifications
        metadata, _ = self.__scrape.get_metadata()
        return metadata

    def __make_delete_classification_events_object(self, event_name):
        event_msgs = \
            utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                               '{}.json'.format(event_name))
        events_obj = []
        for event_msg in event_msgs:
            event_obj = utils.MockedObject()
            event_obj.topic = event_msg['topic']
            event_obj.partition = event_msg['partition']
            event_obj.offset = event_msg['offset']
            event_obj.key = event_msg['key']
            event_obj.value = json.dumps(event_msg['value']).encode('utf-8')
            events_obj.append(event_obj)

        return events_obj

    def __make_classification_object(self):
        classifications = \
            utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                               'classifications.json')
        classifications_obj = []
        for classification in classifications:
            classification_obj = utils.MockedObject()
            classification_obj.name = classification['name']
            classification_obj.guid = classification['guid']
            classification_obj._data = classification['data']
            classifications_obj.append(classification_obj)

        return classifications_obj

    def __make_entity_type_object(self):
        entity_types = \
            utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                               'entity_types.json')
        entity_types_obj = []
        for entity_type in entity_types:
            entity_type_obj = utils.MockedObject()
            entity_type_obj.name = entity_type['name']
            entity_type_obj.superTypes = entity_type['superTypes']
            entity_type_obj._data = entity_type['data']
            entity_types_obj.append(entity_type_obj)

        return entity_types_obj

    def __make_search_results_object(self):
        search_results = \
            utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                               'search_results.json')
        search_results_obj = []
        for search_result in search_results:
            search_result_obj = utils.MockedObject()
            search_result_obj.guid = search_result['guid']
            search_result_obj._data = search_result['data']
            search_results_obj.append(search_result_obj)

        return search_results_obj

    @classmethod
    def __make_enum_types_object(cls):
        enum_types_obj = []

        enum_type_obj = utils.MockedObject()
        enum_type_obj.name = 'my_enum_type'
        enum_type_obj._data = {}
        enum_type_obj.guid = '123'

        enum_types_obj.append(enum_type_obj)

        return enum_types_obj
