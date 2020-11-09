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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons_test import utils

from google.datacatalog_connectors.apache_atlas import prepare


class AssembledEntryFactoryTest(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.apache_atlas.prepare'

    @mock.patch('{}.datacatalog_tag_factory'
                '.DataCatalogTagFactory'.format(__PREPARE_PACKAGE))
    @mock.patch('{}.datacatalog_entry_factory'
                '.DataCatalogEntryFactory'.format(__PREPARE_PACKAGE))
    def setUp(self, mock_entry_factory, mock_tag_factory):
        self.__assembled_data_factory = prepare.AssembledEntryFactory(
            'project-id', 'location-id', 'entry_group_id', 'user_system',
            'https://test.server.com')

        self.__entry_factory = mock_entry_factory.return_value
        self.__tag_factory = mock_tag_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__assembled_data_factory.__dict__

        self.assertIsNotNone(
            attrs['_AssembledEntryFactory__datacatalog_entry_factory'])
        self.assertIsNotNone(
            attrs['_AssembledEntryFactory__datacatalog_tag_factory'])

    def test_make_assembled_entries_list_should_return(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_entity = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_entity = self.__mock_make_tag
        tag_factory.make_tag_for_classification = \
            self.__mock_make_tag_classification
        tag_factory.make_tag_for_column_ref = \
            self.__mock_make_tag_for_column_ref

        tables_metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'tables_metadata.json')

        assembled_entries = \
            self.__assembled_data_factory.\
            make_assembled_entries_list(tables_metadata)

        self.assertEqual(8, len(assembled_entries))

        assembled_entry = assembled_entries[0]

        self.assertEqual('2286a6bd-4b06-4f7c-a666-920d774b040e',
                         assembled_entry.entry_id)
        self.assertEqual('fake_entries/2286a6bd-4b06-4f7c-a666-920d774b040e',
                         assembled_entry.entry.name)

        tags = assembled_entry.tags
        templates = [tag.template for tag in tags]

        entity_type_tag = sum(
            'entity_type' in template for template in templates)
        pii_tag = sum('PII/column' in template for template in templates)
        metric_tag = sum('Metric/column' in template for template in templates)
        column_ref_tag = sum(
            'column_ref' in template for template in templates)

        self.assertEqual(8, len(tags))
        self.assertEqual('fake_template/entity_type/Table', tags[0].template)
        self.assertEqual(1, entity_type_tag)
        self.assertEqual(1, pii_tag)
        self.assertEqual(1, metric_tag)
        self.assertEqual(4, column_ref_tag)

    def test_make_assembled_entries_list_types_found_should_return(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_entity = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_entity = self.__mock_make_tag
        tag_factory.make_tag_for_classification = \
            self.__mock_make_tag_classification
        tag_factory.make_tag_for_column_ref = \
            self.__mock_make_tag_for_column_ref

        tables_metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'tables_metadata.json')

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                tables_metadata,
                apache_entity_types=['Table'])

        self.assertEqual(8, len(assembled_entries))

        assembled_entry = assembled_entries[0]

        self.assertEqual('2286a6bd-4b06-4f7c-a666-920d774b040e',
                         assembled_entry.entry_id)
        self.assertEqual('fake_entries/2286a6bd-4b06-4f7c-a666-920d774b040e',
                         assembled_entry.entry.name)

        tags = assembled_entry.tags
        templates = [tag.template for tag in tags]

        entity_type_tag = sum(
            'entity_type' in template for template in templates)
        pii_tag = sum('PII/column' in template for template in templates)
        metric_tag = sum('Metric/column' in template for template in templates)
        column_ref_tag = sum(
            'column_ref' in template for template in templates)

        self.assertEqual(8, len(tags))
        self.assertEqual('fake_template/entity_type/Table', tags[0].template)
        self.assertEqual(1, entity_type_tag)
        self.assertEqual(1, pii_tag)
        self.assertEqual(1, metric_tag)
        self.assertEqual(4, column_ref_tag)

    def test_make_assembled_entries_list_no_types_found_should_return_empty(
            self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_entity = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_entity = self.__mock_make_tag
        tag_factory.make_tag_for_classification = \
            self.__mock_make_tag_classification
        tag_factory.make_tag_for_column_ref = \
            self.__mock_make_tag_for_column_ref

        tables_metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'tables_metadata.json')

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                tables_metadata,
                apache_entity_types=['InvalidType'])

        self.assertEqual(0, len(assembled_entries))

    @classmethod
    def __mock_make_entry(cls, entity):
        entry = datacatalog.Entry()
        entry_id = entity['guid']
        entry.name = 'fake_entries/{}'.format(entry_id)
        return entry_id, entry

    @classmethod
    def __mock_make_tag(cls, entity, *_):
        tag = datacatalog.Tag()
        tag.template = 'fake_template/entity_type/{}'.format(
            entity['data']['typeName'])
        return tag

    @classmethod
    def __mock_make_tag_classification(cls, classification, *args):
        tag = datacatalog.Tag()

        template = 'fake_template/{}'.format(classification['typeName'])

        if len(args) > 1:
            template = '{}/column'.format(template)

        tag.template = template
        return tag

    @classmethod
    def __mock_make_tag_for_column_ref(cls, column_guid, _):
        tag = datacatalog.Tag()
        tag.template = 'fake_template/{}/column_ref'.format(column_guid)
        return tag
