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

from google.datacatalog_connectors.apache_atlas.scrape import \
    metadata_enricher


class MetadataEnricherTest(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    def setUp(self):
        apache_atlas_facade = mock.MagicMock()

        self.__enricher = metadata_enricher.MetadataEnricher(
            apache_atlas_facade)
        # Shortcut for the object assigned
        # to self.__scrape.__apache_atlas_facade
        self.__apache_atlas_facade = apache_atlas_facade

    def test_enrich_entity_relationships_should_add_column_data(self):
        entity_types_dict = \
            utils.Utils.convert_json_to_object(
                self.__MODULE_PATH,
                'entity_types_dict_before_norm.json')

        # Before Table Columns were not filled.
        entity_tables = entity_types_dict['Table']
        data = entity_tables['entities'][
            '6fd39817-35d0-4f9a-a24b-80fe9f4d56ad']['data']
        attributes = data['attributes']
        columns = attributes['columns']
        first_column = columns[0]
        column_data = first_column.get('data')

        self.assertIsNone(column_data)

        # Retrieve expected columns.
        entity_columns = entity_types_dict['Column']
        expected_column_data = entity_columns['entities'][
            'cb44a958-026c-48d9-b4f3-4a6e2eab7234']['data']

        self.__enricher.enrich_entity_relationships(entity_types_dict)

        column_data = first_column.get('data')

        self.assertIsNotNone(column_data)
        self.assertDictEqual(expected_column_data, column_data)

    def test_enrich_entity_classifications_should_add_classification_data(
            self):
        fetched_entities_dict = {
            'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c': {
                'guid': 'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c',
                'data': {}
            }
        }

        searched_entries = {
            'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c': {
                'guid': 'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c',
                'data': {
                    'typeName': 'DB',
                    'attributes': {
                        'owner': 'John ETL',
                        'createTime': 1589983828059,
                        'name': 'Sales',
                        'description': 'sales database'
                    },
                    'guid': 'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c',
                    'status': 'ACTIVE',
                    'displayText': 'Sales',
                    'classificationNames': ['PII', 'PHI']
                }
            }
        }

        entity_classifications = [{
            "typeName": "PII",
            "entityGuid": "2286a6bd-4b06-4f7c-a666-920d774b040e",
            "propagate": True
        }, {
            "typeName": "PHI",
            "entityGuid": "c8eb658b-15ef-4568-bff9-6b1d1962a160",
            "propagate": True
        }]

        self.assertIsNone(
            fetched_entities_dict['c0ebcb5e-21f8-424d-bf11-f39bb943ae2c'].get(
                'classifications'))

        self.__apache_atlas_facade.\
            fetch_entity_classifications.return_value = entity_classifications

        self.__enricher.enrich_entity_classifications(fetched_entities_dict,
                                                      searched_entries)

        classifications = fetched_entities_dict[
            'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c']['classifications']

        self.assertIsNotNone(
            fetched_entities_dict['c0ebcb5e-21f8-424d-bf11-f39bb943ae2c']
            ['classifications'])
        self.assertEqual(entity_classifications, classifications)
