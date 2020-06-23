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

from google.datacatalog_connectors.apache_atlas.scrape import \
    metadata_event_enricher


class MetadataEventEnricherTest(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    def setUp(self):
        apache_atlas_facade = mock.MagicMock()

        self.__event_enricher = metadata_event_enricher.MetadataEventEnricher(
            apache_atlas_facade)
        # Shortcut for the object assigned
        # to self.__scrape.__apache_atlas_facade
        self.__apache_atlas_facade = apache_atlas_facade

    def test_enrich_entity_types_relationships_should_add_column_data(self):
        entity_types_dict = {
            'Column': {
                'name': 'Column',
                'data': {
                    'category': 'ENTITY',
                    'guid': '4f903d41-3cf6-41bd-bc8c-0a78aee39b17',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806198,
                    'updateTime': 1589983806198,
                    'version': 1,
                    'name': 'Column',
                    'description': 'Column',
                },
                'entities': {}
            }
        }

        column_entity = {
            'c53f1ab7-970f-4199-bc9a-899291c93622': {
                'guid': 'c53f1ab7-970f-4199-bc9a-899291c93622',
                'data': {
                    'typeName': 'Column',
                    'attributes': {
                        'dataType': 'int',
                        'name': 'dayOfYear',
                        'comment': 'day Of Year',
                        'table': None
                    },
                    'guid': 'c53f1ab7-970f-4199-bc9a-899291c93622',
                    'status': 'ACTIVE',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983834061,
                    'updateTime': 1589983834061,
                    'version': 0
                }
            }
        }

        self.__apache_atlas_facade.fetch_entities.return_value = column_entity

        column_classfiication = {
            'typeName': 'PII',
            'entityGuid': '2286a6bd-4b06-4f7c-a666-920d774b040e',
            'propagate': True
        }

        self.__apache_atlas_facade.fetch_entity_classifications.\
            return_value = [column_classfiication]

        entities = {
            'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c': {
                'data': {
                    'typeName': 'Table',
                    'attributes': {
                        'owner':
                            'Joe',
                        'temporary':
                            False,
                        'lastAccessTime':
                            1589983835754,
                        'qualifiedName':
                            'sales_fact',
                        'columns': [{
                            'guid': 'c53f1ab7-970f-4199-bc9a-899291c93622',
                            'typeName': 'Column'
                        }]
                    }
                },
            }
        }

        self.__event_enricher.enrich_entity_types_relationships(
            entities, entity_types_dict)

        self.assertDictEqual(entity_types_dict['Column']['entities'],
                             column_entity)

    def test_enrich_entities_attributes_and_classifications_should_reutrn(
            self):
        expected_entities = {
            'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c': {
                'data': {
                    'typeName': 'Table',
                    'attributes': {
                        'owner':
                            'Joe',
                        'temporary':
                            False,
                        'lastAccessTime':
                            1589983835754,
                        'qualifiedName':
                            'sales_fact',
                        'columns': [{
                            'guid': 'c53f1ab7-970f-4199-bc9a-899291c93622',
                            'typeName': 'Column'
                        }]
                    }
                },
            }
        }

        self.__apache_atlas_facade.fetch_entities.return_value =\
            expected_entities

        classfiication = {
            'typeName': 'PII',
            'entityGuid': '2286a6bd-4b06-4f7c-a666-920d774b040e',
            'propagate': True
        }

        self.__apache_atlas_facade.fetch_entity_classifications.\
            return_value = [classfiication]

        guids = ['c0ebcb5e-21f8-424d-bf11-f39bb943ae2c']

        returned_entities = self.__event_enricher.\
            enrich_entities_attributes_and_classifications(guids)

        self.assertDictEqual(expected_entities, returned_entities)
