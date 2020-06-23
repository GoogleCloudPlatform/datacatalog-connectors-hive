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

import unittest
from unittest.mock import patch

from google.datacatalog_connectors.commons_test import utils

from google.datacatalog_connectors.apache_atlas import scrape


class ApacheAtlasFacadeTestCase(unittest.TestCase):

    @patch('atlasclient.client.Atlas')
    def setUp(self, atlas_client):
        self.__atlas_facade = scrape.ApacheAtlasFacade({
            'host': 'my_host',
            'port': 'my_port',
            'user': 'my_user',
            'pass': 'my_pass',
        })
        # Shortcut for the object assigned
        # to self.__atlas_facade.__apache_atlas
        self.__atlas_client = atlas_client.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__atlas_facade.__dict__
        self.assertIsNotNone(attrs['_ApacheAtlasFacade__apache_atlas'])

    def test_get_admin_metrics_should_succeed(self):
        metric = utils.MockedObject()
        metric.entity = {
            'entityActive': {
                'AtlasGlossary': 2,
                'StorageDesc': 1,
                'Table': 8,
                'Column': 17,
                'AtlasGlossaryTerm': 44,
                'AtlasGlossaryCategory': 23,
                'View': 2,
                'DB': 3,
                'LoadProcess': 3
            },
            'entityDeleted': {
                'hbase_table': 2
            }
        }

        metric.general = {
            'collectionTime': 1590773837477,
            'entityCount': 105,
            'tagCount': 13,
            'typeUnusedCount': 47,
            'typeCount': 138
        }

        self.__atlas_client.admin_metrics = [metric]

        response = self.__atlas_facade.get_admin_metrics()[0]

        self.assertDictEqual(metric.entity['entityActive'],
                             response['entityActive'])
        self.assertDictEqual(metric.entity['entityDeleted'],
                             response['entityDeleted'])
        self.assertDictEqual(metric.general, response['generalMetrics'])

    def test_get_typedefs(self):
        self.__atlas_client.typedefs = True

        # Just assert it was called, since typedefs is a lazy return.
        self.assertEqual(True, self.__atlas_facade.get_typedefs())

    def test_search_entities_from_entity_type_should_return(self):
        search_result = utils.MockedObject()
        search_results = [search_result]

        table_1 = utils.MockedObject()
        table_1._data = {
            'typeName': 'Table',
            'attributes': {
                'owner': 'Joe',
                'createTime': 1589983835754,
                'qualifiedName': 'sales_fact',
                'name': 'sales_fact',
                'description': 'sales fact table'
            },
            'guid': '2286a6bd-4b06-4f7c-a666-920d774b040e',
            'status': 'ACTIVE',
            'displayText': 'sales_fact',
            'classificationNames': ['Fact']
        }

        table_2 = utils.MockedObject()
        table_2._data = {
            'typeName': 'Table',
            'attributes': {
                'owner': 'Tim ETL',
                'createTime': 1589983847754,
                'qualifiedName': 'log_fact_daily_mv',
                'name': 'log_fact_daily_mv',
                'description': 'log fact daily materialized '
                               'view'
            },
            'guid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
            'status': 'ACTIVE',
            'displayText': 'log_fact_daily_mv',
            'classificationNames': ['Log Data']
        }

        search_result.entities = [table_1, table_2]

        self.__atlas_client.search_dsl.side_effect = [search_results, []]

        response = self.__atlas_facade.search_entities_from_entity_type(
            'Table')

        self.assertDictEqual(table_1._data, response[0]._data)
        self.assertDictEqual(table_2._data, response[1]._data)
        self.assertEqual(2, len(response))

    def test_search_entities_from_entity_type_paged_should_return(self):
        search_result = utils.MockedObject()

        table_1 = utils.MockedObject()
        table_1._data = {
            'typeName': 'Table',
            'attributes': {
                'owner': 'Joe',
                'createTime': 1589983835754,
                'qualifiedName': 'sales_fact',
                'name': 'sales_fact',
                'description': 'sales fact table'
            },
            'guid': '2286a6bd-4b06-4f7c-a666-920d774b040e',
            'status': 'ACTIVE',
            'displayText': 'sales_fact',
            'classificationNames': ['Fact']
        }

        search_result.entities = [table_1]
        search_results = [search_result]

        search_result_2 = utils.MockedObject()

        table_2 = utils.MockedObject()
        table_2._data = {
            'typeName': 'Table',
            'attributes': {
                'owner': 'Tim ETL',
                'createTime': 1589983847754,
                'qualifiedName': 'log_fact_daily_mv',
                'name': 'log_fact_daily_mv',
                'description': 'log fact daily materialized '
                               'view'
            },
            'guid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
            'status': 'ACTIVE',
            'displayText': 'log_fact_daily_mv',
            'classificationNames': ['Log Data']
        }

        search_result_2.entities = [table_2]
        search_results_2 = [search_result_2]

        self.__atlas_client.search_dsl.side_effect = [
            search_results, search_results_2, []
        ]

        response = self.__atlas_facade.search_entities_from_entity_type(
            'Table')

        self.assertDictEqual(table_1._data, response[0]._data)
        self.assertDictEqual(table_2._data, response[1]._data)
        self.assertEqual(2, len(response))

    def test_search_entities_from_non_existent_entity_type_should_return_empty(
            self):
        search_result = utils.MockedObject()

        table_1 = utils.MockedObject()
        table_1._data = {
            'typeName': 'Table',
            'attributes': {
                'owner': 'Joe',
                'createTime': 1589983835754,
                'qualifiedName': 'sales_fact',
                'name': 'sales_fact',
                'description': 'sales fact table'
            },
            'guid': '2286a6bd-4b06-4f7c-a666-920d774b040e',
            'status': 'ACTIVE',
            'displayText': 'sales_fact',
            'classificationNames': ['Fact']
        }

        search_result.entities = [table_1]
        search_results = [search_result]

        search_result_2 = utils.MockedObject()

        table_2 = utils.MockedObject()
        table_2._data = {
            'typeName': 'Table',
            'attributes': {
                'owner': 'Tim ETL',
                'createTime': 1589983847754,
                'qualifiedName': 'log_fact_daily_mv',
                'name': 'log_fact_daily_mv',
                'description': 'log fact daily materialized '
                               'view'
            },
            'guid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
            'status': 'ACTIVE',
            'displayText': 'log_fact_daily_mv',
            'classificationNames': ['Log Data']
        }

        search_result_2.entities = [table_2]
        search_results_2 = [search_result_2]

        self.__atlas_client.search_dsl.side_effect = [
            search_results, search_results_2, []
        ]

        response = self.__atlas_facade.search_entities_from_entity_type(
            'WrongType')

        self.assertEqual(0, len(response))

    def test_search_entities_no_return_should_return_empty(self):
        self.__atlas_client.search_dsl.return_value = []

        response = self.__atlas_facade.search_entities_from_entity_type(
            'Table')

        self.assertEqual(0, len(response))

    def test_fetch_entities_should_return(self):
        table_1 = utils.MockedObject()
        guid = '2286a6bd-4b06-4f7c-a666-920d774b040e'
        table_1.guid = guid
        table_1._data = {
            'typeName': 'Table',
            'attributes': {
                'owner': 'Joe',
                'temporary': False,
                'lastAccessTime': 1589983835754,
                'replicatedTo': None,
                'replicatedFrom': None,
                'qualifiedName': 'sales_fact',
                'columns': [{
                    'guid': 'c53f1ab7-970f-4199-bc9a-899291c93622',
                    'typeName': 'Column'
                }, {
                    'guid': 'e5188d7b-916e-4420-b02e-8eac75b17a68',
                    'typeName': 'Column'
                }, {
                    'guid': '84d83e16-e767-46f9-8165-db7e3a7a3762',
                    'typeName': 'Column'
                }, {
                    'guid': '909805b1-59f0-44db-b0c8-b6b0c384d2c5',
                    'typeName': 'Column'
                }],
                'description': 'sales fact table',
                'viewExpandedText': None,
                'sd': {
                    'guid': 'a0989a5b-19aa-4ac4-92db-e2449554c799',
                    'typeName': 'StorageDesc'
                },
                'tableType': 'Managed',
                'createTime': 1589983835754,
                'name': 'sales_fact',
                'additionalProperties': None,
                'db': {
                    'guid': 'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c',
                    'typeName': 'DB'
                },
                'retention': 1589983835754,
                'viewOriginalText': None
            },
            'guid': guid,
            'status': 'ACTIVE',
            'createdBy': 'admin',
            'updatedBy': 'admin',
            'createTime': 1589983835765,
            'updateTime': 1589983850688,
            'version': 0,
            'relationshipAttributes': {
                'schema': [],
                'inputToProcesses': [{
                    'guid': '2aa15a65-ae8e-4e48-9b77-2ecb049f82b4',
                    'typeName': 'LoadProcess',
                    'displayText': 'loadSalesDaily',
                    'relationshipGuid': '95d240a4-009b-48c8-b1d3-66703720f60a',
                    'relationshipStatus': 'ACTIVE',
                    'relationshipAttributes': {
                        'typeName': 'dataset_process_inputs'
                    }
                }],
                'relatedFromObjectAnnotations': [],
                'resourceListAnchors': [],
                'relatedToObjectAnnotations': None,
                'supportingResources': [],
                'meanings': [],
                'outputFromProcesses': []
            }
        }

        collection_item = utils.MockedObject()
        entities = [table_1]
        collection_item.entities = lambda: entities

        bulk_collection = [collection_item]

        self.__atlas_client.entity_bulk.return_value = bulk_collection

        response = self.__atlas_facade.fetch_entities([guid])

        self.assertEqual(1, len(response))
        self.assertEqual(table_1.guid, response[guid]['guid'])
        self.assertDictEqual(table_1._data, response[guid]['data'])

    def test_fetch_entity_classifications_should_return(self):
        returned_entity = utils.MockedObject()
        cursor = utils.MockedObject()
        cursor._data = {
            'list': [{
                'typeName': 'Log Data',
                'entityGuid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
                'propagate': True
            }, {
                'typeName': 'ETL',
                'entityGuid': 'e73656c9-da05-4db7-9b88-e3669c1a98eb',
                'propagate': True
            }]
        }
        returned_entity.classifications = iter([cursor])

        self.__atlas_client.entity_guid.return_value = returned_entity

        response = self.__atlas_facade.fetch_entity_classifications(
            ['30cfc9fc-aec4-4017-b649-f1f92351a727'])

        self.assertEqual(2, len(response))

    def test_fetch_entity_classifications_error_should_not_raise_it(self):
        returned_entity = utils.MockedObject()
        cursor = utils.MockedObject()
        cursor._data = {
            'list': [{
                'typeName': 'Log Data',
                'entityGuid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
                'propagate': True
            }, {
                'typeName': 'ETL',
                'entityGuid': 'e73656c9-da05-4db7-9b88-e3669c1a98eb',
                'propagate': True
            }]
        }
        returned_entity.classifications = iter([])

        self.__atlas_client.entity_guid.return_value = returned_entity

        response = self.__atlas_facade.fetch_entity_classifications(
            ['30cfc9fc-aec4-4017-b649-f1f92351a727'])

        self.assertIsNone(response)

    def test_fetch_entity_classifications_with_duplicates_should_return(self):
        returned_entity = utils.MockedObject()
        cursor = utils.MockedObject()
        cursor._data = {
            'list': [{
                'typeName': 'Log Data',
                'entityGuid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
                'propagate': True
            }, {
                'typeName': 'ETL',
                'entityGuid': 'e73656c9-da05-4db7-9b88-e3669c1a98eb',
                'propagate': True
            }, {
                'typeName': 'ETL',
                'entityGuid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
                'propagate': True
            }]
        }
        returned_entity.classifications = iter([cursor])

        self.__atlas_client.entity_guid.return_value = returned_entity

        response = self.__atlas_facade.fetch_entity_classifications(
            '30cfc9fc-aec4-4017-b649-f1f92351a727')

        self.assertEqual(2, len(response))
        self.assertEqual('30cfc9fc-aec4-4017-b649-f1f92351a727',
                         response[1]['entityGuid'])
