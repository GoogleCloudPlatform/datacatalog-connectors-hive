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

from google.datacatalog_connectors.apache_atlas.prepare import \
    datacatalog_entry_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'

    def setUp(self):
        self.__factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            'test-project', 'test-location', 'test-entry-group', 'test-system',
            'https://test.server.com')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual('test-project',
                         attrs['_DataCatalogEntryFactory__project_id'])
        self.assertEqual('test-location',
                         attrs['_DataCatalogEntryFactory__location_id'])
        self.assertEqual('test-entry-group',
                         attrs['_DataCatalogEntryFactory__entry_group_id'])
        self.assertEqual(
            'test-system',
            attrs['_DataCatalogEntryFactory__user_specified_system'])
        self.assertEqual('https://test.server.com',
                         attrs['_DataCatalogEntryFactory__instance_url'])
        self.assertEqual('test.server.com',
                         attrs['_DataCatalogEntryFactory__server_id'])

    def test_make_entry_for_entity_should_set_all_available_fields(self):
        entity = self.__create_entity_dict()

        entry_id, entry = self.__factory.make_entry_for_entity(entity)
        self.assertEqual('table_2286a6bd_4b06_4f7c_a666_920d774b040e',
                         entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/'
            'entries/table_2286a6bd_4b06_4f7c_a666_920d774b040e', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('table', entry.user_specified_type)
        self.assertEqual('sales_fact', entry.display_name)
        self.assertEqual(
            'https://test.server.com/table/'
            'table_2286a6bd_4b06_4f7c_a666_920d774b040e',
            entry.linked_resource)

        self.assertEqual(
            1589983836.0,
            entry.source_system_timestamps.create_time.timestamp())
        self.assertEqual(
            1589983851.0,
            entry.source_system_timestamps.update_time.timestamp())

        columns = entry.schema.columns
        self.assertEqual(4, len(columns))
        column_1 = columns[0]
        self.assertEqual('time_id', column_1.column)
        self.assertEqual('time id', column_1.description)
        self.assertEqual('int', column_1.type)

        column_2 = columns[1]
        self.assertEqual('product_id', column_2.column)
        self.assertEqual('product id', column_2.description)
        self.assertEqual('int', column_2.type)

        column_3 = columns[2]
        self.assertEqual('customer_id', column_3.column)
        self.assertEqual('customer id', column_3.description)
        self.assertEqual('int', column_3.type)

        column_4 = columns[3]
        self.assertEqual('sales', column_4.column)
        self.assertEqual('product id', column_4.description)
        self.assertEqual('double', column_4.type)

    def test_make_entry_for_entity_opt_fields_should_set_all_available_fields(
            self):
        entity = self.__create_entity_dict()

        entity['data']['attributes']['name'] = None
        entity['data']['attributes'][
            'location'] = 'my_entry_specified_location'
        entity['data']['updateTime'] = None

        entry_id, entry = self.__factory.make_entry_for_entity(entity)

        self.assertEqual('table_2286a6bd_4b06_4f7c_a666_920d774b040e',
                         entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/'
            'entries/table_2286a6bd_4b06_4f7c_a666_920d774b040e', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('table', entry.user_specified_type)
        self.assertEqual('table_2286a6bd_4b06_4f7c_a666_920d774b040e',
                         entry.display_name)
        self.assertEqual('//my_entry_specified_location',
                         entry.linked_resource)

        self.assertEqual(
            1589983836.0,
            entry.source_system_timestamps.create_time.timestamp())
        self.assertEqual(
            1589983836.0,
            entry.source_system_timestamps.update_time.timestamp())

        columns = entry.schema.columns
        self.assertEqual(4, len(columns))
        column_1 = columns[0]
        self.assertEqual('time_id', column_1.column)
        self.assertEqual('time id', column_1.description)
        self.assertEqual('int', column_1.type)

        column_2 = columns[1]
        self.assertEqual('product_id', column_2.column)
        self.assertEqual('product id', column_2.description)
        self.assertEqual('int', column_2.type)

        column_3 = columns[2]
        self.assertEqual('customer_id', column_3.column)
        self.assertEqual('customer id', column_3.description)
        self.assertEqual('int', column_3.type)

        column_4 = columns[3]
        self.assertEqual('sales', column_4.column)
        self.assertEqual('product id', column_4.description)
        self.assertEqual('double', column_4.type)

    @classmethod
    def __create_entity_dict(cls):
        return {
            'guid':
                '2286a6bd-4b06-4f7c-a666-920d774b040e',
            'data': {
                'typeName': 'Table',
                'attributes': {
                    'owner': 'Joe',
                    'temporary': False,
                    'lastAccessTime': 1589983835754,
                    'qualifiedName': 'sales_fact',
                    'columns': [{
                        'guid': 'c53f1ab7-970f-4199-bc9a-899291c93622',
                        'typeName': 'Column',
                        'data': {
                            'typeName': 'Column',
                            'attributes': {
                                'dataType': 'int',
                                'name': 'time_id',
                                'comment': 'time id',
                                'table': None
                            },
                            'guid': 'c53f1ab7-970f-4199-bc9a-899291c93622',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983829550,
                            'updateTime': 1589983829550,
                            'version': 0
                        },
                        'classifications': None
                    }, {
                        'guid': 'e5188d7b-916e-4420-b02e-8eac75b17a68',
                        'typeName': 'Column',
                        'data': {
                            'typeName': 'Column',
                            'attributes': {
                                'dataType': 'int',
                                'name': 'product_id',
                                'comment': 'product id',
                                'table': None
                            },
                            'guid': 'e5188d7b-916e-4420-b02e-8eac75b17a68',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983829705,
                            'updateTime': 1589983829705,
                            'version': 0
                        },
                        'classifications': None
                    }, {
                        'guid':
                            '84d83e16-e767-46f9-8165-db7e3a7a3762',
                        'typeName':
                            'Column',
                        'data': {
                            'typeName': 'Column',
                            'attributes': {
                                'dataType': 'int',
                                'name': 'customer_id',
                                'comment': 'customer id',
                                'table': None
                            },
                            'guid': '84d83e16-e767-46f9-8165-db7e3a7a3762',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983829845,
                            'updateTime': 1589983829845,
                            'version': 0
                        },
                        'classifications': [{
                            'typeName':
                                'PII',
                            'entityGuid':
                                '84d83e16-e767-46f9-8165-db7e3a7a3762',
                            'propagate':
                                True
                        }]
                    }, {
                        'guid':
                            '909805b1-59f0-44db-b0c8-b6b0c384d2c5',
                        'typeName':
                            'Column',
                        'data': {
                            'typeName': 'Column',
                            'attributes': {
                                'dataType': 'double',
                                'name': 'sales',
                                'comment': 'product id',
                                'table': None
                            },
                            'guid': '909805b1-59f0-44db-b0c8-b6b0c384d2c5',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983832013,
                            'updateTime': 1589983832013,
                            'version': 0
                        },
                        'classifications': [{
                            'typeName':
                                'Metric',
                            'entityGuid':
                                '909805b1-59f0-44db-b0c8-b6b0c384d2c5',
                            'propagate':
                                True
                        }]
                    }],
                    'description': 'sales fact table',
                    'viewExpandedText': None,
                    'sd': {
                        'guid': 'a0989a5b-19aa-4ac4-92db-e2449554c799',
                        'typeName': 'StorageDesc',
                        'data': {
                            'typeName': 'StorageDesc',
                            'attributes': {
                                'location':
                                    'hdfs://host:8000/apps/warehouse/sales',
                                'compressed':
                                    True,
                                'inputFormat':
                                    'TextInputFormat',
                                'outputFormat':
                                    'TextOutputFormat',
                                'table':
                                    None
                            },
                            'guid': 'a0989a5b-19aa-4ac4-92db-e2449554c799',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983829382,
                            'updateTime': 1589983849245,
                            'version': 0
                        },
                        'classifications': None
                    },
                    'tableType': 'Managed',
                    'createTime': 1589983835754,
                    'name': 'sales_fact',
                    'db': {
                        'guid': 'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c',
                        'typeName': 'DB',
                        'data': {
                            'typeName': 'DB',
                            'attributes': {
                                'owner': 'John ETL',
                                'createTime': 1589983828059,
                                'name': 'Sales',
                                'description': 'sales database',
                                'locationUri': None
                            },
                            'guid': 'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983828109,
                            'updateTime': 1589983847367,
                            'version': 0
                        },
                        'classifications': None
                    },
                    'retention': 1589983835754,
                    'viewOriginalText': None
                },
                'guid': '2286a6bd-4b06-4f7c-a666-920d774b040e',
                'status': 'ACTIVE',
                'createdBy': 'admin',
                'updatedBy': 'admin',
                'createTime': 1589983835765,
                'updateTime': 1589983850688,
                'version': 0,
                'relationshipAttributes': {
                    'schema': [],
                    'inputToProcesses': [{
                        'guid':
                            '2aa15a65-ae8e-4e48-9b77-2ecb049f82b4',
                        'typeName':
                            'LoadProcess',
                        'displayText':
                            'loadSalesDaily',
                        'relationshipGuid':
                            '95d240a4-009b-48c8-b1d3-66703720f60a',
                        'relationshipStatus':
                            'ACTIVE',
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
            },
            'classifications': [{
                'typeName': 'Fact',
                'entityGuid': '2286a6bd-4b06-4f7c-a666-920d774b040e',
                'propagate': True
            }]
        }
