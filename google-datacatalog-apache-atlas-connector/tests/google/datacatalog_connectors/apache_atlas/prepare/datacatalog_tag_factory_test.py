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
    datacatalog_tag_factory


class DataCatalogTagFactoryTest(unittest.TestCase):
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'

    def setUp(self):
        self.__factory = datacatalog_tag_factory.DataCatalogTagFactory(
            'test-project', 'test-location', 'https://test.server.com')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__
        self.assertEqual('test-project',
                         attrs['_DataCatalogTagFactory__project_id'])
        self.assertEqual('test-location',
                         attrs['_DataCatalogTagFactory__location_id'])
        self.assertEqual('https://test.server.com',
                         attrs['_DataCatalogTagFactory__instance_url'])

    def test_make_tag_for_entity_should_set_all_available_fields(self):
        entity = {
            'guid': '73bd400b-3698-4b74-a1aa-06f694c8e156',
            'data': {
                'typeName': 'DB',
                'attributes': {
                    'owner': 'Jane BI',
                    'createTime': 1589983828968,
                    'name': 'Reporting',
                    'description': 'reporting database',
                    'abTest': True,
                    'sizeBytes': 560000003,
                    'locationUri': None,
                    'weirdField': '1234324',
                    'governacenStatus': 'CHECKED'
                },
                'guid': '73bd400b-3698-4b74-a1aa-06f694c8e156',
                'status': 'ACTIVE',
                'createdBy': 'admin',
                'updatedBy': 'admin',
                'createTime': 1589983828984,
                'updateTime': 1589983850259,
                'version': 0
            }
        }

        entity_types_dict = {
            'DB': {
                'name': 'DB',
                'data': {
                    'category': 'ENTITY',
                    'guid': 'b11e24ca-c0f4-4754-92f7-d3a5febd2d2c',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806140,
                    'updateTime': 1589983806140,
                    'version': 1,
                    'name': 'DB',
                    'description': 'DB',
                    'typeVersion': '1.0',
                    'attributeDefs': [{
                        'name': 'abTest',
                        'typeName': 'boolean'
                    }, {
                        'name': 'sizeBytes',
                        'typeName': 'double'
                    }, {
                        'name': 'weirdField',
                        'typeName': 'WEIRD_UNDEFINED_TYPE'
                    }, {
                        'name': 'governacenStatus',
                        'typeName': 'GovernanceClassificationStatus'
                    }],
                    'superTypes': []
                },
                'superTypes': [],
                'entities': {}
            }
        }

        enum_types_dict = {
            'GovernanceClassificationStatus': {
                'data': {
                    'elementDefs': [{
                        'value': 'UNCHECKED'
                    }, {
                        'value': 'CHECKED'
                    }]
                }
            }
        }

        tag = self.__factory.make_tag_for_entity(entity, entity_types_dict,
                                                 enum_types_dict)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_db_1', tag.template)

        self.assertEqual(True, tag.fields['db'].bool_value)
        self.assertEqual('73bd400b-3698-4b74-a1aa-06f694c8e156',
                         tag.fields['guid'].string_value)
        self.assertEqual('Reporting', tag.fields['name'].string_value)
        self.assertEqual('Jane BI', tag.fields['owner'].string_value)
        self.assertEqual('reporting database',
                         tag.fields['description'].string_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)
        self.assertEqual('1589983828968',
                         tag.fields['createtime'].string_value)
        self.assertEqual(True, tag.fields['abtest'].bool_value)
        self.assertEqual(560000003, tag.fields['sizebytes'].double_value)
        self.assertEqual('1234324', tag.fields['weirdfield'].string_value)
        self.assertEqual(
            'CHECKED', tag.fields['governacenstatus'].enum_value.display_name)

    def test_make_tag_for_table_entity_should_set_all_available_fields(self):
        entity = self.__create_table_entity_dict()

        entity_types_dict = {
            'Table': {
                'name': 'Table',
                'data': {
                    'category': 'ENTITY',
                    'guid': 'b11e24ca-c0f4-4754-92f7-d3a5febd2d2c',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806140,
                    'updateTime': 1589983806140,
                    'version': 1,
                    'name': 'Table',
                    'description': 'Table',
                    'typeVersion': '1.0',
                    'attributeDefs': [],
                    'superTypes': []
                },
                'superTypes': [],
                'entities': {}
            }
        }

        tag = self.__factory.make_tag_for_entity(entity, entity_types_dict, {})

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_table_1', tag.template)

        self.assertEqual(True, tag.fields['table'].bool_value)
        self.assertEqual('22fc6dcd-ff53-41ae-878e-526e41d6035f',
                         tag.fields['guid'].string_value)
        self.assertEqual('fetl', tag.fields['owner'].string_value)
        self.assertEqual('customer_dim',
                         tag.fields['qualifiedname'].string_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)
        self.assertEqual('customer dimension table',
                         tag.fields['description'].string_value)
        self.assertEqual('Sales', tag.fields['db_name'].string_value)
        self.assertEqual('c0ebcb5e-21f8-424d-bf11-f39bb943ae2c',
                         tag.fields['db_guid'].string_value)
        self.assertEqual('a0989a5b-19aa-4ac4-92db-e2449554c799',
                         tag.fields['sd_guid'].string_value)
        self.assertEqual('External', tag.fields['tabletype'].string_value)
        self.assertEqual(
            "{\'guid\': \'c0ebcb5e-21f8-424d-bf11-f39bb943ae2c\',"
            " \'typeName\': \'DB\', \'name\': \'Sales\'}",
            tag.fields['db'].string_value)
        self.assertEqual(
            "{\'guid\': \'a0989a5b-19aa-4ac4-92db-e2449554c799\',"
            " \'typeName\': \'StorageDesc\', \'name\': \'\'}",
            tag.fields['sd'].string_value)

    def test_make_tag_for_view_entity_should_set_all_available_fields(self):
        entity = self.__create_view_entity_dict()

        entity_types_dict = {
            'View': {
                'name': 'View',
                'data': {
                    'category': 'ENTITY',
                    'guid': 'b11e24ca-c0f4-4754-92f7-d3a5febd2d2c',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806140,
                    'updateTime': 1589983806140,
                    'version': 1,
                    'name': 'View',
                    'description': 'View',
                    'typeVersion': '1.0',
                    'attributeDefs': [],
                    'superTypes': []
                },
                'superTypes': [],
                'entities': {}
            }
        }

        tag = self.__factory.make_tag_for_entity(entity, entity_types_dict, {})

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_view_1', tag.template)

        self.assertEqual(True, tag.fields['view'].bool_value)
        self.assertEqual('55a5f51f-1b21-4742-9059-818b7531df85',
                         tag.fields['guid'].string_value)
        self.assertIsNone(tag.fields.get('owner'))
        self.assertEqual('customer_dim',
                         tag.fields['input_tables_names'].string_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)
        self.assertEqual('customer_dim_view',
                         tag.fields['qualifiedname'].string_value)
        self.assertEqual('Reporting', tag.fields['db_name'].string_value)
        self.assertEqual('73bd400b-3698-4b74-a1aa-06f694c8e156',
                         tag.fields['db_guid'].string_value)
        self.assertEqual(
            "{\'guid\': \'73bd400b-3698-4b74-a1aa-06f694c8e156\',"
            " \'typeName\': \'DB\', \'name\': \'Reporting\'}",
            tag.fields['db'].string_value)

    def test_make_tag_for_load_process_entity_should_set_all_available_fields(
            self):
        entity = self.__create_load_process_entity_dict()

        entity_types_dict = {
            'LoadProcess': {
                'name': 'LoadProcess',
                'data': {
                    'category': 'ENTITY',
                    'guid': 'b11e24ca-c0f4-4754-92f7-d3a5febd2d2c',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806140,
                    'updateTime': 1589983806140,
                    'version': 1,
                    'name': 'LoadProcess',
                    'description': 'LoadProcess',
                    'typeVersion': '1.0',
                    'attributeDefs': [],
                    'superTypes': []
                },
                'superTypes': [],
                'entities': {}
            }
        }

        tag = self.__factory.make_tag_for_entity(entity, entity_types_dict, {})

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_loadprocess_1',
            tag.template)

        self.assertEqual(True, tag.fields['loadprocess'].bool_value)
        self.assertEqual('e73656c9-da05-4db7-9b88-e3669c1a98eb',
                         tag.fields['guid'].string_value)
        self.assertIsNone(tag.fields.get('owner'))
        self.assertEqual('log_fact_daily_mv',
                         tag.fields['inputs_names'].string_value)
        self.assertEqual(
            "[{\'guid\': \'30cfc9fc-aec4-4017-b649-f1f92351a727\', "
            "\'typeName\': \'Table\', \'name\': \'log_fact_daily_mv\'}]",
            tag.fields['inputs'].string_value)
        self.assertEqual('logging_fact_monthly_mv',
                         tag.fields['outputs_names'].string_value)
        self.assertEqual(
            "[{\'guid\': \'15f5bf50-5a39-4994-9cba-6c01d016f52f\', "
            "\'typeName\': \'Table\', \'name\': "
            "\'logging_fact_monthly_mv\'}]",
            tag.fields['outputs'].string_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)
        self.assertEqual('loadLogsMonthly',
                         tag.fields['qualifiedname'].string_value)
        self.assertEqual('create table as select ',
                         tag.fields['querytext'].string_value)
        self.assertEqual('id', tag.fields['queryid'].string_value)

    def test_make_tag_for_classification_column_should_set_all_available_fields(  # noqa: E501
            self):
        entity_classification = {
            'typeName': 'Metric',
            'entityGuid': '909805b1-59f0-44db-b0c8-b6b0c384d2c5',
            'propagate': True
        }
        classifications = {
            'Metric': {
                'name': 'Metric',
                'guid': 'f71b2d91-eb59-4cc6-99c3-e6671f9b8e55',
                'data': {
                    'category': 'CLASSIFICATION',
                    'guid': 'f71b2d91-eb59-4cc6-99c3-e6671f9b8e55',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806121,
                    'updateTime': 1589983806121,
                    'version': 1,
                    'name': 'Metric',
                    'description': 'Metric Classification',
                    'typeVersion': '1.0',
                    'attributeDefs': [],
                    'superTypes': []
                }
            }
        }
        column_name = 'test-column'

        tag = self.__factory.make_tag_for_classification(
            entity_classification, classifications, {}, column_name)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_classification_metric_1', tag.template)

        self.assertEqual(True, tag.fields['metric'].bool_value)
        self.assertEqual('test_column', tag.column)

    def test_make_tag_for_classification_should_set_all_available_fields(
            self):  # noqa: E501
        entity_classification = {
            'typeName': 'Metric',
            'entityGuid': '909805b1-59f0-44db-b0c8-b6b0c384d2c5',
            'propagate': True
        }
        classifications = {
            'Metric': {
                'name': 'Metric',
                'guid': 'f71b2d91-eb59-4cc6-99c3-e6671f9b8e55',
                'data': {
                    'category': 'CLASSIFICATION',
                    'guid': 'f71b2d91-eb59-4cc6-99c3-e6671f9b8e55',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806121,
                    'updateTime': 1589983806121,
                    'version': 1,
                    'name': 'Metric',
                    'description': 'Metric Classification',
                    'typeVersion': '1.0',
                    'attributeDefs': [],
                    'superTypes': []
                }
            }
        }

        tag = self.__factory.make_tag_for_classification(
            entity_classification, classifications, {})

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_classification_metric_1', tag.template)

        self.assertEqual(True, tag.fields['metric'].bool_value)
        self.assertEqual('', tag.column)

    def test_make_tag_for_classification_with_attributes_should_set_all_available_fields(  # noqa: E501
            self):
        entity_classification = {
            'typeName': 'Confidentiality',
            'attributes': {
                'notes': 'notes were updated with additional info',
                'steward': 'richard updated',
                'level': 30,
                'confidence': 10,
                'source': 'governance department updated',
                'types': [{
                    'name': 'Type 1'
                }, {
                    'name': 'Type 2'
                }],
                'status': 'Proposed'
            },
            'entityGuid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
            'propagate': True,
            'validityPeriods': []
        }
        classifications = {
            'Confidentiality': {
                'name': 'Confidentiality',
                'guid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
                'data': {
                    'category': 'CLASSIFICATION',
                    'guid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983806121,
                    'updateTime': 1589983806121,
                    'version': 1,
                    'name': 'Metric',
                    'description': 'Confidentiality Classification',
                    'typeVersion': '1.0',
                    'attributeDefs': [],
                    'superTypes': []
                }
            }
        }

        tag = self.__factory.make_tag_for_classification(
            entity_classification, classifications, {})

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_classification_confidentiality_1',
            tag.template)

        self.assertEqual(True, tag.fields['confidentiality'].bool_value)
        self.assertEqual('10', tag.fields['confidence'].string_value)
        self.assertEqual('Proposed', tag.fields['status'].string_value)
        self.assertEqual('notes were updated with additional info',
                         tag.fields['notes'].string_value)
        self.assertEqual("[{\'name\': \'Type 1\'}, {\'name\': \'Type 2\'}]",
                         tag.fields['types'].string_value)
        self.assertEqual('governance department updated',
                         tag.fields['source'].string_value)
        self.assertEqual('richard updated', tag.fields['steward'].string_value)
        self.assertEqual('30', tag.fields['level'].string_value)

    def test_make_tag_for_column_ref_should_set_all_available_fields(self):
        tag = self.__factory.make_tag_for_column_ref('1', 'test-column')

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_column_ref', tag.template)

        self.assertEqual('1', tag.fields['column_guid'].string_value)
        self.assertEqual('test_column', tag.column)

    @classmethod
    def __create_table_entity_dict(cls):
        return {
            'guid':
                '22fc6dcd-ff53-41ae-878e-526e41d6035f',
            'data': {
                'typeName': 'Table',
                'attributes': {
                    'owner': 'fetl',
                    'temporary': False,
                    'lastAccessTime': 1589983846967,
                    'qualifiedName': 'customer_dim',
                    'columns': [{
                        'guid':
                            '4307bb52-599e-4e83-9e8c-3a6269dd3d04',
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
                            'guid': '4307bb52-599e-4e83-9e8c-3a6269dd3d04',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983834365,
                            'updateTime': 1589983834365,
                            'version': 0
                        },
                        'classifications': [{
                            'typeName':
                                'PII',
                            'entityGuid':
                                '4307bb52-599e-4e83-9e8c-3a6269dd3d04',
                            'propagate':
                                True
                        }, {
                            'typeName':
                                'Business Glossary',
                            'attributes': {
                                'level': '6',
                                'confidence': '5',
                                'classification': 'Imported',
                                'status': '3'
                            },
                            'entityGuid':
                                '4307bb52-599e-4e83-9e8c-3a6269dd3d04',
                            'propagate':
                                True,
                            'validityPeriods': []
                        }, {
                            'typeName':
                                'ETL',
                            'entityGuid':
                                '4307bb52-599e-4e83-9e8c-3a6269dd3d04',
                            'propagate':
                                True,
                            'validityPeriods': []
                        }, {
                            'typeName':
                                'Fact',
                            'entityGuid':
                                '4307bb52-599e-4e83-9e8c-3a6269dd3d04',
                            'propagate':
                                True,
                            'validityPeriods': []
                        }]
                    }, {
                        'guid':
                            'c786287c-f319-4bf5-a3bb-517337d76619',
                        'typeName':
                            'Column',
                        'data': {
                            'typeName': 'Column',
                            'attributes': {
                                'dataType': 'string',
                                'name': 'name',
                                'comment': 'customer name',
                                'table': None
                            },
                            'guid': 'c786287c-f319-4bf5-a3bb-517337d76619',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983834740,
                            'updateTime': 1589983834740,
                            'version': 0
                        },
                        'classifications': [{
                            'typeName':
                                'PII',
                            'entityGuid':
                                'c786287c-f319-4bf5-a3bb-517337d76619',
                            'propagate':
                                True
                        }]
                    }, {
                        'guid':
                            '559ef572-249a-465c-ba38-366d5062d291',
                        'typeName':
                            'Column',
                        'data': {
                            'typeName': 'Column',
                            'attributes': {
                                'dataType': 'string',
                                'name': 'address',
                                'comment': 'customer address',
                                'table': None
                            },
                            'guid': '559ef572-249a-465c-ba38-366d5062d291',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983835316,
                            'updateTime': 1589983835316,
                            'version': 0
                        },
                        'classifications': [{
                            'typeName':
                                'PII',
                            'entityGuid':
                                '559ef572-249a-465c-ba38-366d5062d291',
                            'propagate':
                                True
                        }]
                    }],
                    'description': 'customer dimension table',
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
                    'tableType': 'External',
                    'createTime': 1589983846967,
                    'name': 'customer_dim',
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
                    'retention': 1589983846967,
                    'viewOriginalText': None
                },
                'guid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
                'status': 'ACTIVE',
                'createdBy': 'admin',
                'updatedBy': 'admin',
                'createTime': 1589983846971,
                'updateTime': 1589983846971,
                'version': 0,
                'relationshipAttributes': {
                    'schema': [],
                    'inputToProcesses': [],
                    'relatedFromObjectAnnotations': [],
                    'resourceListAnchors': [],
                    'relatedToObjectAnnotations': None,
                    'supportingResources': [],
                    'meanings': [{
                        'guid':
                            '29ebe9a3-34ee-4f60-aa1d-8236e014a447',
                        'typeName':
                            'AtlasGlossaryTerm',
                        'displayText':
                            'Area',
                        'relationshipGuid':
                            '0f4869ad-9def-47d1-bbfa-60bcb101e4bf',
                        'relationshipStatus':
                            'ACTIVE',
                        'relationshipAttributes': {
                            'typeName': 'AtlasGlossarySemanticAssignment',
                            'attributes': {
                                'expression': None,
                                'createdBy': None,
                                'steward': None,
                                'confidence': None,
                                'description': None,
                                'source': None,
                                'status': None
                            }
                        }
                    }],
                    'outputFromProcesses': []
                }
            },
            'classifications': [{
                'typeName': 'Dimension',
                'entityGuid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
                'propagate': True
            }, {
                'typeName': 'Confidentiality',
                'attributes': {
                    'notes': 'notes were updated with additional info',
                    'steward': 'richard updated',
                    'level': 30,
                    'confidence': 10,
                    'source': 'governance department updated',
                    'status': 'Proposed'
                },
                'entityGuid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
                'propagate': True,
                'validityPeriods': []
            }]
        }

    @classmethod
    def __create_view_entity_dict(cls):
        return {
            'guid':
                '55a5f51f-1b21-4742-9059-818b7531df85',
            'data': {
                'typeName': 'View',
                'attributes': {
                    'owner': None,
                    'qualifiedName': 'customer_dim_view',
                    'name': 'customer_dim_view',
                    'description': None,
                    'inputTables': [{
                        'guid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
                        'typeName': 'Table',
                        'data': {
                            'typeName': 'Table',
                            'attributes': {
                                'owner': 'fetl',
                                'temporary': False,
                                'lastAccessTime': 1589983846967,
                                'qualifiedName': 'customer_dim',
                                'description': 'customer dimension table',
                                'viewExpandedText': None,
                                'tableType': 'External',
                                'createTime': 1589983846967,
                                'name': 'customer_dim',
                                'retention': 1589983846967,
                                'viewOriginalText': None
                            },
                            'guid': '22fc6dcd-ff53-41ae-878e-526e41d6035f',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983846971,
                            'updateTime': 1589983846971,
                            'version': 0,
                        }
                    }],
                    'db': {
                        'guid': '73bd400b-3698-4b74-a1aa-06f694c8e156',
                        'typeName': 'DB',
                        'data': {
                            'typeName': 'DB',
                            'attributes': {
                                'owner': 'Jane BI',
                                'createTime': 1589983828968,
                                'name': 'Reporting',
                                'description': 'reporting database',
                                'locationUri': None
                            },
                            'guid': '73bd400b-3698-4b74-a1aa-06f694c8e156',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983828984,
                            'updateTime': 1589983850259,
                            'version': 0
                        },
                        'classifications': None
                    }
                },
                'guid': '55a5f51f-1b21-4742-9059-818b7531df85',
                'status': 'ACTIVE',
                'createdBy': 'admin',
                'updatedBy': 'admin',
                'createTime': 1589983850259,
                'updateTime': 1589983850259,
                'version': 0,
                'relationshipAttributes': {
                    'schema': [],
                    'inputToProcesses': [],
                    'relatedFromObjectAnnotations': [],
                    'resourceListAnchors': [],
                    'relatedToObjectAnnotations': None,
                    'supportingResources': [],
                    'meanings': [],
                    'outputFromProcesses': []
                }
            },
            'classifications': [{
                'typeName': 'JdbcAccess',
                'entityGuid': '55a5f51f-1b21-4742-9059-818b7531df85',
                'propagate': True
            }, {
                'typeName': 'Dimension',
                'entityGuid': '55a5f51f-1b21-4742-9059-818b7531df85',
                'propagate': True
            }]
        }

    @classmethod
    def __create_load_process_entity_dict(cls):
        return {
            'guid':
                'e73656c9-da05-4db7-9b88-e3669c1a98eb',
            'data': {
                'typeName': 'LoadProcess',
                'attributes': {
                    'owner': None,
                    'outputs': [{
                        'guid': '15f5bf50-5a39-4994-9cba-6c01d016f52f',
                        'typeName': 'Table',
                        'data': {
                            'typeName': 'Table',
                            'attributes': {
                                'owner': 'Tim ETL',
                                'temporary': False,
                                'lastAccessTime': 1589983848291,
                                'qualifiedName': 'logging_fact_monthly_mv',
                                'name': 'logging_fact_monthly_mv',
                            },
                            'guid': '15f5bf50-5a39-4994-9cba-6c01d016f52f',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983848303,
                            'updateTime': 1589983852532,
                            'version': 0
                        }
                    }],
                    'queryGraph': 'graph',
                    'qualifiedName': 'loadLogsMonthly',
                    'inputs': [{
                        'guid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
                        'typeName': 'Table',
                        'data': {
                            'typeName': 'Table',
                            'attributes': {
                                'owner': 'Tim ETL',
                                'temporary': False,
                                'lastAccessTime': 1589983847754,
                                'qualifiedName': 'log_fact_daily_mv',
                                'name': 'log_fact_daily_mv'
                            },
                            'guid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
                            'status': 'ACTIVE',
                            'createdBy': 'admin',
                            'updatedBy': 'admin',
                            'createTime': 1589983847758,
                            'updateTime': 1589983852532,
                            'version': 0
                        }
                    }],
                    'description': 'hive query for monthly summary',
                    'userName': None,
                    'queryId': 'id',
                    'name': 'loadLogsMonthly',
                    'queryText': 'create table as select ',
                    'startTime': 1589983852529,
                    'queryPlan': 'plan',
                    'endTime': 1589983862529
                },
                'guid': 'e73656c9-da05-4db7-9b88-e3669c1a98eb',
                'status': 'ACTIVE',
                'createdBy': 'admin',
                'updatedBy': 'admin',
                'createTime': 1589983852532,
                'updateTime': 1589983852532,
                'version': 0,
                'relationshipAttributes': {}
            },
            'classifications': [{
                'typeName': 'Log Data',
                'entityGuid': '30cfc9fc-aec4-4017-b649-f1f92351a727',
                'propagate': True
            }, {
                'typeName': 'ETL',
                'entityGuid': 'e73656c9-da05-4db7-9b88-e3669c1a98eb',
                'propagate': True
            }]
        }
