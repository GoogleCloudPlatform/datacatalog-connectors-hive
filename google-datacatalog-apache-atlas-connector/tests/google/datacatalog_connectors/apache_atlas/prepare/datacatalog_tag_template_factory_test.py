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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons_test import utils

from google.datacatalog_connectors.apache_atlas.prepare import \
    datacatalog_tag_template_factory


class DataCatalogTagTemplateFactoryTest(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    __BOOL_TYPE = datacatalog.enums.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.enums.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.enums.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.enums.FieldType.PrimitiveType.TIMESTAMP

    def setUp(self):
        self.__factory = datacatalog_tag_template_factory. \
            DataCatalogTagTemplateFactory('test-project', 'test-location')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual('test-project',
                         attrs['_DataCatalogTagTemplateFactory__project_id'])
        self.assertEqual('test-location',
                         attrs['_DataCatalogTagTemplateFactory__location_id'])

    def test_make_tag_templates_from_classification_metatada_should_return(
            self):
        metadata_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH,
            'classifications_metadata_for_tag_templates.json')

        enum_types_dict = {
            'GovernanceClassificationStatus': {
                'data': {
                    'elementDefs': [{
                        'value': 'LEVEL_1'
                    }, {
                        'value': 'LEVEL_2'
                    }]
                }
            }
        }

        tag_templates = self.__factory.\
            make_tag_templates_from_classification_metatada(
                metadata_dict, enum_types_dict)

        self.assertEqual(14, len(tag_templates))

        tag_template_pii = tag_templates['apache_atlas_classification_pii_1']
        tag_template_confidentiality = tag_templates[
            'apache_atlas_classification_confidentiality_1']

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_classification_pii_1',
            tag_template_pii.name)

        self.assertEqual('PII', tag_template_pii.display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template_pii.fields['pii'].type.primitive_type)
        self.assertEqual('pii', tag_template_pii.fields['pii'].display_name)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_classification_confidentiality_1',
            tag_template_confidentiality.name)

        self.assertEqual('Confidentiality',
                         tag_template_confidentiality.display_name)

        self.assertEqual(
            self.__BOOL_TYPE, tag_template_confidentiality.
            fields['confidentiality'].type.primitive_type)
        self.assertEqual(
            'confidentiality', tag_template_confidentiality.
            fields['confidentiality'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template_confidentiality.fields['level'].type.primitive_type)
        self.assertEqual(
            'level', tag_template_confidentiality.fields['level'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE, tag_template_confidentiality.
            fields['confidence'].type.primitive_type)
        self.assertEqual(
            'confidence',
            tag_template_confidentiality.fields['confidence'].display_name)

        status_enum_type = tag_template_confidentiality.fields[
            'status'].type.enum_type.allowed_values

        self.assertEqual('LEVEL_1', status_enum_type[0].display_name)
        self.assertEqual('LEVEL_2', status_enum_type[1].display_name)

        self.assertEqual(
            'status',
            tag_template_confidentiality.fields['status'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_confidentiality.fields['notes'].type.primitive_type)
        self.assertEqual(
            'notes', tag_template_confidentiality.fields['notes'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_confidentiality.fields['source'].type.primitive_type)
        self.assertEqual(
            'source',
            tag_template_confidentiality.fields['source'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_confidentiality.fields['steward'].type.primitive_type)
        self.assertEqual(
            'steward',
            tag_template_confidentiality.fields['steward'].display_name)

    def test_make_column_tag_template_should_return(self):
        tag_template_dict = self.__factory.make_column_tag_template()

        tag_template_id, tag_template = next(iter(tag_template_dict.items()))

        self.assertEqual('apache_atlas_column_ref', tag_template_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/tagTemplates/'
            'apache_atlas_column_ref', tag_template.name)

        self.assertEqual('Column', tag_template.display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['column_guid'].type.primitive_type)
        self.assertEqual('column guid',
                         tag_template.fields['column_guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['column_entry'].type.primitive_type)
        self.assertEqual('column data catalog entry',
                         tag_template.fields['column_entry'].display_name)

    def test_make_tag_templates_from_entity_types_no_entities_should_return_empty(  # noqa: E501
            self):
        metadata_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH,
            'entity_types_metadata_no_entities_for_tag_templates.json')

        tag_templates = self.__factory.\
            make_tag_templates_from_entity_types_metatada(
                metadata_dict, {})

        self.assertEqual(0, len(tag_templates))

    def test_make_tag_templates_from_apache_atlas_metadata_should_return(self):
        entity_types_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'entity_types_metadata_for_tag_templates.json')

        classifications_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH,
            'classifications_metadata_for_tag_templates.json')

        tag_templates = self.__factory.\
            make_tag_templates_from_apache_atlas_metadata(
                {
                    'entity_types': entity_types_dict,
                    'classifications': classifications_dict,
                    'enum_types': {}
                })

        self.assertEqual(19, len(tag_templates))

    def test_make_tag_templates_from_entity_types_should_return(self):
        metadata_dict = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'entity_types_metadata_for_tag_templates.json')

        tag_templates = self.__factory.\
            make_tag_templates_from_entity_types_metatada(
                metadata_dict, {})

        self.assertEqual(4, len(tag_templates))

        # types with entities should have templates
        tag_template_entity_type_storagedesc = tag_templates[
            'apache_atlas_entity_type_storagedesc_1']
        self.assertIsNotNone(tag_template_entity_type_storagedesc)
        tag_template_entity_type_column = tag_templates[
            'apache_atlas_entity_type_column_1']
        self.assertIsNotNone(tag_template_entity_type_column)
        tag_template_entity_type_table = tag_templates[
            'apache_atlas_entity_type_table_1']
        self.assertIsNotNone(tag_template_entity_type_table)
        tag_template_entity_type_db = tag_templates[
            'apache_atlas_entity_type_db_1']
        self.assertIsNotNone(tag_template_entity_type_db)

        # types without entities shouldnt have templates
        tag_template_entity_type_referenceable = tag_templates.get(
            'apache_atlas_entity_type_referenceable_1')
        self.assertIsNone(tag_template_entity_type_referenceable)
        tag_template_entity_type_dataset = tag_templates.get(
            'apache_atlas_entity_type_dataset_1')
        self.assertIsNone(tag_template_entity_type_dataset)
        tag_template_entity_type_asset = tag_templates.get(
            'apache_atlas_entity_type_asset_1')
        self.assertIsNone(tag_template_entity_type_asset)

        # assert storage desc template
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_storagedesc_1',
            tag_template_entity_type_storagedesc.name)

        self.assertEqual('Type - StorageDesc',
                         tag_template_entity_type_storagedesc.display_name)

        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_storagedesc.
            fields['table'].type.primitive_type)
        self.assertEqual(
            'table',
            tag_template_entity_type_storagedesc.fields['table'].display_name)
        self.assertEqual(
            self.__BOOL_TYPE, tag_template_entity_type_storagedesc.
            fields['compressed'].type.primitive_type)
        self.assertEqual(
            'compressed', tag_template_entity_type_storagedesc.
            fields['compressed'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_storagedesc.
            fields['guid'].type.primitive_type)
        self.assertEqual(
            'entity guid',
            tag_template_entity_type_storagedesc.fields['guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_storagedesc.
            fields['instance_url'].type.primitive_type)
        self.assertEqual(
            'instance url', tag_template_entity_type_storagedesc.
            fields['instance_url'].display_name)
        self.assertEqual(
            self.__BOOL_TYPE, tag_template_entity_type_storagedesc.
            fields['storagedesc'].type.primitive_type)
        self.assertEqual(
            'storagedesc', tag_template_entity_type_storagedesc.
            fields['storagedesc'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_storagedesc.
            fields['outputformat'].type.primitive_type)
        self.assertEqual(
            'outputFormat', tag_template_entity_type_storagedesc.
            fields['outputformat'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_storagedesc.
            fields['location'].type.primitive_type)
        self.assertEqual(
            'location', tag_template_entity_type_storagedesc.
            fields['location'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_storagedesc.
            fields['inputformat'].type.primitive_type)
        self.assertEqual(
            'inputFormat', tag_template_entity_type_storagedesc.
            fields['inputformat'].display_name)

        # assert column template
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_column_1',
            tag_template_entity_type_column.name)

        self.assertEqual('Type - Column',
                         tag_template_entity_type_column.display_name)

        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_column.
            fields['comment'].type.primitive_type)
        self.assertEqual(
            'comment',
            tag_template_entity_type_column.fields['comment'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_column.
            fields['table'].type.primitive_type)
        self.assertEqual(
            'table',
            tag_template_entity_type_column.fields['table'].display_name)
        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_entity_type_column.fields['guid'].type.primitive_type)
        self.assertEqual(
            'entity guid',
            tag_template_entity_type_column.fields['guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_column.
            fields['instance_url'].type.primitive_type)
        self.assertEqual(
            'instance url', tag_template_entity_type_column.
            fields['instance_url'].display_name)
        self.assertEqual(
            self.__BOOL_TYPE, tag_template_entity_type_column.fields['column'].
            type.primitive_type)
        self.assertEqual(
            'column',
            tag_template_entity_type_column.fields['column'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_column.
            fields['datatype'].type.primitive_type)
        self.assertEqual(
            'dataType',
            tag_template_entity_type_column.fields['datatype'].display_name)

        # assert table template
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_table_1',
            tag_template_entity_type_table.name)

        self.assertEqual('Type - Table',
                         tag_template_entity_type_table.display_name)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template_entity_type_table.fields['table'].type.primitive_type)
        self.assertEqual(
            'table',
            tag_template_entity_type_table.fields['table'].display_name)
        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_entity_type_table.fields['guid'].type.primitive_type)
        self.assertEqual(
            'entity guid',
            tag_template_entity_type_table.fields['guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['instance_url'].type.primitive_type)
        self.assertEqual(
            'instance url',
            tag_template_entity_type_table.fields['instance_url'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['columns'].type.primitive_type)
        self.assertEqual(
            'columns',
            tag_template_entity_type_table.fields['columns'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['tabletype'].type.primitive_type)
        self.assertEqual(
            'tableType',
            tag_template_entity_type_table.fields['tabletype'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['sd_guid'].type.primitive_type)
        self.assertEqual(
            'sd guid',
            tag_template_entity_type_table.fields['sd_guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['sd_entry'].type.primitive_type)
        self.assertEqual(
            'sd data catalog entry',
            tag_template_entity_type_table.fields['sd_entry'].display_name)
        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_entity_type_table.fields['sd'].type.primitive_type)
        self.assertEqual(
            'sd', tag_template_entity_type_table.fields['sd'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['sd_guid'].type.primitive_type)
        self.assertEqual(
            'db guid',
            tag_template_entity_type_table.fields['db_guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['sd_entry'].type.primitive_type)
        self.assertEqual(
            'db data catalog entry',
            tag_template_entity_type_table.fields['db_entry'].display_name)
        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_entity_type_table.fields['db'].type.primitive_type)
        self.assertEqual(
            'db', tag_template_entity_type_table.fields['db'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['db_name'].type.primitive_type)
        self.assertEqual(
            'db name',
            tag_template_entity_type_table.fields['db_name'].display_name)
        self.assertEqual(
            self.__STRING_TYPE,
            tag_template_entity_type_table.fields['owner'].type.primitive_type)
        self.assertEqual(
            'owner',
            tag_template_entity_type_table.fields['owner'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['createtime'].type.primitive_type)
        self.assertEqual(
            'createTime',
            tag_template_entity_type_table.fields['createtime'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['lastaccesstime'].type.primitive_type)
        self.assertEqual(
            'lastAccessTime', tag_template_entity_type_table.
            fields['lastaccesstime'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_table.
            fields['retention'].type.primitive_type)
        self.assertEqual(
            'retention',
            tag_template_entity_type_table.fields['retention'].display_name)

    def test_make_tag_templates_from_view_entity_type_should_return(self):
        metadata_dict = {
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
                'entities': {
                    "1": {},
                    "2": {}
                }
            }
        }

        tag_templates = self.__factory.\
            make_tag_templates_from_entity_types_metatada(
                metadata_dict, {})

        self.assertEqual(1, len(tag_templates))

        # types with entities should have templates
        tag_template_entity_type_view = tag_templates[
            'apache_atlas_entity_type_view_1']
        self.assertIsNotNone(tag_template_entity_type_view)

        # assert storage desc template
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_view_1',
            tag_template_entity_type_view.name)

        self.assertEqual('Type - View',
                         tag_template_entity_type_view.display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['input_tables_names'].type.primitive_type)
        self.assertEqual(
            'input tables names', tag_template_entity_type_view.
            fields['input_tables_names'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['db_name'].type.primitive_type)
        self.assertEqual(
            'db name',
            tag_template_entity_type_view.fields['db_name'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['db_entry'].type.primitive_type)
        self.assertEqual(
            'db data catalog entry',
            tag_template_entity_type_view.fields['db_entry'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['db_guid'].type.primitive_type)
        self.assertEqual(
            'db guid',
            tag_template_entity_type_view.fields['db_guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['db_guid'].type.primitive_type)
        self.assertEqual(
            'entity guid',
            tag_template_entity_type_view.fields['guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['instance_url'].type.primitive_type)
        self.assertEqual(
            'instance url',
            tag_template_entity_type_view.fields['instance_url'].display_name)
        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template_entity_type_view.fields['view'].type.primitive_type)
        self.assertEqual(
            'view', tag_template_entity_type_view.fields['view'].display_name)

    def test_make_tag_templates_from_load_process_entity_type_should_return(
            self):
        metadata_dict = {
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
                'entities': {
                    "1": {},
                    "2": {}
                }
            }
        }

        tag_templates = self.__factory.\
            make_tag_templates_from_entity_types_metatada(
                metadata_dict, {})

        self.assertEqual(1, len(tag_templates))

        # types with entities should have templates
        tag_template_entity_type_view = tag_templates[
            'apache_atlas_entity_type_loadprocess_1']
        self.assertIsNotNone(tag_template_entity_type_view)

        # assert storage desc template
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/apache_atlas_entity_type_loadprocess_1',
            tag_template_entity_type_view.name)

        self.assertEqual('Type - LoadProcess',
                         tag_template_entity_type_view.display_name)

        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['inputs_names'].type.primitive_type)
        self.assertEqual(
            'inputs names',
            tag_template_entity_type_view.fields['inputs_names'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['inputs_names'].type.primitive_type)
        self.assertEqual(
            'outputs names',
            tag_template_entity_type_view.fields['outputs_names'].display_name)
        self.assertEqual(
            'entity guid',
            tag_template_entity_type_view.fields['guid'].display_name)
        self.assertEqual(
            self.__STRING_TYPE, tag_template_entity_type_view.
            fields['instance_url'].type.primitive_type)
        self.assertEqual(
            'instance url',
            tag_template_entity_type_view.fields['instance_url'].display_name)
        self.assertEqual(
            self.__BOOL_TYPE, tag_template_entity_type_view.
            fields['loadprocess'].type.primitive_type)
        self.assertEqual(
            'loadprocess',
            tag_template_entity_type_view.fields['loadprocess'].display_name)
