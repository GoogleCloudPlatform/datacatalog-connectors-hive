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

import copy

from google.cloud import datacatalog
from google.cloud.datacatalog import types
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.apache_atlas.prepare import constant
from google.datacatalog_connectors.apache_atlas.prepare import \
    datacatalog_attribute_normalizer as attr_normalizer


class DataCatalogTagFactory(prepare.BaseTagFactory):
    __STRING_VALUE_ARRAY_ELEM_SEP = ', '
    # String field values are limited to 2000 bytes
    # size when encoded in UTF-8.
    __STRING_VALUE_MAX_LENGTH = 2000

    __IGNORED_ATTRIBUTES_LIST = ['columns']

    def __init__(self, project_id, location_id, instance_url):
        self.__project_id = project_id
        self.__location_id = location_id
        self.__instance_url = instance_url

    def make_tag_for_classification(self,
                                    entity_classification,
                                    classifications,
                                    enum_types_dict,
                                    column_name=None):
        tag = types.Tag()

        classification_name = entity_classification['typeName']

        classification = classifications[classification_name]

        classification_data = classification['data']
        formatted_name = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name(classification_name)
        version = classification_data.get('version')

        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id(formatted_name,
                                   constant.CLASSIFICATION_PREFIX,
                                   version)

        tag.template = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=tag_template_id)

        super()._set_bool_field(tag, formatted_name, True)

        attributes = entity_classification.get('attributes')
        attribute_defs = classification_data['attributeDefs']
        if attributes:
            self.__add_fields_from_attributes(tag, attributes, attribute_defs,
                                              enum_types_dict)

        if column_name:
            tag.column = attr_normalizer.DataCatalogAttributeNormalizer.\
                format_name(column_name)

        return tag

    def make_tag_for_column_ref(self, column_guid, column_name):
        tag = types.Tag()

        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id('ref', constant.COLUMN_PREFIX)

        tag.template = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=tag_template_id)

        super()._set_string_field(tag, 'column_guid', column_guid)

        tag.column = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name(column_name)

        return tag

    def make_tag_for_entity(self, entity, entity_types_dict, enum_types_dict):
        tag = types.Tag()

        guid = entity['guid']
        data = entity['data']
        entity_type_name = data['typeName']
        entity_type = entity_types_dict[entity_type_name]
        formatted_name = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name(entity_type_name)

        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id(formatted_name,
                                   constant.ENTITY_TYPE_PREFIX,
                                   entity_type['data']['version'])

        tag.template = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=tag_template_id)

        attributes = data['attributes']
        super()._set_bool_field(tag, formatted_name, True)
        super()._set_string_field(tag, constant.ENTITY_GUID, guid)
        super()._set_string_field(tag, constant.INSTANCE_URL_FIELD,
                                  self.__instance_url)

        attribute_defs = entity_type['data']['attributeDefs']
        self.__add_fields_from_attributes(tag, attributes, attribute_defs,
                                          enum_types_dict)

        self.__create_custom_fields_for_entity_type(tag, entity_type_name,
                                                    attributes)

        return tag

    @classmethod
    def __add_fields_from_attributes(cls, tag, attributes, attribute_defs,
                                     enum_types_dict):
        for name, value in attributes.items():
            if not value:
                value = ''

            # Create a copy from value to remove
            # fields without impacting other workloads
            tag_value = copy.deepcopy(value)
            if isinstance(tag_value, list):
                for item in tag_value:
                    cls.__set_up_verbose_attributes(item)
            else:
                cls.__set_up_verbose_attributes(tag_value)

            cls.__set_tag_field(attribute_defs, enum_types_dict, name, tag,
                                tag_value)

    @classmethod
    def __set_tag_field(cls, attribute_defs, enum_types_dict, field_name, tag,
                        tag_value):
        formatted_name = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name(field_name)

        if formatted_name not in cls.__IGNORED_ATTRIBUTES_LIST:
            attribute_def = cls.__find_attribute_def(attribute_defs,
                                                     field_name)

            type_name = attribute_def.get('typeName')

            enum_type = enum_types_dict.get(type_name)

            if type_name in constant.DATACATALOG_TARGET_PRIMITIVE_TYPES:
                if type_name in constant.DATACATALOG_TARGET_DOUBLE_TYPE:
                    super()._set_double_field(tag, formatted_name, tag_value)
                elif type_name in constant.DATACATALOG_TARGET_BOOLEAN_TYPE:
                    super()._set_bool_field(tag, formatted_name,
                                            bool(tag_value))
                else:
                    super()._set_string_field(tag, formatted_name,
                                              str(tag_value))
            elif enum_type:
                tag.fields[formatted_name].enum_value.display_name = tag_value
            else:
                super()._set_string_field(tag, formatted_name, str(tag_value))

    @classmethod
    def __find_attribute_def(cls, attribute_defs, field_name):
        try:
            attribute_def = next(
                filter(
                    lambda attribute_def: attribute_def['name'] == field_name,
                    attribute_defs))
        except StopIteration:
            attribute_def = {'typeName': constant.ATLAS_STRING_TYPE}
        return attribute_def

    @classmethod
    def __set_up_verbose_attributes(cls, value):
        # Clean attributes from Tag
        if isinstance(value, dict):
            value.pop('classifications', None)

            data = value.get('data')
            if data:
                attributes = data.pop('attributes', None)
                if attributes:
                    name = attributes.get('name', '')

                    value['name'] = name

                # Remove data, we are adding only the name field to the tag.
                value.pop('data', None)

    def __create_custom_fields_for_entity_type(self, tag, entity_type_name,
                                               attributes):
        if entity_type_name == constant.ENTITY_TYPE_TABLE:
            self.__create_custom_fields_for_table_type(tag, attributes)
        elif entity_type_name == constant.ENTITY_TYPE_VIEW:
            self.__create_custom_fields_for_view_type(tag, attributes)
        elif entity_type_name == constant.ENTITY_TYPE_LOAD_PROCESS:
            self.__create_custom_fields_for_load_process_type(tag, attributes)

    def __create_custom_fields_for_table_type(self, tag, attributes):
        self.__add_db_field(attributes, tag)

        sd = attributes.get('sd')
        if sd:
            sd_guid = sd.get('guid')
            super()._set_string_field(tag, 'sd_guid', sd_guid)

    def __create_custom_fields_for_view_type(self, tag, attributes):
        self.__add_db_field(attributes, tag)

        input_tables = attributes.get('inputTables')
        if input_tables:
            input_tables_names = []
            if isinstance(input_tables, list):
                for input_table in input_tables:
                    input_table_name = input_table.get('data', {}).get(
                        'attributes', {}).get('name', '')
                    input_tables_names.append(input_table_name)
            super()._set_string_field(tag, 'input_tables_names',
                                      ','.join(input_tables_names))

    def __create_custom_fields_for_load_process_type(self, tag, attributes):
        self.__add_db_field(attributes, tag)

        self.__extract_names_from_list('inputs', attributes, tag)
        self.__extract_names_from_list('outputs', attributes, tag)

    def __extract_names_from_list(self, attribute_name, attributes, tag):
        field_list = attributes.get(attribute_name)
        if field_list:
            field_names = []
            if isinstance(field_list, list):
                for field_dict in field_list:
                    field_name = field_dict.get('data',
                                                {}).get('attributes',
                                                        {}).get('name', '')
                    field_names.append(field_name)

            super()._set_string_field(tag, '{}_names'.format(attribute_name),
                                      ','.join(field_names))

    def __add_db_field(self, attributes, tag):
        db = attributes.get('db')
        if db:
            db_name = db.get('data', {}).get('attributes', {}).get('name', '')
            db_guid = db.get('guid')
            super()._set_string_field(tag, 'db_name', db_name)
            super()._set_string_field(tag, 'db_guid', db_guid)
