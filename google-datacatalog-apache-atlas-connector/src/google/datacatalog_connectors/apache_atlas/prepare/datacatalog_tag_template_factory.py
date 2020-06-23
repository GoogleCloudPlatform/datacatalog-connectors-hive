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

from google.cloud import datacatalog
from google.cloud.datacatalog import enums, types

from google.datacatalog_connectors.apache_atlas.prepare import constant
from google.datacatalog_connectors.apache_atlas.prepare import \
    datacatalog_attribute_normalizer as attr_normalizer


class DataCatalogTagTemplateFactory:

    __BOOL_TYPE = enums.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = enums.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = enums.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = enums.FieldType.PrimitiveType.TIMESTAMP

    def __init__(self, project_id, location_id):
        self.__project_id = project_id
        self.__location_id = location_id

    def make_tag_templates_from_apache_atlas_metadata(self, metadata_dict):
        classifications_dict = metadata_dict['classifications']
        enum_types_dict = metadata_dict['enum_types']
        entity_types_dict = metadata_dict['entity_types']

        tag_templates = {}

        tag_templates.update(
            self.make_tag_templates_from_classification_metatada(
                classifications_dict, enum_types_dict))
        tag_templates.update(
            self.make_tag_templates_from_entity_types_metatada(
                entity_types_dict, enum_types_dict))
        tag_templates.update(self.make_column_tag_template())

        return tag_templates

    def make_tag_templates_from_classification_metatada(
            self, metadata_dict, enum_types_dict):
        tag_templates = {}
        for _, classification in metadata_dict.items():
            tag_templates.update(
                self.__create_classification_tag_template(
                    classification, metadata_dict, enum_types_dict))
        return tag_templates

    def make_column_tag_template(self):
        tag_template = types.TagTemplate()

        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id(
                'ref', constant.COLUMN_PREFIX)

        tag_template.name = self.get_tag_template_path(tag_template_id)

        tag_template.display_name = 'Column'

        tag_template.fields[
            'column_guid'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['column_guid'].display_name = 'column guid'
        tag_template.fields[
            'column_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields[
            'column_entry'].display_name = 'column data catalog entry'

        return {tag_template_id: tag_template}

    def get_tag_template_path(self, tag_template_id):
        return datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=tag_template_id)

    def make_tag_templates_from_entity_types_metatada(self, metadata_dict,
                                                      enum_types_dict):
        tag_templates = {}
        for _, entity_type in metadata_dict.items():

            # Only create templates for types that contain entities.
            if entity_type.get('entities'):
                tag_templates.update(
                    self.__create_entity_type_tag_template(
                        entity_type, metadata_dict, enum_types_dict))
        return tag_templates

    def __create_classification_tag_template(self, classification_dict,
                                             classifications_dict,
                                             enum_types_dict):
        tag_template = types.TagTemplate()

        classification_data = classification_dict['data']

        name = classification_data['name']
        formatted_name = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name(name)
        version = classification_data.get('version')

        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id(formatted_name,
                                   constant.CLASSIFICATION_PREFIX,
                                   version)

        tag_template.name = self.get_tag_template_path(tag_template_id)

        tag_template.display_name = '{}'.format(name)

        classification_data = classification_dict['data']
        classification_super_types = classification_data.get('superTypes')
        attribute_defs = classification_data['attributeDefs']

        if classification_super_types:
            self.__add_fields_from_super_types(tag_template,
                                               classification_super_types,
                                               classifications_dict,
                                               enum_types_dict)

        self.__add_fields_from_attribute_defs(tag_template, attribute_defs,
                                              enum_types_dict)

        tag_template.fields[
            formatted_name].type.primitive_type = self.__BOOL_TYPE
        tag_template.fields[formatted_name].display_name = formatted_name

        return {tag_template_id: tag_template}

    def __create_entity_type_tag_template(self, entity_type_dict,
                                          entity_types_dict, enum_types_dict):
        tag_template = types.TagTemplate()

        entity_type_data = entity_type_dict['data']
        name = entity_type_data['name']
        formatted_name = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name(name)
        version = entity_type_data.get('version')

        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id(formatted_name,
                                   constant.ENTITY_TYPE_PREFIX,
                                   version)

        tag_template.name = self.get_tag_template_path(tag_template_id)

        tag_template.display_name = 'Type - {}'.format(name)

        entity_super_types = entity_type_dict.get('superTypes')
        attribute_defs = entity_type_data['attributeDefs']

        if entity_super_types:
            self.__add_fields_from_super_types(tag_template,
                                               entity_super_types,
                                               entity_types_dict,
                                               enum_types_dict)

        self.__add_fields_from_attribute_defs(tag_template, attribute_defs,
                                              enum_types_dict)

        tag_template.fields[
            formatted_name].type.primitive_type = self.__BOOL_TYPE
        tag_template.fields[formatted_name].display_name = formatted_name

        tag_template.fields[
            constant.ENTITY_GUID].type.primitive_type = self.__STRING_TYPE
        tag_template.fields[constant.ENTITY_GUID].display_name = 'entity guid'

        tag_template.fields[
            constant.
            INSTANCE_URL_FIELD].type.primitive_type = self.__STRING_TYPE
        tag_template.fields[
            constant.INSTANCE_URL_FIELD].display_name = 'instance url'

        self.__create_custom_fields_for_entity_type(tag_template,
                                                    entity_type_data)

        return {tag_template_id: tag_template}

    def __add_fields_from_attribute_defs(self, tag_template, attribute_defs,
                                         enum_types_dict):
        for attribute_def in attribute_defs:
            name = attribute_def['name']
            self.__set_tag_template_field(attribute_def, enum_types_dict, name,
                                          tag_template)

    def __set_tag_template_field(self, attribute_def, enum_types_dict, name,
                                 tag_template):
        formatted_name = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name(name)
        type_name = attribute_def['typeName']
        enum_type = enum_types_dict.get(type_name)
        if type_name in constant.DATACATALOG_TARGET_PRIMITIVE_TYPES:
            if type_name in constant.DATACATALOG_TARGET_DOUBLE_TYPE:
                target_type = self.__DOUBLE_TYPE
            elif type_name in constant.DATACATALOG_TARGET_BOOLEAN_TYPE:
                target_type = self.__BOOL_TYPE
            else:
                target_type = self.__STRING_TYPE

            tag_template.fields[
                formatted_name].type.primitive_type = target_type
        elif enum_type:
            enum_element_defs = enum_type['data']['elementDefs']
            for enum_element_def in enum_element_defs:
                tag_template.fields[formatted_name].type.enum_type\
                    .allowed_values.add().display_name =\
                    enum_element_def['value']
        else:
            # String is the default type.
            tag_template.fields[
                formatted_name].type.primitive_type = self.__STRING_TYPE

        tag_template.fields[formatted_name].display_name = name

    def __add_fields_from_super_types(self, tag_template, super_types,
                                      types_dict, enum_types_dict):
        for super_type in super_types:
            type_dict = types_dict[super_type]

            # Here we handle multiple ancestors fields.
            recursive_super_types = type_dict.get('superTypes')
            if recursive_super_types:
                self.__add_fields_from_super_types(tag_template,
                                                   recursive_super_types,
                                                   types_dict, enum_types_dict)

            type_data = type_dict['data']
            self.__add_fields_from_attribute_defs(tag_template,
                                                  type_data['attributeDefs'],
                                                  enum_types_dict)

    def __create_custom_fields_for_entity_type(self, tag_template,
                                               entity_type_data):
        entity_type = entity_type_data['name']
        if entity_type == constant.ENTITY_TYPE_TABLE:
            self.__create_custom_fields_for_table_type(tag_template)
        elif entity_type == constant.ENTITY_TYPE_VIEW:
            self.__create_custom_fields_for_view_type(tag_template)
        elif entity_type == constant.ENTITY_TYPE_LOAD_PROCESS:
            self.__create_custom_fields_for_load_process_type(tag_template)

    def __create_custom_fields_for_table_type(self, tag_template):
        self.__add_db_fields(tag_template)

        tag_template.fields['sd_guid'].type.primitive_type =\
            self.__STRING_TYPE
        tag_template.fields['sd_guid'].display_name = 'sd guid'

        tag_template.fields[
            'sd_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['sd_entry'].display_name = 'sd data catalog entry'

    def __add_db_fields(self, tag_template):
        tag_template.fields['db_name'].type.primitive_type =\
            self.__STRING_TYPE
        tag_template.fields['db_name'].display_name = 'db name'
        tag_template.fields['db_guid'].type.primitive_type =\
            self.__STRING_TYPE
        tag_template.fields['db_guid'].display_name = 'db guid'
        tag_template.fields[
            'db_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['db_entry'].display_name = 'db data catalog entry'

    def __create_custom_fields_for_view_type(self, tag_template):
        self.__add_db_fields(tag_template)

        tag_template.fields[
            'input_tables_names'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields[
            'input_tables_names'].display_name = 'input tables names'

    def __create_custom_fields_for_load_process_type(self, tag_template):
        tag_template.fields[
            'inputs_names'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['inputs_names'].display_name = 'inputs names'
        tag_template.fields[
            'outputs_names'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['outputs_names'].display_name = 'outputs names'
