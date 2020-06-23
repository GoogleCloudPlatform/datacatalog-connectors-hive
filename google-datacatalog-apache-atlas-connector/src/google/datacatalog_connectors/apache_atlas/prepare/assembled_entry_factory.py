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

import logging

from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.apache_atlas.prepare import \
    datacatalog_attribute_normalizer as attr_normalizer
from . import datacatalog_entry_factory, datacatalog_tag_factory


class AssembledEntryFactory:

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, instance_url):

        self.__datacatalog_entry_factory = datacatalog_entry_factory \
            .DataCatalogEntryFactory(
                project_id, location_id, entry_group_id,
                user_specified_system, instance_url)

        self.__datacatalog_tag_factory = \
            datacatalog_tag_factory.DataCatalogTagFactory(
                project_id,
                location_id,
                instance_url)

    def make_assembled_entries_list(self,
                                    metadata_dict,
                                    apache_entity_types=None):
        entity_types_dict = metadata_dict['entity_types']
        classifications = metadata_dict['classifications']
        enum_types_dict = metadata_dict['enum_types']

        assembled_entries = []
        for _, entity_type_dict in entity_types_dict.items():
            entity_type_name = entity_type_dict['name']
            if (apache_entity_types and
                entity_type_name in apache_entity_types) or \
                    apache_entity_types is None:

                logging.info('===> Processing entities for type: %s...',
                             entity_type_name)
                entities = entity_type_dict['entities']
                if entities:
                    for _, entity_dict in entities.items():
                        assembled_entries.append(
                            self.__make_assembled_entry_for_entity(
                                entity_dict, entity_types_dict,
                                classifications, enum_types_dict))
            else:
                logging.info(
                    '===> Ignoring entities for type: %s...,'
                    ' not in allowed list: %s', entity_type_name,
                    apache_entity_types)

        return assembled_entries

    def __make_assembled_entry_for_entity(self, entity, entity_types_dict,
                                          classifications, enum_types_dict):

        entry_id, entry = self.__datacatalog_entry_factory. \
            make_entry_for_entity(entity)

        tags = [
            self.__datacatalog_tag_factory.make_tag_for_entity(
                entity, entity_types_dict, enum_types_dict)
        ]

        entry_classifications = entity.get('classifications')

        if entry_classifications:
            for classification in entry_classifications:
                tags.append(
                    self.__datacatalog_tag_factory.make_tag_for_classification(
                        classification, classifications, enum_types_dict))
        self.__make_tags_for_columns(classifications, entity, tags,
                                     enum_types_dict)

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_tags_for_columns(self, classifications, entity, tags,
                                enum_types_dict):
        entity_data = entity['data']
        attributes = entity_data['attributes']
        columns = attributes.get('columns')

        if columns:
            for column in columns:
                column_guid = column['guid']
                column_data = column.get('data')
                if column_data:
                    column_attributes = column_data.get('attributes')
                    column_name = column_attributes.get('name')
                    data_type = attr_normalizer.\
                        DataCatalogAttributeNormalizer.\
                        get_column_data_type(
                            column_attributes)

                    # This means this is a valid column.
                    if data_type and column_name:
                        column_classifications = column.get('classifications')
                        if column_classifications:
                            for classification in column_classifications:
                                tags.append(
                                    self.__datacatalog_tag_factory.
                                    make_tag_for_classification(
                                        classification, classifications,
                                        enum_types_dict, column_name))
                        tags.append(
                            self.__datacatalog_tag_factory.
                            make_tag_for_column_ref(column_guid, column_name))
