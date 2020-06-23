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

from google.cloud import datacatalog
from google.cloud.datacatalog import types
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.apache_atlas.prepare import \
    datacatalog_attribute_normalizer as attr_normalizer


class DataCatalogEntryFactory(prepare.BaseEntryFactory):

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, instance_url):

        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id
        self.__user_specified_system = user_specified_system
        self.__instance_url = instance_url

        # Strip schema (http | https) and slashes from the server url.
        self.__server_id = instance_url[instance_url.find('//') + 2:]

    def make_entry_for_entity(self, entity):
        entry = types.Entry()

        guid = entity['guid']
        data = entity['data']
        type_name = attr_normalizer.DataCatalogAttributeNormalizer.format_name(
            data['typeName'])

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = type_name

        generated_id, name, location, columns = self.__get_entry_attributes(
            entry, guid, data)

        # ADD type to generated_id since ids can be reused between types.
        generated_id = '{}_{}'.format(type_name, generated_id)

        self.__set_entry_names(entry, generated_id, name)

        self.__set_linked_resource(entry, generated_id, location, type_name)

        self.__set_source_timestamp_fields(entry, data, generated_id)

        self.__create_schema(entry, columns)

        return generated_id, entry

    def __get_entry_attributes(self, entry, guid, data):
        generated_id = self.__format_id(guid)
        name = None
        location = None
        attributes = data['attributes']

        if attributes:
            location = attributes.get('location')

            name = attributes.get('name')
            if name:
                name = self.__format_id(name)

            description = attributes.get('description')
            if description:
                entry.description = description

        return generated_id, name, location, attributes.get('columns')

    def __format_id(self, source_id):
        return super()._format_id(source_id).lower()

    def __set_entry_names(self, entry, generated_id, name):
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        if name:
            entry_name = super()._format_display_name(name)
        else:
            entry_name = super()._format_display_name(generated_id)

        entry.display_name = entry_name

    def __set_linked_resource(self, entry, generated_id, location, type_name):
        if location:
            linked_resource = '//{}'.format(location)
        else:
            linked_resource = '{}/{}/{}'.format(self.__instance_url, type_name,
                                                generated_id)
        entry.linked_resource = linked_resource

    @classmethod
    def __set_source_timestamp_fields(cls, entry, data, generated_id):
        create_time = data.get('createTime')
        update_time = data.get('updateTime')
        if create_time:
            # Transform millis to seconds.
            entry.source_system_timestamps.create_time.seconds = round(
                create_time / 1000)

            if not update_time:
                update_time = create_time

            # Transform millis to seconds. ADD 10 seconds because
            entry.source_system_timestamps.update_time.seconds = round(
                update_time / 1000)
        else:
            logging.info('Entity "%s" has no created_time information!',
                         generated_id)

    @classmethod
    def __create_schema(cls, entry, columns):
        entry_columns = []
        if columns:
            for column in columns:
                column_data = column.get('data')
                if column_data:
                    column_attributes = column_data.get('attributes')
                    data_type = attr_normalizer.\
                        DataCatalogAttributeNormalizer.\
                        get_column_data_type(column_attributes)
                    column_name = column_attributes.get('name')
                    column_desc = column_attributes.get('comment')
                    if data_type and column_name:
                        column_name = attr_normalizer.\
                            DataCatalogAttributeNormalizer.format_name(
                                column_name)
                        entry_columns.append(
                            datacatalog.types.ColumnSchema(
                                column=column_name,
                                description=column_desc,
                                type=data_type,
                                mode=None))
        entry.schema.columns.extend(entry_columns)
