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

from google.cloud import datacatalog_v1beta1


class DataCatalogEntryFactory:

    def __init__(self, project_id, location_id, metadata_host_server,
                 entry_group_id):
        self.__project_id = project_id
        self.__location_id = location_id
        self.__metadata_host_server = metadata_host_server
        self.__entry_group_id = entry_group_id

    def make_entries_for_database(self, database_metadata):
        entry_id = '{}'.format(database_metadata.name)
        # Force lowercase since hive is case insensitive
        entry_id = entry_id.lower()

        entry = datacatalog_v1beta1.types.Entry()

        entry.user_specified_type = 'database'
        entry.user_specified_system = 'hive'

        entry.display_name = database_metadata.name

        entry.name = datacatalog_v1beta1.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        database_desc = database_metadata.desc
        if isinstance(database_desc, str):
            entry.description = database_desc
        entry.linked_resource = \
            '//{}//{}'.format(
                self.__metadata_host_server,
                database_metadata.uri
            )

        return entry_id, entry

    def make_entry_for_table(self, table_metadata, database_name):
        entry_id = '{}__{}'.format(database_name, table_metadata.name)
        # Force lowercase since hive is case insensitive
        entry_id = entry_id.lower()

        entry = datacatalog_v1beta1.types.Entry()

        entry.user_specified_type = 'table'
        entry.user_specified_system = 'hive'

        entry.display_name = table_metadata.name

        entry.name = datacatalog_v1beta1.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        # For now we are using the first table_storage relationship,
        # with table partitions we might have to deal
        # with more than one record
        table_storage = table_metadata.table_storages[0]

        entry.linked_resource = \
            '//{}//{}'.format(self.__metadata_host_server,
                              table_storage.location)

        entry.source_system_timestamps. \
            create_time.seconds = table_metadata.create_time
        update_time_seconds = \
            DataCatalogEntryFactory. \
            __extract_update_time_from_table_metadata(table_metadata)
        if update_time_seconds is not None:
            entry.source_system_timestamps.update_time.seconds = \
                update_time_seconds
        else:
            entry.source_system_timestamps.update_time.seconds = \
                table_metadata.create_time

        columns = []
        for column in table_storage.columns:
            columns.append(
                datacatalog_v1beta1.types.ColumnSchema(
                    column=column.name,
                    type=DataCatalogEntryFactory.__format_entry_column_type(
                        column.type),
                    description=column.comment,
                    mode=None))
        entry.schema.columns.extend(columns)

        return entry_id, entry

    @staticmethod
    def __extract_update_time_from_table_metadata(table_metadata):
        try:
            param = next([
                param for param in table_metadata.table_params
                if param.param_key == 'last_modified_time'
            ].__iter__())
            return int(param.param_value)
        except StopIteration:
            return None

    @staticmethod
    def __format_entry_column_type(source_name):
        formatted_name = source_name.replace('&', '_')
        formatted_name = formatted_name.replace(':', '_')
        formatted_name = formatted_name.replace('/', '_')
        return formatted_name
