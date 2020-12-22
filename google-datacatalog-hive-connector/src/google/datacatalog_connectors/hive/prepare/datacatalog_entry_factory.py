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
from google.protobuf import timestamp_pb2

from google.datacatalog_connectors.commons.prepare import base_entry_factory


class DataCatalogEntryFactory(base_entry_factory.BaseEntryFactory):
    __ENTRY_ID_INVALID_CHARS_REGEX_PATTERN = r'[^a-zA-Z0-9_]+'

    def __init__(self, project_id, location_id, metadata_host_server,
                 entry_group_id):
        self.__project_id = project_id
        self.__location_id = location_id
        self.__metadata_host_server = metadata_host_server
        self.__entry_group_id = entry_group_id

    def make_entries_for_database(self, database_metadata):
        entry_id = self._format_id_with_hashing(
            database_metadata.name.lower(),
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

        entry = datacatalog.Entry()

        entry.user_specified_type = 'database'
        entry.user_specified_system = 'hive'

        entry.display_name = self._format_display_name(database_metadata.name)

        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        database_desc = database_metadata.desc
        if isinstance(database_desc, str):
            entry.description = database_desc
        entry.linked_resource = \
            self._format_linked_resource('//{}//{}'.format(
                self.__metadata_host_server,
                database_metadata.uri
            ))

        return entry_id, entry

    def make_entry_for_table(self, table_metadata, database_name):
        entry_id = self.__make_entry_id_for_table(database_name,
                                                  table_metadata)

        entry = datacatalog.Entry()

        entry.user_specified_type = 'table'
        entry.user_specified_system = 'hive'

        entry.display_name = self._format_display_name(table_metadata.name)

        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        table_storage = table_metadata.table_storages[0]

        entry.linked_resource = \
            self._format_linked_resource(
                '//{}//{}'.format(self.__metadata_host_server,
                                  table_storage.location))

        created_timestamp = timestamp_pb2.Timestamp()
        created_timestamp.FromSeconds(table_metadata.create_time)

        entry.source_system_timestamps.create_time = created_timestamp

        update_time_seconds = \
            DataCatalogEntryFactory. \
            __extract_update_time_from_table_metadata(table_metadata)
        if update_time_seconds is not None:
            updated_timestamp = timestamp_pb2.Timestamp()
            updated_timestamp.FromSeconds(update_time_seconds)

            entry.source_system_timestamps.update_time = updated_timestamp
        else:
            entry.source_system_timestamps.update_time = created_timestamp

        columns = []
        for column in table_storage.columns:
            columns.append(
                datacatalog.ColumnSchema(
                    column=column.name,
                    type=DataCatalogEntryFactory.__format_entry_column_type(
                        column.type),
                    description=column.comment))
        entry.schema.columns.extend(columns)

        return entry_id, entry

    def __make_entry_id_for_table(self, database_name, table_metadata):
        # We normalize and hash first the database_name.
        normalized_database_name = self._format_id_with_hashing(
            database_name.lower(),
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

        # Next we do the same for the table name.
        normalized_table_name = self._format_id_with_hashing(
            table_metadata.name.lower(),
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

        entry_id = '{}__{}'.format(normalized_database_name,
                                   normalized_table_name)

        # Then we hash the combined result again to make sure it
        # does not hit the 64 chars limit.
        return self._format_id_with_hashing(
            entry_id,
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

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
