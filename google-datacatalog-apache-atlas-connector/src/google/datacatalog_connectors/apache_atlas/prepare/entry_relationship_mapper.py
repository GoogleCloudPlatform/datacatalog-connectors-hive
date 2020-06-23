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

from google.datacatalog_connectors.commons import prepare
from google.datacatalog_connectors.apache_atlas.prepare import constant


class EntryRelationshipMapper(prepare.BaseEntryRelationshipMapper):
    __COLUMN = 'column'
    __DB = 'db'
    __SD = 'storagedesc'
    __TABLE = 'table'
    __VIEW = 'view'

    def fulfill_tag_fields(self, assembled_entries_data):
        resolvers = (self.__resolve_table_mappings,
                     self.__resolve_view_mappings)

        self._fulfill_tag_fields(assembled_entries_data, resolvers)

    @classmethod
    def _get_asset_identifier_tag_field_key(cls):
        return constant.ENTITY_GUID

    @classmethod
    def __resolve_table_mappings(cls, assembled_entries_data, id_name_pairs):
        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if entry.user_specified_type == cls.__TABLE:
                cls._map_related_entry(assembled_entry_data, cls.__DB,
                                       'db_guid', 'db_entry', id_name_pairs)
                cls._map_related_entry(assembled_entry_data, cls.__SD,
                                       'sd_guid', 'sd_entry', id_name_pairs)
                cls._map_related_entry(assembled_entry_data, cls.__COLUMN,
                                       'column_guid', 'column_entry',
                                       id_name_pairs)

    @classmethod
    def __resolve_view_mappings(cls, assembled_entries_data, id_name_pairs):
        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if entry.user_specified_type == cls.__VIEW:
                cls._map_related_entry(assembled_entry_data, cls.__DB,
                                       'db_guid', 'db_entry', id_name_pairs)
