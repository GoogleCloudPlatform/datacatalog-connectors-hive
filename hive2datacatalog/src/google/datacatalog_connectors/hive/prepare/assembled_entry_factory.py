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

from google.datacatalog_connectors.hive.prepare import\
    datacatalog_entry_factory


class AssembledEntryFactory:

    def __init__(self, project_id, location_id, metadata_host_server,
                 entry_group_id):
        self.__datacatalog_entry_factory = \
            datacatalog_entry_factory.DataCatalogEntryFactory(
                project_id,
                location_id,
                metadata_host_server,
                entry_group_id)

    def make_entries_from_database_metadata(self, metadata_dict):
        assmbled_entries = []
        databases = metadata_dict['databases']
        for database in databases:
            database_name = database.name
            tables = database.tables
            assembled_database = self.__make_entries_for_database(database)

            logging.info('\n--> Database: %s', database_name)
            logging.info('\n%s tables ready to be ingested...', len(tables))
            assembled_tables = self.__make_entry_for_tables(
                tables, database_name)

            assmbled_entries.append((assembled_database, assembled_tables))
        return assmbled_entries

    def __make_entries_for_database(self, database_dict):
        entry_id, entry = self.\
            __datacatalog_entry_factory.make_entries_for_database(
                database_dict)

        return prepare.AssembledEntryData(entry_id, entry)

    def __make_entry_for_tables(self, tables_dict, database_name):
        entries = []
        for table_dict in tables_dict:
            entry_id, entry = self.\
                __datacatalog_entry_factory.make_entry_for_table(table_dict,
                                                                 database_name)

            entries.append(prepare.AssembledEntryData(entry_id, entry))
        return entries
