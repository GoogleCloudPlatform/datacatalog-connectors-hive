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

from google.datacatalog_connectors.hive import entities
from google.datacatalog_connectors.hive.entities import sync_event


class MetadataSyncEventScraper:

    @classmethod
    def get_database_metadata(cls, message):
        event = message.get('event')
        if not event:
            raise Exception('Message does not contain a event type')

        if event not in [event.name for event in sync_event.SyncEvent]:
            raise Exception('Unsupported event type: {}'.format(event))

        if event == sync_event.SyncEvent.CREATE_TABLE.name:
            return cls.__build_metadata_entities_for_create_table_event(
                message)

        if event == sync_event.SyncEvent.ALTER_TABLE.name:
            return cls.__build_metadata_entities_for_update_table_event(
                message)

        if event == sync_event.SyncEvent.CREATE_DATABASE.name:
            return cls.__build_metadata_entities_for_create_database_event(
                message)

        if event == sync_event.SyncEvent.DROP_TABLE.name:
            return cls.__build_metadata_entities_for_drop_table_event(message)

        if event == sync_event.SyncEvent.DROP_DATABASE.name:
            return cls.__build_metadata_entities_for_drop_database_event(
                message)

    @classmethod
    def __build_metadata_entities_for_create_table_event(cls, message):
        database, table = cls.__build_common_metadata_fields(message['table'])
        table.table_params = []

        tables = [table]
        database.tables = tables
        return {'databases': [database]}

    @classmethod
    def __build_metadata_entities_for_create_database_event(cls, message):
        database, _ = cls.__build_database_fields(message['database'])
        database.tables = []
        return {'databases': [database]}

    @classmethod
    def __build_metadata_entities_for_drop_table_event(cls, message):
        database, table = cls.__build_common_metadata_fields(message['table'])
        table.table_params = []

        tables = [table]
        database.tables = tables
        return {'databases': [database]}

    @classmethod
    def __build_metadata_entities_for_drop_database_event(cls, message):
        database, _ = cls.__build_database_fields(message['database'])
        database.tables = []
        return {'databases': [database]}

    @classmethod
    def __build_metadata_entities_for_update_table_event(cls, message):
        new_table = message['newTable']
        database, table = cls.__build_common_metadata_fields(
            message['newTable'])
        parameters_message = new_table['parameters']

        table_param = entities.TableParams()
        table_param.id = 1
        table_param.param_key = 'last_modified_time'
        table_param.param_value = parameters_message['last_modified_time']

        table.table_params = [table_param]

        tables = [table]
        database.tables = tables
        return {'databases': [database]}

    @classmethod
    def __build_common_metadata_fields(cls, table_message):
        database = entities.Database()
        database.id = None
        database.name = table_message['dbName']
        table = entities.Table()
        table.id = None
        table.name = table_message['tableName']
        table.type = 'table'
        table.create_time = table_message['createTime']
        table.database_id = None
        table.sd_id = None
        storage_message = table_message['sd']
        table_storage = entities.TableStorage()
        table_storage.sd_id = None
        table_storage.location = storage_message['location']
        table_storage.cd_id = None
        cols_message = storage_message['cols']
        columns = []
        for col_message in cols_message:
            column = entities.Column()

            column.id = None
            column.name = col_message['name']
            column.type = col_message['type']
            column.comment = col_message['comment']
            columns.append(column)
        table_storage.columns = columns
        table_storages = [table_storage]
        table.table_storages = table_storages
        return database, table

    @classmethod
    def __build_database_fields(cls, database_message):
        database = entities.Database()
        database.id = None
        database.name = database_message['name']
        database.uri = database_message['locationUri']
        return database, None
