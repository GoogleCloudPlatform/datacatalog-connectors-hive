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
import json
import uuid

from google.datacatalog_connectors.commons.cleanup \
    import datacatalog_metadata_cleaner
from google.datacatalog_connectors.commons.ingest \
    import datacatalog_metadata_ingestor
from google.datacatalog_connectors.commons.monitoring \
    import metrics_processor
from google.datacatalog_connectors.hive import entities
from google.datacatalog_connectors.hive import scrape
from google.datacatalog_connectors.hive.prepare import \
    assembled_entry_factory


class DataCatalogSynchronizer:

    __CLEAN_UP_EVENTS = [
        entities.SyncEvent.DROP_DATABASE, entities.SyncEvent.DROP_TABLE
    ]

    def __init__(self,
                 project_id,
                 location_id,
                 hive_metastore_db_host=None,
                 hive_metastore_db_user=None,
                 hive_metastore_db_pass=None,
                 hive_metastore_db_name=None,
                 hive_metastore_db_type=None,
                 metadata_sync_event=None,
                 enable_monitoring=None):
        self.__entry_group_id = 'hive'
        self.__project_id = project_id
        self.__location_id = location_id
        self.__hive_metastore_db_host = hive_metastore_db_host
        self.__hive_metastore_db_user = hive_metastore_db_user
        self.__hive_metastore_db_pass = hive_metastore_db_pass
        self.__hive_metastore_db_name = hive_metastore_db_name
        self.__hive_metastore_db_type = hive_metastore_db_type
        self.__metadata_sync_event = metadata_sync_event
        self.__task_id = uuid.uuid4().hex[:8]
        self.__metrics_processor = metrics_processor.MetricsProcessor(
            project_id, location_id, self.__entry_group_id, enable_monitoring,
            self.__task_id)

    def run(self):
        logging.info('\n==============Start hive-to-datacatalog============')

        logging.info('\n\n==============Scrape metadata===============')
        if self.__metadata_sync_event:
            databases_metadata = \
                scrape.MetadataSyncEventScraper.get_database_metadata(
                    self.__metadata_sync_event)
            sync_event = entities.SyncEvent[
                self.__metadata_sync_event['event']]
            # If we have a metadata_sync_event then we
            # use the host_name that created the event
            host_name = self.__metadata_sync_event['hostName']
        else:
            host_name = self.__hive_metastore_db_host
            metadata_database_scraper = scrape.MetadataDatabaseScraper(
                host_name, self.__hive_metastore_db_user,
                self.__hive_metastore_db_pass, self.__hive_metastore_db_name,
                self.__hive_metastore_db_type)
            databases_metadata = \
                metadata_database_scraper.get_database_metadata()
            sync_event = entities.SyncEvent.MANUAL_DATABASE_SYNC

        logging.info('\n--> {}'.format(sync_event))

        logging.info('\n{}'.format(len(databases_metadata['databases'])) +
                     ' databases ready to be ingested...')

        logging.info('\n\n==============Prepare metadata===============')
        # Prepare.
        logging.info('\nPreparing the metadata...')

        factory = assembled_entry_factory.AssembledEntryFactory(
            self.__project_id, self.__location_id, host_name,
            self.__entry_group_id)
        prepared_entries = factory.make_entries_from_database_metadata(
            databases_metadata)

        self.__log_metadata(databases_metadata)
        self.__log_entries(prepared_entries)

        logging.info('\n==============Ingest metadata===============')

        cleaner = datacatalog_metadata_cleaner.DataCatalogMetadataCleaner(
            self.__project_id, self.__location_id, self.__entry_group_id)

        # Since we can't rely on search returning the ingested entries,
        # we clean up the obsolete entries before ingesting.
        if sync_event == entities.SyncEvent.MANUAL_DATABASE_SYNC:
            assembled_entries_data = []
            for database_entry, table_related_entries in prepared_entries:
                assembled_entries_data.extend(
                    [database_entry, *table_related_entries])

            cleaner.delete_obsolete_metadata(
                assembled_entries_data,
                'system={}'.format(self.__entry_group_id))

            del assembled_entries_data

        # Ingest.
        logging.info('\nStarting to ingest custom metadata...')
        if sync_event not in self.__CLEAN_UP_EVENTS:
            self.__ingest_created_or_updated(prepared_entries)
        elif sync_event == entities.SyncEvent.DROP_DATABASE:
            self.__cleanup_deleted_databases(cleaner, prepared_entries)
        elif sync_event == entities.SyncEvent.DROP_TABLE:
            self.__cleanup_deleted_tables(cleaner, prepared_entries)

        logging.info('\n==============End hive-to-datacatalog===============')
        self.__after_run()

        return self.__task_id

    def __ingest_created_or_updated(self, prepared_entries):
        ingestor = datacatalog_metadata_ingestor.DataCatalogMetadataIngestor(
            self.__project_id, self.__location_id, self.__entry_group_id)
        for database_entry, table_related_entries in prepared_entries:
            ingestor.ingest_metadata([database_entry, *table_related_entries])

    def __after_run(self):
        self.__metrics_processor.process_elapsed_time_metric()

    def __log_entries(self, prepared_entries):
        entries_len = sum([len(tables) for (_, tables) in prepared_entries],
                          len(prepared_entries))
        self.__metrics_processor.process_entries_length_metric(entries_len)

    def __log_metadata(self, metadata):
        # sqlalchemy uses a object proxy, so we must convert it
        # before generating the metric.
        databases_json = json.dumps(
            ([database.dump() for database in metadata['databases']]))
        self.__metrics_processor.process_metadata_payload_bytes_metric(
            databases_json)

    @classmethod
    def __cleanup_deleted_tables(cls, cleaner, prepared_entries):
        for _, table_related_entries in prepared_entries:
            try:
                cleaner.delete_metadata(table_related_entries)
                logging.info('\nTables deleted: {}'.format(
                    len(table_related_entries)))
            except:  # noqa: E722
                logging.info('Exception deleting Entries')

    @classmethod
    def __cleanup_deleted_databases(cls, cleaner, prepared_entries):
        for database_entry, _ in prepared_entries:
            databases = [database_entry]
            try:
                cleaner.delete_metadata(databases)
                logging.info('\nDatabases deleted: {}'.format(len(databases)))
            except:  # noqa: E722
                logging.info('Exception deleting Entries')
