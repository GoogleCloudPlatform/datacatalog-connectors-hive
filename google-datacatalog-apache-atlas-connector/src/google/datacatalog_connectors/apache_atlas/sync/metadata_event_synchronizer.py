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
from time import sleep

from google.cloud.datacatalog import enums
from google.datacatalog_connectors.commons import \
    datacatalog_facade

from google.datacatalog_connectors.apache_atlas.prepare import constant
from google.datacatalog_connectors.apache_atlas import scrape
from google.datacatalog_connectors.apache_atlas.sync import \
    metadata_synchronizer


class MetadataEventSynchronizer(metadata_synchronizer.MetadataSynchronizer):
    __EVENT_POLL_SLEEP_TIME_SECONDS = 5
    __STRING_TYPE = enums.FieldType.PrimitiveType.STRING

    def __init__(self,
                 datacatalog_project_id,
                 datacatalog_location_id,
                 atlas_connection_args,
                 atlas_entity_types=None,
                 enable_monitoring=None):
        super().__init__(datacatalog_project_id, datacatalog_location_id,
                         atlas_connection_args, atlas_entity_types,
                         enable_monitoring)

        self._metadata_scraper = scrape.MetadataEventScraper(
            atlas_connection_args)

        self.__datacatalog_facade = datacatalog_facade.DataCatalogFacade(
            self._project_id)

    def run(self):
        logging.info(
            '===> Event hook execution, will keep polling for new events...')
        while True:
            self.__run()
            logging.info('===> Sleeping...')
            sleep(self.__EVENT_POLL_SLEEP_TIME_SECONDS)

    def __run(self):
        """Coordinates a full scrape > prepare > ingest process."""
        logging.info('')
        logging.info('===> Scraping Apache Atlas metadata...')
        metadata_dict, event_consumer = self._metadata_scraper.get_metadata()
        self.__process_column_events(metadata_dict)
        self._log_metadata(metadata_dict)
        tag_templates_dict = self._make_tag_templates_dict(metadata_dict)
        assembled_entries = self._make_assembled_entries(
            metadata_dict, self._atlas_entity_types)
        logging.info('==== DONE ========================================')
        logging.info('')
        self._sync_assembled_entries(assembled_entries, tag_templates_dict)
        self.__process_delete_events(metadata_dict)
        self.__ack_event_consumer(event_consumer)
        self._after_run()

    def __process_column_events(self, metadata_dict):
        # Apache Atlas column changes events does not contain tables
        # relationships on it
        # so we have to enrich the table metadata for column events
        table_guids = self.__get_table_entities_for_column_events(
            metadata_dict)
        table_events = self.__create_table_events(table_guids)
        if table_events:
            logging.info('')
            logging.info('===> Column events, scraping metadata of'
                         ' related tables {}...'.format(table_guids))
            metadata_dict_table, _ = self._metadata_scraper.get_metadata(
                entity_events=table_events)
            self.__merge_metadata_dict(metadata_dict, metadata_dict_table)

            scrape.MetadataEnricher.enrich_entity_relationships(
                metadata_dict['entity_types'])

    def __process_delete_events(self, metadata_dict):
        logging.info('')
        logging.info('===> Processing delete events...')

        entity_events = metadata_dict.get('entity_events')

        if entity_events:
            for entity_event in entity_events:
                operation_type = entity_event['operationType']
                if operation_type == constant.ENTITY_DELETE_EVENT:
                    entity = entity_event['entity']
                    guid = entity['guid']

                    entries_resource_names = self.\
                        __get_resource_names_for_delete_events(guid)

                    for entries_resource_name in entries_resource_names:
                        self.__datacatalog_facade.\
                            delete_entry(entries_resource_name)
        logging.info('==== DONE ========================================')
        logging.info('')

    def __get_resource_names_for_delete_events(self, guid):
        query_template = 'system={} tag:instance_url:{}' \
                         ' tag:guid:{}'
        query = query_template.format(self._SPECIFIED_SYSTEM,
                                      self._instance_url, guid)
        entries_resource_names = self.__datacatalog_facade. \
            search_catalog_relative_resource_name(query)
        return entries_resource_names

    def __merge_metadata_dict(self, metadata_dict, metadata_dict_table):
        target_table_type = metadata_dict['entity_types'].get('Table')
        source_table_type = metadata_dict_table['entity_types'].get('Table')
        if target_table_type and source_table_type:
            target_table_type['entities'].update(source_table_type['entities'])

    def __get_table_entities_for_column_events(self, metadata_dict):
        entity_events = metadata_dict.get('entity_events')

        if entity_events:
            for entity_event in entity_events:
                entity = entity_event['entity']
                operation_type = entity_event['operationType']

                guid = entity['guid']
                type_name = entity['typeName']

                # We don't scrape metadata for delete events
                if type_name == constant.ATLAS_COLUMN_TYPE and \
                        operation_type != constant.ENTITY_DELETE_EVENT:
                    column_guid = guid

                    query_template = 'system={} tag:instance_url:{}' \
                                     ' type=table tag:column_guid:{}'
                    query = query_template.format(self._SPECIFIED_SYSTEM,
                                                  self._instance_url,
                                                  column_guid)

                    return self.__datacatalog_facade.\
                        get_tag_field_values_for_search_results(
                            query, 'apache_atlas_entity_type_table', 'guid',
                            self.__STRING_TYPE)

    @classmethod
    def __create_table_events(cls, table_guids):
        entity_events = []
        if table_guids:
            for table_guid in table_guids:
                entity_event = {}
                entity = {'guid': table_guid, 'typeName': 'Table'}
                entity_event['entity'] = entity
                entity_event['operationType'] = constant.ENTITY_SYNC_EVENT

                entity_events.append(entity_event)

        return entity_events

    @classmethod
    def __ack_event_consumer(cls, event_consumer):
        # If it's a successful ingestion we ACK
        # the messages by closing the consumer.
        if event_consumer:
            event_consumer.commit()
            event_consumer.close()
