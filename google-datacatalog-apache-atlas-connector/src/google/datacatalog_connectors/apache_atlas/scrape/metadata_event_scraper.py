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

import json
import logging

from google.datacatalog_connectors.apache_atlas.scrape import \
    apache_atlas_event_facade
from google.datacatalog_connectors.apache_atlas.scrape import \
    metadata_event_enricher
from google.datacatalog_connectors.apache_atlas.scrape import \
    metadata_enricher
from google.datacatalog_connectors.apache_atlas.scrape import \
    metadata_scraper

from google.datacatalog_connectors.apache_atlas.prepare import constant


class MetadataEventScraper(metadata_scraper.MetadataScraper):

    def __init__(self, connection_args):
        super().__init__(connection_args)
        self.__apache_atlas_event_facade = apache_atlas_event_facade.\
            ApacheAtlasEventFacade(connection_args)
        self.__metadata_event_enricher = metadata_event_enricher.\
            MetadataEventEnricher(self._apache_atlas_facade)

    def get_metadata(self, **kwargs):
        entity_events = None
        if kwargs:
            entity_events = kwargs.get('entity_events')
        return self.__scrape_entity_events(entity_events)

    def __scrape_entity_events(self, entity_events=None):
        entity_events, event_consumer = self.__retrieve_entity_events(
            entity_events)

        if entity_events:
            logging.info('Scraping Entity Events...')

            self._log_scrape_start(
                'Scraping Metadata for {} entity events...'.format(
                    len(entity_events)))
            classifications_dict = {}
            entity_types_dict = {}
            enum_types_dict = {}

            self._log_scrape_start('Scraping admin metrics...')
            admin_metrics = self._apache_atlas_facade.get_admin_metrics()
            logging.info(admin_metrics)
            self._log_single_object_scrape_result(admin_metrics)

            self._log_scrape_start('Scraping typedefs...')
            for typedef in self._apache_atlas_facade.get_typedefs():
                self._scrape_classification_types(classifications_dict,
                                                  typedef)

                self._scrape_enum_types(enum_types_dict, typedef)

                self.__scrape_entity_types_for_events(entity_events,
                                                      entity_types_dict,
                                                      typedef)

            metadata_enricher.MetadataEnricher.enrich_entity_relationships(
                entity_types_dict)

            return {
                'classifications': classifications_dict,
                'enum_types': enum_types_dict,
                'entity_types': entity_types_dict,
                'entity_events': entity_events
            }, event_consumer

        else:
            logging.info('No Entity Events...')
            return {
                'classifications': {},
                'enum_types': {},
                'entity_types': {},
                'entity_events': []
            }, event_consumer

    def __retrieve_entity_events(self, entity_events=None):
        # In case we receive the entity_events we don't create
        # the event consumer.
        if entity_events:
            return entity_events, None

        if not entity_events:
            entity_events = []

            event_consumer = self.__apache_atlas_event_facade.\
                create_event_consumer()

            for msg in event_consumer:
                if msg:
                    logging.info("Event %s:%s:%s: key=%s ", msg.topic,
                                 msg.partition, msg.offset, msg.key)
                    entity_event = json.loads(msg.value)
                    entity_events.append(entity_event['message'])
            return entity_events, event_consumer

    def __scrape_entity_types_for_events(self, entity_events,
                                         entity_types_dict, typedef):
        types_event_dict = self.__create_types_event_dict(entity_events)

        self._log_scrape_start('Scraping EntityTypes...')
        for entity_type in typedef.entityDefs:
            entity_type_name = entity_type.name

            entity_type_dict = {
                'name': entity_type_name,
                'data': entity_type._data,
                'superTypes': entity_type.superTypes,
                'entities': {}
            }

            # Enrich entity info for the event type
            if entity_type_name in types_event_dict.keys():
                event_guids = types_event_dict.get(entity_type_name)

                entities = self.__metadata_event_enricher.\
                    enrich_entities_attributes_and_classifications(
                        event_guids)

                self.__metadata_event_enricher.\
                    enrich_entity_types_relationships(
                        entities, entity_types_dict)

                entity_type_dict['entities'] = entities

            entity_types_dict[entity_type_name] = entity_type_dict

    @classmethod
    def __create_types_event_dict(cls, entity_events):
        types_event_dict = {}

        for entity_event in entity_events:
            entity = entity_event['entity']
            operation_type = entity_event['operationType']

            # We don't scrape metadata for delete events
            if operation_type != constant.ENTITY_DELETE_EVENT:
                guid = entity['guid']
                type_name = entity['typeName']

                guids = types_event_dict.get(type_name)

                if not guids:
                    guids = []

                guids.append(guid)

                types_event_dict[type_name] = guids

        return types_event_dict
