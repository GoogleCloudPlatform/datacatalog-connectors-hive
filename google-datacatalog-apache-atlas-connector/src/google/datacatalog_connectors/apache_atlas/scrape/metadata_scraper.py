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

from google.datacatalog_connectors.apache_atlas import scrape


class MetadataScraper:

    def __init__(self, connection_args):
        self._apache_atlas_facade = scrape.apache_atlas_facade.\
            ApacheAtlasFacade(connection_args)
        self.__metadata_enricher = scrape.metadata_enricher.\
            MetadataEnricher(self._apache_atlas_facade)

    def get_metadata(self, **kwargs):
        self._log_scrape_start('Scraping all Metadata...')
        classifications_dict = {}
        entity_types_dict = {}
        enum_types_dict = {}

        self._log_scrape_start('Scraping admin metrics...')
        admin_metrics = self._apache_atlas_facade.get_admin_metrics()
        logging.info(admin_metrics)
        self._log_single_object_scrape_result(admin_metrics)

        self._log_scrape_start('Scraping typedefs...')
        for typedef in self._apache_atlas_facade.get_typedefs():
            self._scrape_classification_types(classifications_dict, typedef)

            self._scrape_enum_types(enum_types_dict, typedef)

            self._scrape_entity_types(entity_types_dict, typedef)

        self.__metadata_enricher.enrich_entity_relationships(entity_types_dict)

        return {
            'classifications': classifications_dict,
            'enum_types': enum_types_dict,
            'entity_types': entity_types_dict
        }, None

    def _scrape_entity_types(self, entity_types_dict, typedef):
        self._log_scrape_start('Scraping EntityTypes...')
        for entity_type in typedef.entityDefs:
            entity_type_name = entity_type.name

            entity_type_dict = {
                'name': entity_type_name,
                'data': entity_type._data,
                'superTypes': entity_type.superTypes,
                'entities': {}
            }

            entities = self.__scrape_entity_type(entity_type)
            entity_type_dict['entities'] = entities

            entity_types_dict[entity_type_name] = entity_type_dict

    def _scrape_classification_types(self, classifications_dict, typedef):
        self._log_scrape_start('Scraping Classifications/Templates...')
        for classification_type in typedef.classificationDefs:
            classification_data = classification_type._data
            logging.info('Classification: %s', classification_type.name)
            logging.debug(classification_data)
            classifications_dict[classification_type.name] = {
                'name': classification_type.name,
                'guid': classification_type.guid,
                'data': classification_data
            }

    def _scrape_enum_types(self, enum_types_dict, typedef):
        self._log_scrape_start('Scraping Enum types...')
        for enum_type in typedef.enumDefs:
            enum_data = enum_type._data
            logging.info('Enum type: %s', enum_type.name)
            logging.debug(enum_data)
            enum_types_dict[enum_type.name] = {
                'name': enum_type.name,
                'guid': enum_type.guid,
                'data': enum_data
            }

    def __scrape_entity_type(self, entity_type):
        searched_entries = {}
        entity_type_name = entity_type.name

        logging.info('=> Entity Type: %s', entity_type_name)
        logging.debug(entity_type._data)

        search_results = self._apache_atlas_facade.\
            search_entities_from_entity_type(entity_type_name)

        guids = []
        for entity in search_results:
            # Collecting guids and storing entity to enricher data later on.
            guid = entity.guid
            guids.append(guid)
            searched_entries[guid] = {'guid': guid, 'data': entity._data}

        fetched_entities_dict = {}
        if guids:
            fetched_entities_dict = self._apache_atlas_facade.fetch_entities(
                guids)
            self.__metadata_enricher.enrich_entity_classifications(
                fetched_entities_dict, searched_entries)

        logging.info('Entity Type: %s scrapped!', entity_type_name)
        logging.info('')
        return fetched_entities_dict

    @classmethod
    def _log_scrape_start(cls, message, *args):
        logging.info('')
        logging.info(message, *args)
        logging.info('-------------------------------------------------')

    @classmethod
    def _log_single_object_scrape_result(cls, the_object):
        logging.info('Found!' if the_object else 'NOT found!')
