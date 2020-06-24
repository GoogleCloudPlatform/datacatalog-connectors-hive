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

from atlasclient import client


class ApacheAtlasFacade:
    """Apache Atlas API communication facade."""
    __DELETED_ENTITY_STATUS = 'DELETED'
    __MAX_CHUNK_SIZE = 300

    def __init__(self, connection_args):
        # Initialize the API client.
        self.__apache_atlas = client.Atlas(connection_args['host'],
                                           port=connection_args['port'],
                                           username=connection_args['user'],
                                           password=connection_args['pass'])

    def get_admin_metrics(self):
        metrics = []
        for metric in self.__apache_atlas.admin_metrics:
            entityActive = metric.entity['entityActive']
            logging.debug('Active entities: %s', entityActive)

            entityDeleted = metric.entity['entityDeleted']
            logging.debug('Deleted entities: %s', entityDeleted)

            generalMetrics = metric.general
            logging.debug('General metrics: %s', generalMetrics)
            metrics.append({
                'entityActive': entityActive,
                'entityDeleted': entityDeleted,
                'generalMetrics': generalMetrics
            })
        return metrics

    def get_typedefs(self):
        return self.__apache_atlas.typedefs

    def search_entities_from_entity_type(self, entity_type_name):
        logging.info('Searching for entities from entity_type: %s',
                     entity_type_name)

        fetched_search_results = []
        params = {'typeName': entity_type_name, 'offset': 0, 'limit': 100}
        search_results = self.__apache_atlas.search_dsl(**params)

        # Fetch lazy response
        search_results = [
            entity for s in search_results for entity in s.entities
        ]
        fetched_search_results.extend(search_results)

        # Keep fetching search_results to retrieve all the pages
        while search_results:
            params['offset'] = params['offset'] + params['limit']

            logging.debug('offset: %s, limit: %s', params['offset'],
                          params['limit'])

            search_results = self.__apache_atlas.search_dsl(**params)
            # Fetch lazy response
            search_results = [
                entity for s in search_results for entity in s.entities
            ]
            fetched_search_results.extend(search_results)

        # Filter out deleted results and subtypes, only return the actual type.
        fetched_search_results = [
            result for result in fetched_search_results
            if result._data['status'] != self.__DELETED_ENTITY_STATUS and
            result._data['typeName'] == entity_type_name
        ]

        logging.info('Returned: %s entities from Search',
                     len(fetched_search_results))

        return fetched_search_results

    def fetch_entities(self, guids):
        logging.info('Retreiving all entries from guids: %s', guids)
        entities_dict = {}

        # We chunk the limit because of the request size limit
        for chunked_guids in self.__chunk_list(guids):
            bulk_collection = self.__apache_atlas.entity_bulk(
                guid=chunked_guids)
            for collection in bulk_collection:
                entities = collection.entities()
                for fetched_entity in entities:
                    guid = fetched_entity.guid
                    entity_data = fetched_entity._data
                    logging.debug(entity_data)
                    entities_dict[guid] = {'guid': guid, 'data': entity_data}
        logging.info('Returned: %s entities from Fetch', len(entities_dict))
        return entities_dict

    def fetch_entity_classifications(self, guid):
        returned_entity = self.__apache_atlas.entity_guid(guid)

        try:
            # Lazy return, we need to fetch it
            cursor = next(returned_entity.classifications)
            classification_list = cursor._data.get('list')
            classification_list = self.__remove_duplicates_classification_list(
                classification_list, guid)
            return classification_list
        except:  # noqa: E722
            logging.exception('Error fetching entity classifications')

    @classmethod
    def __remove_duplicates_classification_list(cls, classification_list,
                                                entity_guid):
        classification_dict = {}

        for classification in classification_list:
            type_name = classification['typeName']
            classification_entity_guid = classification['entityGuid']

            duplicated_classification = classification_dict.get(type_name)

            if duplicated_classification:
                # We make sure we are keeping the classification
                # at the entity levelin the scenario there's a
                # classification with the same name for a super type.
                if entity_guid == classification_entity_guid:
                    classification_dict[type_name] = classification
            else:
                classification_dict[type_name] = classification

        return list(classification_dict.values())

    @classmethod
    def __chunk_list(cls, list_arg):
        for i in range(0, len(list_arg), cls.__MAX_CHUNK_SIZE):
            yield list_arg[i:i + cls.__MAX_CHUNK_SIZE]
