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


class MetadataEventEnricher:

    def __init__(self, apache_atlas_facade):
        self.__apache_atlas_facade = apache_atlas_facade

    def enrich_entities_attributes_and_classifications(self, guids):
        entities_dict = self.__apache_atlas_facade.fetch_entities(guids)
        for guid in guids:
            fetched_entity_dict = entities_dict.get(guid)

            if fetched_entity_dict:
                fetched_entity_dict[
                    'classifications'] = self.__apache_atlas_facade.\
                    fetch_entity_classifications(guid)
        logging.info('Entities: %s scrapped!', guids)
        logging.info('')
        return entities_dict

    def enrich_entity_types_relationships(self, entities, entity_types_dict):
        # We need to enrich entity types relationships because the event
        # does not contain it.
        self.__enrich_event_relationships(entities, entity_types_dict)

    def __enrich_event_relationships(self, entities, entity_types_dict):
        for guid, entity in entities.items():
            data = entity['data']
            attributes = data['attributes']

            for key, attribute in attributes.items():
                if isinstance(attribute, list):
                    for item in attribute:
                        self.__enrich_relationship_dict(
                            item, entity_types_dict)
                else:
                    self.__enrich_relationship_dict(attribute,
                                                    entity_types_dict)

    def __enrich_relationship_dict(self, attribute_dict, entity_types_dict):
        if isinstance(attribute_dict, dict):
            type_name = attribute_dict.get('typeName')
            guid = attribute_dict.get('guid')
            data = attribute_dict.get('data')
            # Verify if the attribute implements an entity type
            # and if the attribute data is not fetched.
            if type_name and guid and not data:
                entity_type = entity_types_dict.get(type_name)
                if entity_type:
                    relationship_entities = entity_type['entities']
                    fetched_entity = relationship_entities.get(guid)
                    if not fetched_entity:
                        entities_dict = \
                            self.\
                            enrich_entities_attributes_and_classifications(
                                [guid])
                        if entities_dict:
                            fetched_entity_dict = entities_dict.get(guid)
                            if fetched_entity_dict:
                                relationship_entities[
                                    guid] = fetched_entity_dict
