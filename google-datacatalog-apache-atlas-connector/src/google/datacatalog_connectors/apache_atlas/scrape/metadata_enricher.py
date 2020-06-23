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


class MetadataEnricher:

    def __init__(self, apache_atlas_facade):
        self.__apache_atlas_facade = apache_atlas_facade

    def enrich_entity_classifications(self, fetched_entities_dict,
                                      searched_entries):
        for key, fetched_entity in fetched_entities_dict.items():
            classification_names = searched_entries.get(key, {}).get(
                'data', {}).get('classificationNames', [])

            # We only do this fetch call if required.
            if len(classification_names) > 0:
                fetched_entity['classifications'] = \
                    self.__apache_atlas_facade.\
                    fetch_entity_classifications(key)

    @classmethod
    def enrich_entity_relationships(cls, entity_types_dict):
        for _, entity_type_dict in entity_types_dict.items():
            entities = entity_type_dict['entities']

            if entities:
                for _, entity_dict in entities.items():
                    data = entity_dict['data']
                    attributes = data['attributes']

                    if attributes:
                        for key, attribute in attributes.items():
                            if isinstance(attribute, list):
                                for item in attribute:
                                    cls.__enrich_relationship_dict(
                                        item, entity_types_dict)
                            else:
                                cls.__enrich_relationship_dict(
                                    attribute, entity_types_dict)

    @classmethod
    def __enrich_relationship_dict(cls, attribute_dict, entity_types_dict):
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
                    if relationship_entities:
                        fetched_entity = relationship_entities.get(guid)
                        if fetched_entity:
                            fetched_entity_data = fetched_entity['data']

                            fetched_classification_names = fetched_entity.get(
                                'classifications')
                            attribute_dict['data'] = fetched_entity_data
                            attribute_dict[
                                'classifications'] =\
                                fetched_classification_names
