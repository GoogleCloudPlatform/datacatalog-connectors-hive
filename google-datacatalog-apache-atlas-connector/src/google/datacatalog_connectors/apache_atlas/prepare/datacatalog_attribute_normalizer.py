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

from google.datacatalog_connectors.apache_atlas.prepare import constant


class DataCatalogAttributeNormalizer:

    @classmethod
    def create_tag_template_id(cls, name, atlas_type, version=None):
        tag_template_id = '{}_{}_{}'.format(constant.TEMPLATE_PREFIX,
                                            atlas_type, name)
        if version:
            tag_template_id = '{}_{}'.format(tag_template_id, version)
        return tag_template_id

    @classmethod
    def format_name(cls, name):
        formatted_name = name.lower().replace(" ", "_").replace("-", "_")
        return formatted_name

    @classmethod
    def get_column_data_type(cls, column_attributes):
        # Here we try to support multiple data_types
        # Apache Atlas Table types use dataType,
        # rdbms_table types use data_type
        # and hive_table types use type.
        data_type = column_attributes.get('dataType')
        if not data_type:
            data_type = column_attributes.get('data_type')
            if not data_type:
                data_type = column_attributes.get('type')
        return data_type
