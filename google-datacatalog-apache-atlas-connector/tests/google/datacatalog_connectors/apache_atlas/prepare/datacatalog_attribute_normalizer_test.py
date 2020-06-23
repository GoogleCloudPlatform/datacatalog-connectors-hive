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

import unittest

from google.datacatalog_connectors.apache_atlas.prepare import \
    datacatalog_attribute_normalizer as attr_normalizer


class DataCatalogAttributeNormalizerTest(unittest.TestCase):

    def test_create_tag_template_id_should_return(self):
        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id('my_template', 'table_type')

        self.assertEqual('apache_atlas_table_type_my_template',
                         tag_template_id)

    def test_create_tag_template_id_with_version_should_return(self):
        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            create_tag_template_id('my_template', 'table_type', 1)

        self.assertEqual('apache_atlas_table_type_my_template_1',
                         tag_template_id)

    def test_format_name_should_return(self):
        tag_template_id = attr_normalizer.DataCatalogAttributeNormalizer.\
            format_name('my template name - test')

        self.assertEqual('my_template_name___test', tag_template_id)

    def test_get_column_data_type_should_return(self):
        data_type = attr_normalizer.DataCatalogAttributeNormalizer.\
            get_column_data_type({'dataType': 'Column'})

        self.assertEqual('Column', data_type)

    def test_get_column_data_type_second_option_should_return(self):
        data_type = attr_normalizer.DataCatalogAttributeNormalizer.\
            get_column_data_type({'data_type': 'Column2'})

        self.assertEqual('Column2', data_type)

    def test_get_column_data_type_third_option_should_return(self):
        data_type = attr_normalizer.DataCatalogAttributeNormalizer.\
            get_column_data_type({'type': 'Column3'})

        self.assertEqual('Column3', data_type)
