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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import \
    prepare as commons_prepare

from google.datacatalog_connectors.apache_atlas import prepare


class EntryRelationshipMapperTest(unittest.TestCase):

    def test_fulfill_tag_fields_should_resolve_table_mappings(self):
        db_id = 'test_db'
        db_entry = self.__make_fake_entry(db_id, 'db')
        db_tag = self.__make_fake_tag(string_fields=(('guid', db_id),))

        sd_id = 'test_sd'
        sd_entry = self.__make_fake_entry(sd_id, 'storagedesc')
        sd_tag = self.__make_fake_tag(string_fields=(('guid', sd_id),))

        column_id = 'test_column'
        column_entry = self.__make_fake_entry(column_id, 'column')
        column_tag = self.__make_fake_tag(string_fields=(('guid', column_id),))

        table_id = 'test_table'
        table_entry = self.__make_fake_entry(table_id, 'table')
        string_fields = ('guid', table_id), ('db_guid',
                                             db_id), ('sd_guid',
                                                      sd_id), ('column_guid',
                                                               column_id)
        table_tag = self.__make_fake_tag(string_fields=string_fields)

        db_assembled_entry = commons_prepare.AssembledEntryData(
            db_id, db_entry, [db_tag])
        sd_assembled_entry = commons_prepare.AssembledEntryData(
            sd_id, sd_entry, [sd_tag])
        column_assembled_entry = commons_prepare.AssembledEntryData(
            column_id, column_entry, [column_tag])

        table_assembled_entry = commons_prepare.AssembledEntryData(
            table_id, table_entry, [table_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields([
            db_assembled_entry, sd_assembled_entry, column_assembled_entry,
            table_assembled_entry
        ])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            '{}'.format(db_entry.name),
            table_tag.fields['db_entry'].string_value)
        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            '{}'.format(sd_entry.name),
            table_tag.fields['sd_entry'].string_value)
        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            '{}'.format(column_entry.name),
            table_tag.fields['column_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_view_mappings(self):
        db_id = 'test_db'
        db_entry = self.__make_fake_entry(db_id, 'db')
        db_tag = self.__make_fake_tag(string_fields=(('guid', db_id),))

        view_id = 'test_view'
        view_entry = self.__make_fake_entry(view_id, 'view')
        string_fields = ('guid', view_id), ('db_guid', db_id)
        view_tag = self.__make_fake_tag(string_fields=string_fields)

        db_assembled_entry = commons_prepare.AssembledEntryData(
            db_id, db_entry, [db_tag])
        view_assembled_entry = commons_prepare.AssembledEntryData(
            view_id, view_entry, [view_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [db_assembled_entry, view_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            '{}'.format(db_entry.name),
            view_tag.fields['db_entry'].string_value)

    @classmethod
    def __make_fake_entry(cls, entry_id, entry_type):
        entry = datacatalog.Entry()
        entry.name = 'fake_entries/{}'.format(entry_id)
        entry.user_specified_type = entry_type
        return entry

    @classmethod
    def __make_fake_tag(cls, string_fields=None, double_fields=None):
        tag = datacatalog.Tag()

        if string_fields:
            for field in string_fields:
                string_field = datacatalog.TagField()
                string_field.string_value = field[1]
                tag.fields[field[0]] = string_field

        if double_fields:
            for field in double_fields:
                double_field = datacatalog.TagField()
                double_field.double_value = field[1]
                tag.fields[field[0]] = double_field

        return tag
