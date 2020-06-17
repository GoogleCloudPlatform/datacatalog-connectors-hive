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
import os
import unittest
from unittest.mock import patch

from google.datacatalog_connectors.hive.sync import datacatalog_synchronizer


@patch('google.datacatalog_connectors.hive.scrape.'
       'MetadataDatabaseScraper.__init__', lambda self, *args: None)
@patch('google.datacatalog_connectors.hive.prepare.'
       'assembled_entry_factory.AssembledEntryFactory.__init__',
       lambda self, *args: None)
@patch('google.datacatalog_connectors.commons.ingest.'
       'datacatalog_metadata_ingestor.DataCatalogMetadataIngestor.__init__',
       lambda self, *args: None)
@patch('google.datacatalog_connectors.commons.cleanup.'
       'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.__init__',
       lambda self, *args: None)
@patch('google.datacatalog_connectors.commons.monitoring.'
       'metrics_processor.MetricsProcessor.__init__', lambda self, *args: None)
class DatacatalogSynchronizerTestCase(unittest.TestCase):
    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __HIVE_METASTORE_DB_HOST = 'localhost'
    __HIVE_METASTORE_DB_USER = 'test_user'
    __HIVE_METASTORE_DB_PASS = 'test_pass'
    __HIVE_METASTORE_DB_NAME = 'metastore'
    __HIVE_METASTORE_DB_TYPE = 'postgresql'

    @patch('google.datacatalog_connectors.hive.scrape.'
           'MetadataDatabaseScraper.get_database_metadata')
    @patch('google.datacatalog_connectors.hive.'
           'prepare.assembled_entry_factory.'
           'AssembledEntryFactory.make_entries_from_database_metadata')
    @patch('google.datacatalog_connectors.commons.ingest.'
           'datacatalog_metadata_ingestor.'
           'DataCatalogMetadataIngestor.ingest_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_obsolete_metadata')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_elapsed_time_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_metadata_payload_bytes_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_entries_length_metric')
    def test_synchronize_metadata_should_not_raise_error(
            self, process_entries_length_metric,
            process_metadata_payload_bytes_metric, process_elapsed_time_metric,
            delete_obsolete_metadata, ingest_metadata,
            make_entries_from_database_metadata,
            get_database_metadata):  # noqa

        make_entries_from_database_metadata.return_value = [({}, [])]

        synchronizer = datacatalog_synchronizer.DataCatalogSynchronizer(
            DatacatalogSynchronizerTestCase.__PROJECT_ID,
            DatacatalogSynchronizerTestCase.__LOCATION_ID,
            DatacatalogSynchronizerTestCase.__HIVE_METASTORE_DB_HOST,
            DatacatalogSynchronizerTestCase.__HIVE_METASTORE_DB_USER,
            DatacatalogSynchronizerTestCase.__HIVE_METASTORE_DB_PASS,
            DatacatalogSynchronizerTestCase.__HIVE_METASTORE_DB_NAME,
            DatacatalogSynchronizerTestCase.__HIVE_METASTORE_DB_TYPE)
        synchronizer.run()
        self.assertEqual(1, get_database_metadata.call_count)
        self.assertEqual(1, make_entries_from_database_metadata.call_count)
        self.assertEqual(1, ingest_metadata.call_count)
        self.assertEqual(1, delete_obsolete_metadata.call_count)
        self.assertEqual(process_entries_length_metric.call_count, 1)
        self.assertEqual(process_metadata_payload_bytes_metric.call_count, 1)
        self.assertEqual(process_elapsed_time_metric.call_count, 1)

    @patch('google.datacatalog_connectors.hive.scrape.'
           'MetadataSyncEventScraper.get_database_metadata')
    @patch('google.datacatalog_connectors.hive.'
           'prepare.assembled_entry_factory.'
           'AssembledEntryFactory.make_entries_from_database_metadata')
    @patch('google.datacatalog_connectors.commons.ingest.'
           'datacatalog_metadata_ingestor.'
           'DataCatalogMetadataIngestor.ingest_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_obsolete_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_metadata')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_elapsed_time_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_metadata_payload_bytes_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_entries_length_metric')
    def test_synchronize_metadata_with_create_table_sync_event_should_succeed(
            self, process_entries_length_metric,
            process_metadata_payload_bytes_metric, process_elapsed_time_metric,
            delete_metadata, delete_obsolete_metadata, ingest_metadata,
            make_entries_from_database_metadata,
            get_database_metadata):  # noqa

        make_entries_from_database_metadata.return_value = [({}, [])]

        synchronizer = datacatalog_synchronizer.DataCatalogSynchronizer(
            project_id=DatacatalogSynchronizerTestCase.__PROJECT_ID,
            location_id=DatacatalogSynchronizerTestCase.__LOCATION_ID,
            metadata_sync_event=retrieve_json_file(
                '/hooks/message_create_table.json'))
        synchronizer.run()
        self.assertEqual(1, get_database_metadata.call_count)
        self.assertEqual(1, make_entries_from_database_metadata.call_count)
        self.assertEqual(1, ingest_metadata.call_count)
        self.assertEqual(0, delete_metadata.call_count)
        self.assertEqual(0, delete_obsolete_metadata.call_count)
        self.assertEqual(process_entries_length_metric.call_count, 1)
        self.assertEqual(process_metadata_payload_bytes_metric.call_count, 1)
        self.assertEqual(process_elapsed_time_metric.call_count, 1)

    @patch('google.datacatalog_connectors.hive.scrape.'
           'MetadataSyncEventScraper.get_database_metadata')
    @patch('google.datacatalog_connectors.hive.'
           'prepare.assembled_entry_factory.'
           'AssembledEntryFactory.make_entries_from_database_metadata')
    @patch('google.datacatalog_connectors.commons.ingest.'
           'datacatalog_metadata_ingestor.'
           'DataCatalogMetadataIngestor.ingest_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_obsolete_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_metadata')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_elapsed_time_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_metadata_payload_bytes_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_entries_length_metric')
    def test_synchronize_metadata_with_update_table_sync_event_should_succeed(
            self, process_entries_length_metric,
            process_metadata_payload_bytes_metric, process_elapsed_time_metric,
            delete_metadata, delete_obsolete_metadata, ingest_metadata,
            make_entries_from_database_metadata,
            get_database_metadata):  # noqa

        make_entries_from_database_metadata.return_value = [({}, [])]

        synchronizer = datacatalog_synchronizer.DataCatalogSynchronizer(
            project_id=DatacatalogSynchronizerTestCase.__PROJECT_ID,
            location_id=DatacatalogSynchronizerTestCase.__LOCATION_ID,
            metadata_sync_event=retrieve_json_file(
                '/hooks/message_update_table.json'))
        synchronizer.run()
        self.assertEqual(1, get_database_metadata.call_count)
        self.assertEqual(1, make_entries_from_database_metadata.call_count)
        self.assertEqual(1, ingest_metadata.call_count)
        self.assertEqual(0, delete_metadata.call_count)
        self.assertEqual(0, delete_obsolete_metadata.call_count)
        self.assertEqual(process_entries_length_metric.call_count, 1)
        self.assertEqual(process_metadata_payload_bytes_metric.call_count, 1)
        self.assertEqual(process_elapsed_time_metric.call_count, 1)

    @patch('google.datacatalog_connectors.hive.scrape.'
           'MetadataSyncEventScraper.get_database_metadata')
    @patch('google.datacatalog_connectors.hive.'
           'prepare.assembled_entry_factory.'
           'AssembledEntryFactory.make_entries_from_database_metadata')
    @patch('google.datacatalog_connectors.commons.ingest.'
           'datacatalog_metadata_ingestor.'
           'DataCatalogMetadataIngestor.ingest_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_obsolete_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_metadata')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_elapsed_time_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_metadata_payload_bytes_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_entries_length_metric')
    def test_synchronize_metadata_with_drop_database_sync_event_should_succeed(  # noqa
            self, process_entries_length_metric,
            process_metadata_payload_bytes_metric, process_elapsed_time_metric,
            delete_metadata, delete_obsolete_metadata, ingest_metadata,
            make_entries_from_database_metadata,
            get_database_metadata):  # noqa

        make_entries_from_database_metadata.return_value = [({}, [])]

        synchronizer = datacatalog_synchronizer.DataCatalogSynchronizer(
            project_id=DatacatalogSynchronizerTestCase.__PROJECT_ID,
            location_id=DatacatalogSynchronizerTestCase.__LOCATION_ID,
            metadata_sync_event=retrieve_json_file(
                '/hooks/message_drop_database.json'))
        synchronizer.run()
        self.assertEqual(1, get_database_metadata.call_count)
        self.assertEqual(1, make_entries_from_database_metadata.call_count)
        self.assertEqual(0, ingest_metadata.call_count)
        self.assertEqual(1, delete_metadata.call_count)
        self.assertEqual(0, delete_obsolete_metadata.call_count)
        self.assertEqual(process_entries_length_metric.call_count, 1)
        self.assertEqual(process_metadata_payload_bytes_metric.call_count, 1)
        self.assertEqual(process_elapsed_time_metric.call_count, 1)

    @patch('google.datacatalog_connectors.hive.scrape.'
           'MetadataSyncEventScraper.get_database_metadata')
    @patch('google.datacatalog_connectors.hive.'
           'prepare.assembled_entry_factory.'
           'AssembledEntryFactory.make_entries_from_database_metadata')
    @patch('google.datacatalog_connectors.commons.ingest.'
           'datacatalog_metadata_ingestor.'
           'DataCatalogMetadataIngestor.ingest_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_obsolete_metadata')
    @patch('google.datacatalog_connectors.commons.cleanup.'
           'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
           'delete_metadata')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_elapsed_time_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_metadata_payload_bytes_metric')
    @patch('google.datacatalog_connectors.commons.monitoring.'
           'metrics_processor.MetricsProcessor.'
           'process_entries_length_metric')
    def test_synchronize_metadata_with_drop_table_sync_event_should_succeed(  # noqa
            self, process_entries_length_metric,
            process_metadata_payload_bytes_metric, process_elapsed_time_metric,
            delete_metadata, delete_obsolete_metadata, ingest_metadata,
            make_entries_from_database_metadata,
            get_database_metadata):  # noqa

        make_entries_from_database_metadata.return_value = [({}, [])]

        synchronizer = datacatalog_synchronizer.DataCatalogSynchronizer(
            project_id=DatacatalogSynchronizerTestCase.__PROJECT_ID,
            location_id=DatacatalogSynchronizerTestCase.__LOCATION_ID,
            metadata_sync_event=retrieve_json_file(
                '/hooks/message_drop_table.json'))
        synchronizer.run()
        self.assertEqual(1, get_database_metadata.call_count)
        self.assertEqual(1, make_entries_from_database_metadata.call_count)
        self.assertEqual(0, ingest_metadata.call_count)
        self.assertEqual(1, delete_metadata.call_count)
        self.assertEqual(0, delete_obsolete_metadata.call_count)
        self.assertEqual(process_entries_length_metric.call_count, 1)
        self.assertEqual(process_metadata_payload_bytes_metric.call_count, 1)
        self.assertEqual(process_elapsed_time_metric.call_count, 1)


def retrieve_json_file(name):
    resolved_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '../test_data/{}'.format(name))

    with open(resolved_name) as json_file:
        return json.load(json_file)
