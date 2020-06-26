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
from unittest import mock

from google.datacatalog_connectors import apache_atlas
from google.datacatalog_connectors.apache_atlas import \
    apache_atlas2datacatalog_cli


class ApacheAtlas2DataCatalogCliTest(unittest.TestCase):

    def test_parse_args_missing_project_id_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, apache_atlas2datacatalog_cli.
            ApacheAtlas2DataCatalogCli._parse_args, [
                'sync', '--atlas-host', 'my-host', '--atlas-port', 'my-port',
                '--atlas-user', 'my-user', '--atlas-passsword', 'my-pass'
            ])

    def test_parse_args_missing_atlas_credentials_should_raise_system_exit(
            self):
        self.assertRaises(
            SystemExit, apache_atlas2datacatalog_cli.
            ApacheAtlas2DataCatalogCli._parse_args, [
                'sync', '--datacatalog-project-id', 'dc-project_id',
                '--atlas-host', 'my-host', '--atlas-port', 'my-port'
            ])

    @mock.patch('google.datacatalog_connectors.apache_atlas.sync'
                '.MetadataSynchronizer')
    def test_run_should_call_synchronizer(self, mock_metadata_synchonizer):
        apache_atlas2datacatalog_cli.ApacheAtlas2DataCatalogCli.run([
            'sync', '--datacatalog-project-id', 'dc-project_id',
            '--atlas-host', 'my-host', '--atlas-port', 'my-port',
            '--atlas-user', 'my-user', '--atlas-passsword', 'my-pass'
        ])

        mock_metadata_synchonizer.assert_called_once_with(
            atlas_entity_types=None,
            atlas_connection_args={
                'host': 'my-host',
                'port': 'my-port',
                'user': 'my-user',
                'pass': 'my-pass'
            },
            datacatalog_location_id='us-central1',
            datacatalog_project_id='dc-project_id',
            enable_monitoring=None)

        synchonizer = mock_metadata_synchonizer.return_value
        synchonizer.run.assert_called_once()

    @mock.patch('google.datacatalog_connectors.apache_atlas.sync'
                '.MetadataSynchronizer')
    def test_run_with_entity_types_should_call_synchronizer(
            self, mock_metadata_synchonizer):
        apache_atlas2datacatalog_cli.ApacheAtlas2DataCatalogCli.run([
            'sync', '--datacatalog-project-id', 'dc-project_id',
            '--atlas-host', 'my-host', '--atlas-port', 'my-port',
            '--atlas-user', 'my-user', '--atlas-passsword', 'my-pass',
            '--atlas-entity-types', 'Tables,Columns'
        ])

        mock_metadata_synchonizer.assert_called_once_with(
            atlas_entity_types=['Tables', 'Columns'],
            atlas_connection_args={
                'host': 'my-host',
                'port': 'my-port',
                'user': 'my-user',
                'pass': 'my-pass'
            },
            datacatalog_location_id='us-central1',
            datacatalog_project_id='dc-project_id',
            enable_monitoring=None)

        synchonizer = mock_metadata_synchonizer.return_value
        synchonizer.run.assert_called_once()

    @mock.patch('google.datacatalog_connectors.apache_atlas.sync'
                '.MetadataEventSynchronizer')
    def test_run_should_call_event_synchronizer(self,
                                                mock_metadata_synchonizer):
        apache_atlas2datacatalog_cli.ApacheAtlas2DataCatalogCli.run([
            'sync-event-hook',
            '--datacatalog-project-id',
            'dc-project_id',
            '--atlas-host',
            'my-host',
            '--atlas-port',
            'my-port',
            '--atlas-user',
            'my-user',
            '--atlas-passsword',
            'my-pass',
            '--event-servers',
            'my-host:port',
            '--event-consumer-group-id',
            'my_consumer_group',
        ])

        mock_metadata_synchonizer.assert_called_once_with(
            atlas_entity_types=None,
            atlas_connection_args={
                'host': 'my-host',
                'port': 'my-port',
                'user': 'my-user',
                'pass': 'my-pass',
                'event_servers': ['my-host:port'],
                'event_consumer_group_id': 'my_consumer_group',
                'event_hook': True
            },
            datacatalog_location_id='us-central1',
            datacatalog_project_id='dc-project_id',
            enable_monitoring=None)

        synchonizer = mock_metadata_synchonizer.return_value
        synchonizer.run.assert_called_once()

    @mock.patch('google.datacatalog_connectors.apache_atlas.'
                'apache_atlas2datacatalog_cli'
                '.ApacheAtlas2DataCatalogCli')
    def test_main_should_call_cli_run(self, mock_cli):
        apache_atlas.main()
        mock_cli.run.assert_called_once()
