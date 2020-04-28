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
from unittest.mock import patch

from google.datacatalog_connectors.hive import datacatalog_cli


@patch('google.datacatalog_connectors.hive.'
       'sync.datacatalog_synchronizer.DataCatalogSynchronizer.__init__',
       lambda self, **kargs: None)
class DatacatalogCLITestCase(unittest.TestCase):

    @patch('argparse.ArgumentParser.parse_args')
    @patch('argparse.ArgumentParser.add_argument')
    @patch('google.datacatalog_connectors.hive.'
           'sync.datacatalog_synchronizer.DataCatalogSynchronizer.run')
    def test_datacatalog_cli_run_should_not_raise_error(
            self, run, add_argument, parse_args):  # noqa
        mocked_parse_args = DictWithAttributeAccess()
        mocked_parse_args.service_account_path = 'service_account.json'

        mocked_parse_args.datacatalog_project_id = 'test_project_id'
        mocked_parse_args.datacatalog_location_id = 'location_id'
        mocked_parse_args.hive_metastore_db_host = 'host'
        mocked_parse_args.hive_metastore_db_user = 'user'
        mocked_parse_args.hive_metastore_db_pass = 'pass'
        mocked_parse_args.hive_metastore_db_name = 'name'
        mocked_parse_args.hive_metastore_db_type = 'type'
        mocked_parse_args.enable_monitoring = True

        parse_args.return_value = mocked_parse_args

        datacatalog_cli.Hive2DatacatalogCli.run({})

        for call_arg in add_argument.call_args_list:
            arg = call_arg[0]
            command = arg[0]
            # Verify args which should contain the required attribute
            if '-h' not in command\
                    and '--service-account-path' not in command\
                    and '--enable-monitoring' not in command:
                params = call_arg[1]
                required = params['required']
                self.assertTrue(required)

        self.assertEqual(run.call_count, 1)


class DictWithAttributeAccess(dict):

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value
