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

import argparse

import logging
import os
import sys

from .sync import datacatalog_synchronizer


class Hive2DatacatalogCli:

    @staticmethod
    def run(argv):
        args = Hive2DatacatalogCli.__parse_args(argv)
        # Enable logging
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        if args.service_account_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS']\
                = args.service_account_path

        datacatalog_synchronizer.DataCatalogSynchronizer(
            project_id=args.datacatalog_project_id,
            location_id=args.datacatalog_location_id,
            hive_metastore_db_host=args.hive_metastore_db_host,
            hive_metastore_db_user=args.hive_metastore_db_user,
            hive_metastore_db_pass=args.hive_metastore_db_pass,
            hive_metastore_db_name=args.hive_metastore_db_name,
            hive_metastore_db_type=args.hive_metastore_db_type,
            enable_monitoring=args.enable_monitoring).run()

    @staticmethod
    def __parse_args(argv):
        parser = argparse.ArgumentParser(
            description='Command line to sync Hive metadata to Datacatalog')

        parser.add_argument('--datacatalog-project-id',
                            help='Your Google Cloud project ID',
                            required=True)
        parser.add_argument(
            '--datacatalog-location-id',
            help='Location ID to be used for your Google Cloud Datacatalog',
            required=True)
        parser.add_argument('--hive-metastore-db-host',
                            help='Your Hive metastore database host',
                            required=True)
        parser.add_argument('--hive-metastore-db-user',
                            help='Your Hive database credentials user',
                            required=True)
        parser.add_argument('--hive-metastore-db-pass',
                            help='Your Hive database credentials password',
                            required=True)
        parser.add_argument('--hive-metastore-db-name',
                            help='Your Hive database name password',
                            required=True)
        parser.add_argument(
            '--hive-metastore-db-type',
            help='Your Hive database type (Currently supports postgresql)',
            required=True)
        parser.add_argument(
            '--service-account-path',
            help='Local Service Account path '
            '(Can be suplied as GOOGLE_APPLICATION_CREDENTIALS env var)')
        parser.add_argument('--enable-monitoring',
                            help='Enables monitoring metrics on the connector')
        return parser.parse_args(argv)


def main():
    argv = sys.argv
    Hive2DatacatalogCli().run(argv[1:] if len(argv) > 0 else argv)
