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
import sys

from google.datacatalog_connectors.apache_atlas import sync


class ApacheAtlas2DataCatalogCli:
    __DATACATALOG_LOCATION_ID = 'us-central1'

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()

        args = cls._parse_args(argv)
        args.func(args)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)
        cls.__disable_verboose_kafka_loggers()

    @classmethod
    def __disable_verboose_kafka_loggers(cls):
        logging.getLogger('kafka.conn').setLevel(logging.ERROR)
        logging.getLogger('kafka.cluster').setLevel(logging.ERROR)
        logging.getLogger('kafka.coordinator').setLevel(logging.ERROR)
        logging.getLogger('kafka.consumer.fetcher').setLevel(logging.FATAL)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)

        subparsers = parser.add_subparsers()

        cls.__add_sync_command(subparsers)
        cls.__add_event_metadata_hook_command(subparsers)

        return parser.parse_args(argv)

    @classmethod
    def __add_event_metadata_hook_command(cls, subparsers):
        event_metadata_hook_parser = subparsers.add_parser(
            "sync-event-hook", help="Process Event metadata command")
        event_metadata_hook_parser.add_argument(
            '--event-servers',
            help='Event bus server list, split by comma',
            required=True)
        event_metadata_hook_parser.add_argument(
            '--event-consumer-group-id',
            help='Consumer Group id used to connect to '
            'EVENT topic',
            required=True)
        cls.__add_common_args(event_metadata_hook_parser)
        event_metadata_hook_parser.set_defaults(
            func=cls.__run_event_metadata_hook)

    @classmethod
    def __add_sync_command(cls, subparsers):
        sync_sub_parser = subparsers.add_parser("sync", help="Sync commands")

        cls.__add_common_args(sync_sub_parser)
        sync_sub_parser.set_defaults(func=cls.__run_synchronizer)

    @classmethod
    def __add_common_args(cls, sync_sub_parser):
        sync_sub_parser.add_argument('--datacatalog-project-id',
                                     help='Google Cloud Project ID',
                                     required=True)
        sync_sub_parser.add_argument('--apache-host',
                                     help='Apache Atlas Host',
                                     required=True)
        sync_sub_parser.add_argument('--apache-port',
                                     help='Apache Atlas Port',
                                     required=True)
        sync_sub_parser.add_argument('--apache-user',
                                     help='Apache Atlas User',
                                     required=True)
        sync_sub_parser.add_argument('--apache-passsword',
                                     help='Apache Atlas Pass',
                                     required=True)
        sync_sub_parser.add_argument('--apache-entity-types',
                                     help='Apache Atlas Entity Types')
        sync_sub_parser.add_argument(
            '--enable-monitoring',
            help='Enables monitoring metrics on the connector')

    @classmethod
    def __run_synchronizer(cls, args):
        apache_entity_types = cls.__get_apache_entity_types(args)

        sync.MetadataSynchronizer(
            datacatalog_project_id=args.datacatalog_project_id,
            datacatalog_location_id=cls.__DATACATALOG_LOCATION_ID,
            atlas_connection_args={
                'host': args.apache_host,
                'port': args.apache_port,
                'user': args.apache_user,
                'pass': args.apache_passsword
            },
            apache_entity_types=apache_entity_types,
            enable_monitoring=args.enable_monitoring).run()

    @classmethod
    def __run_event_metadata_hook(cls, args):
        apache_entity_types = cls.__get_apache_entity_types(args)

        sync.MetadataEventSynchronizer(
            datacatalog_project_id=args.datacatalog_project_id,
            datacatalog_location_id=cls.__DATACATALOG_LOCATION_ID,
            atlas_connection_args={
                'host': args.apache_host,
                'port': args.apache_port,
                'user': args.apache_user,
                'pass': args.apache_passsword,
                'event_servers': args.event_servers.split(','),
                'event_consumer_group_id': args.event_consumer_group_id,
                'event_hook': True
            },
            apache_entity_types=apache_entity_types,
            enable_monitoring=args.enable_monitoring).run()

    @classmethod
    def __get_apache_entity_types(cls, args):
        # Split multiple values separated by comma.
        apache_entity_types = None
        if args.apache_entity_types:
            apache_entity_types = args.apache_entity_types.split(',')
        return apache_entity_types


def main():
    argv = sys.argv
    ApacheAtlas2DataCatalogCli.run(argv[1:] if len(argv) > 0 else argv)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
