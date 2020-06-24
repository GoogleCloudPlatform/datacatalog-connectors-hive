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
import re

from google.cloud import datacatalog

__DATACATALOG_LOCATION_ID = 'us-central1'
__DATACATALOG_ENTRY_GROUP_ID = 'apache_atlas'

__datacatalog = datacatalog.DataCatalogClient()


def __delete_entries_and_groups(project_id):
    entry_name_pattern = '(?P<entry_group_name>.+?)/entries/(.+?)'

    query = 'system={}'.format(__DATACATALOG_ENTRY_GROUP_ID)

    scope = datacatalog.types.SearchCatalogRequest.Scope()
    scope.include_project_ids.extend([project_id])

    # TODO Replace "search entries" by "list entries by group"
    #  when/if it becomes available.
    search_results = [result for result in __datacatalog.search_catalog(scope=scope,
                                                                        query=query,
                                                                        page_size=1000,
                                                                        timeout=300000)]

    entry_group_names = []
    for result in search_results:
        try:
            __datacatalog.delete_entry(result.relative_resource_name)
            print('Entry deleted: {}'.format(result.relative_resource_name))
            entry_group_name = re.match(
                pattern=entry_name_pattern,
                string=result.relative_resource_name
            ).group('entry_group_name')
            entry_group_names.append(entry_group_name)
        except Exception as e:
            print('Exception deleting Entry')
            print(e)

    # Delete any pre-existing Entry Groups.
    for entry_group_name in set(entry_group_names):
        try:
            __datacatalog.delete_entry_group(entry_group_name, force=True)
            print('--> Entry Group deleted: {}'.format(entry_group_name))
        except Exception as e:
            print('Exception deleting Entry Group')
            print(str(e))


def __delete_tag_templates(project_id):
    query = 'type=TAG_TEMPLATE name:{}'.format(__DATACATALOG_ENTRY_GROUP_ID)

    scope = datacatalog.types.SearchCatalogRequest.Scope()
    scope.include_project_ids.extend([project_id])

    search_results = [result for result in __datacatalog.search_catalog(scope=scope,
                                                                        query=query,
                                                                        page_size=1000,
                                                                        timeout=300000)]

    for result in search_results:
        try:
            __datacatalog.delete_tag_template(result.relative_resource_name, force=True)
            print('--> Tag Template deleted: {}'.format(result.relative_resource_name))
        except Exception as e:
            print('Exception deleting Tag Template')
            print(str(e))


def __parse_args():
    parser = argparse.ArgumentParser(
        description='Command line utility to remove all Apache-atlas related'
                    ' metadata from Data Catalog')

    parser.add_argument(
        '--datacatalog-project-ids',
        help='List of Google Cloud project IDs split by comma.'
             ' At least one must be provided.', required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = __parse_args()

    # Split multiple values separated by comma.
    datacatalog_project_ids = \
        [item for item in args.datacatalog_project_ids.split(',')]

    for project_id in datacatalog_project_ids:
        __delete_entries_and_groups(project_id)
        __delete_tag_templates(project_id)
