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

import logging
import uuid

from google.datacatalog_connectors.commons import cleanup, ingest
from google.datacatalog_connectors.commons.monitoring \
    import metrics_processor

from google.datacatalog_connectors.apache_atlas import prepare, scrape


class MetadataSynchronizer:
    _ENTRY_GROUP_ID = 'apache_atlas'
    _SPECIFIED_SYSTEM = 'apache_atlas'

    def __init__(self,
                 datacatalog_project_id,
                 datacatalog_location_id,
                 atlas_connection_args,
                 apache_entity_types=None,
                 enable_monitoring=None):
        self._project_id = datacatalog_project_id
        self._location_id = datacatalog_location_id
        self._atlas_connection_args = atlas_connection_args
        self._apache_entity_types = apache_entity_types

        event_hook = atlas_connection_args.get('event_hook')

        if not event_hook:
            self._metadata_scraper = scrape.MetadataScraper(
                atlas_connection_args)

        self._tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id)

        self._instance_url = self._extract_instance_url(atlas_connection_args)

        self._assembled_entry_factory = prepare.AssembledEntryFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id,
            entry_group_id=self._ENTRY_GROUP_ID,
            user_specified_system=self._SPECIFIED_SYSTEM,
            instance_url=self._instance_url)

        self._task_id = uuid.uuid4().hex[:8]
        self._metrics_processor = metrics_processor.MetricsProcessor(
            datacatalog_project_id, datacatalog_location_id,
            self._ENTRY_GROUP_ID, enable_monitoring, self._task_id)

    @classmethod
    def _extract_instance_url(cls, atlas_connection_args):
        return atlas_connection_args['host']

    def run(self):
        """Coordinates a full scrape > prepare > ingest process."""
        logging.info('')
        logging.info('===> Scraping Apache Atlas metadata...')
        metadata_dict, _ = self._metadata_scraper.get_metadata()
        self._log_metadata(metadata_dict)
        tag_templates_dict = self._make_tag_templates_dict(metadata_dict)
        assembled_entries = self._make_assembled_entries(
            metadata_dict, self._apache_entity_types)
        logging.info('==== DONE ========================================')
        logging.info('')
        self._clean_up_obsolete_metadata(assembled_entries)
        self._sync_assembled_entries(assembled_entries, tag_templates_dict)
        self._after_run()

    def _sync_assembled_entries(self, assembled_entries, tag_templates_dict):
        self._log_entries(assembled_entries)
        if assembled_entries:
            logging.info('')
            logging.info('===> Mapping Data Catalog entries relationships...')
            self._map_datacatalog_relationships(assembled_entries)
            logging.info('==== DONE ========================================')
            logging.info(
                '===> Synchronizing Apache Atlas :: Data Catalog metadata...')
            self._ingest_metadata(tag_templates_dict, assembled_entries)
            logging.info('==== DONE ========================================')

    def _clean_up_obsolete_metadata(self, assembled_entries):
        logging.info('===> Deleting Data Catalog obsolete metadata...')
        self._delete_obsolete_entries(assembled_entries)
        logging.info('==== DONE ========================================')
        logging.info('')

    def _log_metadata(self, metadata):
        self._metrics_processor.process_metadata_payload_bytes_metric(metadata)

    def _log_entries(self, prepared_entries):
        self._metrics_processor.process_entries_length_metric(
            len(prepared_entries))

    def _after_run(self):
        self._metrics_processor.process_elapsed_time_metric()

    def _make_tag_templates_dict(self, metadata_dict):
        return self._tag_template_factory.\
            make_tag_templates_from_apache_atlas_metadata(
                metadata_dict)

    def _make_assembled_entries(self, metadata_dict, apache_entity_types=None):
        assembled_entries = self._assembled_entry_factory.\
            make_assembled_entries_list(
                metadata_dict, apache_entity_types)

        return assembled_entries

    def _delete_obsolete_entries(self, assembled_entries):
        cleanup.DataCatalogMetadataCleaner(
            self._project_id, self._location_id, self._ENTRY_GROUP_ID). \
            delete_obsolete_metadata(
            assembled_entries,
            'system={} tag:instance_url:{}'.format(self._SPECIFIED_SYSTEM,
                                                   self._instance_url))

    def _ingest_metadata(self, tag_templates_dict, assembled_entries):
        metadata_ingestor = ingest.DataCatalogMetadataIngestor(
            self._project_id, self._location_id, self._ENTRY_GROUP_ID)

        managed_tag_template = self._tag_template_factory.\
            get_tag_template_path(
                self._ENTRY_GROUP_ID)

        metadata_ingestor.ingest_metadata(
            assembled_entries, tag_templates_dict,
            {'delete_tags': {
                'managed_tag_template': managed_tag_template
            }})

        entries_count = len(assembled_entries)
        logging.info('==== %s entries synchronized!', entries_count)

    @classmethod
    def _map_datacatalog_relationships(cls, assembled_entries):
        prepare.EntryRelationshipMapper().fulfill_tag_fields(assembled_entries)
