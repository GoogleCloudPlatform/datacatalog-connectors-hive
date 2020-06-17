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

from google.datacatalog_connectors.hive import entities
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy.orm import defaultload, sessionmaker


class MetadataDatabaseScraper:

    def __init__(self, hive_metastore_db_host, hive_metastore_db_user,
                 hive_metastore_db_pass, hive_metastore_db_name,
                 hive_metastore_db_type):
        self.__engine = create_engine('{}://{}:{}@{}/{}'.format(
            hive_metastore_db_type, hive_metastore_db_user,
            hive_metastore_db_pass, hive_metastore_db_host,
            hive_metastore_db_name))

    def get_database_metadata(self):
        session_wrapper = sessionmaker(bind=self.__engine)
        session = session_wrapper()
        try:
            databases = session.query(entities.Database).options(
                defaultload(entities.Database.tables).joinedload(
                    entities.Table.table_params),
                defaultload(entities.Database.tables).joinedload(
                    entities.Table.table_storages).joinedload(
                        entities.TableStorage.columns)).all()

            return {'databases': databases}
        except exc.OperationalError:
            logging.error('Unable to connect to the metadata database.')
            raise
