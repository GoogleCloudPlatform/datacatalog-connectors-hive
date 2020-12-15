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

from contextlib import contextmanager
import logging

from google.datacatalog_connectors.hive import entities
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import subqueryload, sessionmaker


class MetadataDatabaseScraper:
    CONNECTION_POOL_SIZE = 20
    DATABASES_PER_PAGE = 5
    INITIAL_PAGE_NUMBER = 1

    def __init__(self, hive_metastore_db_host, hive_metastore_db_user,
                 hive_metastore_db_pass, hive_metastore_db_name,
                 hive_metastore_db_type):
        # yapf: disable
        self.__engine = create_engine('{}://{}:{}@{}/{}'.format(
            hive_metastore_db_type, hive_metastore_db_user,
            hive_metastore_db_pass, hive_metastore_db_host,
            hive_metastore_db_name), pool_size=self.CONNECTION_POOL_SIZE)
        # yapf: enable

    def get_database_metadata(self):
        try:
            databases = []
            paginated_query_conf = {
                'execute': True,
                'rows_per_page': self.DATABASES_PER_PAGE,
                'page_number': self.INITIAL_PAGE_NUMBER
            }

            # Since we can have Hive databases with thousands of tables,
            # we add pagination logic to avoid holding the session for
            # too long.
            # Pagination is done at the top level: the databases.
            while paginated_query_conf['execute']:
                # Use context  manager to make sure session is removed.
                with self.session_scope() as session:
                    logging.info('[Scrape] fetching page: %s.',
                                 paginated_query_conf['page_number'])
                    rows_per_page = paginated_query_conf['rows_per_page']

                    # Use subqueryload to eagerly execute
                    # the queries in the same session.
                    query = session.query(entities.Database).options(
                        subqueryload(entities.Database.tables).subqueryload(
                            entities.Table.table_params),
                        subqueryload(entities.Database.tables).subqueryload(
                            entities.Table.table_storages).subqueryload(
                                entities.TableStorage.columns))

                    # Add pagination clause
                    query = query.limit(rows_per_page).offset(
                        (paginated_query_conf['page_number'] - 1) *
                        rows_per_page)

                    results = query.all()
                    databases.extend(results)

                    # Set next page
                    paginated_query_conf['page_number'] = paginated_query_conf[
                        'page_number'] + 1

                    # It means there are no more pages.
                    if len(results) == 0:
                        logging.info(
                            '[Scrape] finished execution at page: %s.',
                            paginated_query_conf['page_number'])
                        paginated_query_conf['execute'] = False

            return {'databases': databases}
        except exc.OperationalError:
            logging.error('Unable to connect to the metadata database.')
            raise
        finally:
            # Make sure we have closed all connections of the connection pool.
            self.__engine.dispose()

    @contextmanager
    def session_scope(self):
        session_wrapper = sessionmaker(bind=self.__engine)
        session = session_wrapper()
        try:
            yield session
        except exc.OperationalError:
            session.rollback()
            raise
        finally:
            session.close()
