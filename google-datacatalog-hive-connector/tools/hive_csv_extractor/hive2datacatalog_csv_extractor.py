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

import pandas as pd
from pyhive import hive


def __get_hive_conn(host, username, port=10000, schema='default'):
    return hive.connect(host=host,
                        port=port,
                        username=username,
                        database=schema,
                        auth=None)


def __extract(csv_file_name):
    con = __get_hive_conn('localhost', 'hive')
    cur = con.cursor()

    query = """  SELECT 
                    d."DB_ID" DATABASE_ID, d."NAME" DATABASE_NAME, d."DESC" DATABASE_DESC,
                    d."DB_LOCATION_URI" DATABASE_URI,
                    t."TBL_ID" TABLE_ID, t."TBL_NAME" TABLE_NAME, t."TBL_TYPE" TABLE_TYPE,
                    t."CREATE_TIME" TABLE_CREATE_TIME, t."LAST_ACCESS_TIME" TABLE_LAST_ACCESS_TIME,
                    s."LOCATION" TABLE_URI,
                    c."COLUMN_NAME" COLUMN_NAME, c."TYPE_NAME" COLUMN_TYPE, c."COMMENT" COLUMN_DESC,
                    c."INTEGER_IDX" COLUMN_INDEX
                    FROM "DBS" d
                    LEFT JOIN "TBLS" t on t."DB_ID" = d."DB_ID"
                    LEFT JOIN "SDS" s on s."SD_ID" = t."SD_ID" 
                    LEFT JOIN "COLUMNS_V2" c on s."CD_ID" = c."CD_ID";
            """

    # Execute query
    cur.execute(query)

    # Put it all to a data frame
    sql_data = pd.DataFrame(cur.fetchall())
    sql_data.columns = [item[0] for item in cur.description]

    # Close the session
    con.close()

    # Show the data
    logging.info(sql_data.head())

    sql_data.to_csv(csv_file_name, sep=',', encoding='utf-8')


if __name__ == '__main__':
    # Enable logging
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    __extract('hive_full_dump.csv')
