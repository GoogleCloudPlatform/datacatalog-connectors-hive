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

from google.datacatalog_connectors.hive.entities import base, table
import sqlalchemy


class TableParams(base.BASE):
    __tablename__ = 'TABLE_PARAMS'

    id = sqlalchemy.Column('TBL_ID',
                           sqlalchemy.Integer,
                           sqlalchemy.ForeignKey(table.Table.id),
                           primary_key=True)
    param_key = sqlalchemy.Column('PARAM_KEY',
                                  sqlalchemy.String,
                                  primary_key=True)
    param_value = sqlalchemy.Column('PARAM_VALUE', sqlalchemy.String)

    def dump(self):
        return {
            'id': self.id,
            'param_key': self.param_key,
            'param_value': self.param_value
        }
