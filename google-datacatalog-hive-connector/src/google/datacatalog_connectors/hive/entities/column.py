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

from google.datacatalog_connectors.hive.entities import base, table_storage
import sqlalchemy


class Column(base.BASE):
    __tablename__ = 'COLUMNS_V2'

    id = sqlalchemy.Column('CD_ID',
                           sqlalchemy.Integer,
                           sqlalchemy.ForeignKey(
                               table_storage.TableStorage.cd_id),
                           primary_key=True)
    name = sqlalchemy.Column('COLUMN_NAME',
                             sqlalchemy.String,
                             primary_key=True)
    type = sqlalchemy.Column('TYPE_NAME', sqlalchemy.String)
    comment = sqlalchemy.Column('COMMENT', sqlalchemy.String, nullable=True)

    def dump(self):
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'comment': self.comment
        }
