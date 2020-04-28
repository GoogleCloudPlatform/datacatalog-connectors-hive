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

from google.datacatalog_connectors.hive.entities import base, database
import sqlalchemy
from sqlalchemy.orm import relationship


class Table(base.BASE):
    __tablename__ = 'TBLS'

    id = sqlalchemy.Column('TBL_ID', sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column('TBL_NAME', sqlalchemy.String)
    type = sqlalchemy.Column('TBL_TYPE', sqlalchemy.String)
    create_time = sqlalchemy.Column('CREATE_TIME', sqlalchemy.String)
    database_id = sqlalchemy.Column(
        'DB_ID', sqlalchemy.Integer,
        sqlalchemy.ForeignKey(database.Database.id))
    sd_id = sqlalchemy.Column('SD_ID', sqlalchemy.Integer)
    table_storages = relationship('TableStorage')
    table_params = relationship('TableParams')

    def dump(self):
        return {
            'id':
                self.id,
            'name':
                self.name,
            'type':
                self.type,
            'create_time':
                self.create_time,
            'database_id':
                self.database_id,
            'sd_id':
                self.sd_id,
            'table_storages': [
                table_storage.dump() for table_storage in self.table_storages
            ],
            'table_params': [
                table_param.dump() for table_param in self.table_params
            ]
        }
