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

from google.datacatalog_connectors.hive.entities import base
import sqlalchemy
from sqlalchemy.orm import relationship


class Database(base.BASE):
    __tablename__ = 'DBS'

    id = sqlalchemy.Column('DB_ID', sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column('NAME', sqlalchemy.String)
    desc = sqlalchemy.Column('DESC', sqlalchemy.String)
    uri = sqlalchemy.Column('DB_LOCATION_URI', sqlalchemy.String)
    tables = relationship('Table')

    def dump(self):
        return {
            'id': self.id,
            'name': self.name,
            'desc': self.desc,
            'uri': self.uri,
            'tables': [table.dump() for table in self.tables]
        }
