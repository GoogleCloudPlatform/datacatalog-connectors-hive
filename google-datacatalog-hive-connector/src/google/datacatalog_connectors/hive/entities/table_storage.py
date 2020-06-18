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
from sqlalchemy.orm import relationship


class TableStorage(base.BASE):
    __tablename__ = 'SDS'

    sd_id = sqlalchemy.Column('SD_ID',
                              sqlalchemy.Integer,
                              sqlalchemy.ForeignKey(table.Table.sd_id),
                              primary_key=True)
    location = sqlalchemy.Column('LOCATION', sqlalchemy.String)
    cd_id = sqlalchemy.Column('CD_ID', sqlalchemy.Integer)
    columns = relationship('Column')

    def dump(self):
        return {
            'sd_id': self.sd_id,
            'location': self.location,
            'cd_id': self.cd_id,
            'columns': [column.dump() for column in self.columns]
        }
