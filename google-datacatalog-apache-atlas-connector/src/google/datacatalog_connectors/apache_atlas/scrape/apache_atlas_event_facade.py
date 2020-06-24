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

from kafka import KafkaConsumer


class ApacheAtlasEventFacade:
    __APACHE_ATLAS_SYNC_TOPIC = 'ATLAS_ENTITIES'

    def __init__(self, connection_args):
        self.__connection_args = connection_args

    def create_event_consumer(self):
        consumer = KafkaConsumer(
            self.__APACHE_ATLAS_SYNC_TOPIC,
            api_version=(0, 10),
            consumer_timeout_ms=10000,
            session_timeout_ms=300000,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            bootstrap_servers=self.__connection_args['event_servers'],
            group_id=self.__connection_args['event_consumer_group_id'])
        return consumer
