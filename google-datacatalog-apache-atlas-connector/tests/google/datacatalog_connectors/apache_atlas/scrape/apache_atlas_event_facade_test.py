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

import unittest
from unittest.mock import patch

from google.datacatalog_connectors.apache_atlas import scrape


class ApacheAtlasEventFacadeTestCase(unittest.TestCase):

    def setUp(self):
        self.__atlas_event_facade = scrape.ApacheAtlasEventFacade({
            'event_servers': 'my_host',
            'event_consumer_group_id': 'my_consumer_group'
        })

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__atlas_event_facade.__dict__
        self.assertIsNotNone(attrs['_ApacheAtlasEventFacade__connection_args'])

    @patch('google.datacatalog_connectors.apache_atlas.scrape.'
           'apache_atlas_event_facade.KafkaConsumer')
    def test_create_event_consumer_should_succeed(self, kafka_consumer):
        self.__atlas_event_facade = scrape.ApacheAtlasEventFacade({
            'event_servers': 'my_host',
            'event_consumer_group_id': 'my_consumer_group'
        })

        returned_consumer = self.__atlas_event_facade.create_event_consumer()
        self.assertEqual(kafka_consumer.return_value, returned_consumer)

        kafka_consumer.assert_called_once_with('ATLAS_ENTITIES',
                                               api_version=(0, 10),
                                               auto_offset_reset='earliest',
                                               bootstrap_servers='my_host',
                                               consumer_timeout_ms=10000,
                                               enable_auto_commit=False,
                                               group_id='my_consumer_group',
                                               session_timeout_ms=300000)
