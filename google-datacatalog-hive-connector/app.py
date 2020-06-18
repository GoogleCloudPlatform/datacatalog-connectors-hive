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

import base64
import json
import os

from google.datacatalog_connectors.hive.sync import datacatalog_synchronizer
from flask import jsonify, make_response, request, Flask

import google.cloud.logging

# Instantiates a client
client = google.cloud.logging.Client()

# Connects the logger to the root logging handler;
# by default this captures
# all logs at INFO level and higher
client.setup_logging()

app = Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def run():

    if request.method == 'POST':
        request_data = request.get_json()
        app.logger.info(request_data)

        message = request_data['message']
        data = message['data']
        sync_event = json.loads(base64.b64decode(data).decode('utf-8'))

        datacatalog_synchronizer.DataCatalogSynchronizer(
            project_id=os.environ['HIVE2DC_DATACATALOG_PROJECT_ID'],
            location_id=os.environ['HIVE2DC_DATACATALOG_LOCATION_ID'],
            hive_metastore_db_host=os.
            environ['HIVE2DC_HIVE_METASTORE_DB_HOST'],
            metadata_sync_event=sync_event).run()

        response = {'message': 'Synchronized', 'code': 'SUCCESS'}
        return make_response(jsonify(response), 200)
    elif request.method == 'GET':
        return 'use POST method with a message event BODY'


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
