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

TEMPLATE_PREFIX = 'apache_atlas'
CLASSIFICATION_PREFIX = 'classification'
COLUMN_PREFIX = 'column'

INSTANCE_URL_FIELD = 'instance_url'

ENTITY_TYPE_TABLE = 'Table'
ENTITY_TYPE_VIEW = 'View'
ENTITY_TYPE_LOAD_PROCESS = 'LoadProcess'
ENTITY_TYPE_PREFIX = 'entity_type'
ENTITY_GUID = 'guid'

DATA_CATALOG_UI_URL = 'https://console.cloud.google.com/datacatalog'

ATLAS_STRING_TYPE = 'string'
ATLAS_BOOL_TYPE = 'boolean'
ATLAS_INT_TYPE = 'int'
ATLAS_SHORT_TYPE = 'int'
ATLAS_DOUBLE_TYPE = 'double'
ATLAS_FLOAT_TYPE = 'float'

DATACATALOG_TARGET_DOUBLE_TYPE = [
    ATLAS_SHORT_TYPE, ATLAS_DOUBLE_TYPE, ATLAS_INT_TYPE, ATLAS_FLOAT_TYPE
]

DATACATALOG_TARGET_STRING_TYPE = [ATLAS_STRING_TYPE]

DATACATALOG_TARGET_BOOLEAN_TYPE = [ATLAS_BOOL_TYPE]

DATACATALOG_TARGET_PRIMITIVE_TYPES = [
    *DATACATALOG_TARGET_DOUBLE_TYPE, *DATACATALOG_TARGET_STRING_TYPE,
    *DATACATALOG_TARGET_BOOLEAN_TYPE
]

ATLAS_COLUMN_TYPE = 'Column'

ENTITY_DELETE_EVENT = 'ENTITY_DELETE'
ENTITY_SYNC_EVENT = 'ENTITY_SYNC'
