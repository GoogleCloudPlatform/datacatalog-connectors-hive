#!/usr/bin/env bash
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

gcloud iam service-accounts create hive-sync-ps-sa --display-name="Cloud Run PubSub"

gcloud beta run services add-iam-policy-binding hive-sync --member=serviceAccount:hive-sync-ps-sa@${HIVE2DC_DATACATALOG_PROJECT_ID}.iam.gserviceaccount.com --role=roles/run.invoker

gcloud beta pubsub topics create ${HIVE2DC_DATACATALOG_TOPIC_ID}

gcloud beta pubsub subscriptions create hive-sync-sub --push-endpoint=${HIVE2DC_DATACATALOG_APP_ENDPOINT} --push-auth-service-account=hive-sync-ps-sa@${HIVE2DC_DATACATALOG_PROJECT_ID}.iam.gserviceaccount.com --topic projects/${HIVE2DC_DATACATALOG_PROJECT_ID}/topics/${HIVE2DC_DATACATALOG_TOPIC_ID}
