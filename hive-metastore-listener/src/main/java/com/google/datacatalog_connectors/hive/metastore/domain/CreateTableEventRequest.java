/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.datacatalog_connectors.hive.metastore.domain;

public class CreateTableEventRequest {

    public CreateTableEventRequest(TableMetadata table, String hostName) {
        this.table = table;
        this.hostName = hostName;
    }

    private TableMetadata table;
    private MetadataEvent event = MetadataEvent.CREATE_TABLE;
    private String hostName;

    public TableMetadata getTable() {
        return table;
    }

    public void setTable(TableMetadata table) {
        this.table = table;
    }

    public MetadataEvent getEvent() {
        return event;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getHostName() {
        return hostName;
    }
}
