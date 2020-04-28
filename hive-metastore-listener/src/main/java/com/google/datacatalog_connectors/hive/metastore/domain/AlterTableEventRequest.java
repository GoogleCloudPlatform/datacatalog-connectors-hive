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

public class AlterTableEventRequest {

    public AlterTableEventRequest(TableMetadata oldTable, TableMetadata newTable, String hostName) {
        this.oldTable = oldTable;
        this.newTable = newTable;
        this.hostName = hostName;
    }

    private TableMetadata oldTable;
    private TableMetadata newTable;
    private MetadataEvent event = MetadataEvent.ALTER_TABLE;
    private String hostName;

    public TableMetadata getOldTable() {
        return oldTable;
    }

    public void setOldTable(TableMetadata oldTable) {
        this.oldTable = oldTable;
    }

    public TableMetadata getNewTable() {
        return newTable;
    }

    public void setNewTable(TableMetadata newTable) {
        this.newTable = newTable;
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
