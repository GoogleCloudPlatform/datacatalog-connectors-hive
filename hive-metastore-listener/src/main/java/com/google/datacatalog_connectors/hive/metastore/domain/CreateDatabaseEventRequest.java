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

public class CreateDatabaseEventRequest {

    public CreateDatabaseEventRequest(DatabaseMetadata database, String hostName) {
        this.database = database;
        this.hostName = hostName;
    }

    private DatabaseMetadata database;
    private MetadataEvent event = MetadataEvent.CREATE_DATABASE;
    private String hostName;

    public DatabaseMetadata getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseMetadata database) {
        this.database = database;
    }

    public MetadataEvent getEvent() {
        return event;
    }

    public void setEvent(MetadataEvent event) {
        this.event = event;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
}
