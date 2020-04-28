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

package com.google.datacatalog_connectors.hive.metastore;

import com.google.datacatalog_connectors.hive.metastore.domain.DatabaseMetadata;
import com.google.datacatalog_connectors.hive.metastore.domain.FieldSchemaMetadata;
import com.google.datacatalog_connectors.hive.metastore.domain.StorageDescriptorMetadata;
import com.google.datacatalog_connectors.hive.metastore.domain.TableMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;

public class HiveMetadataBuilder {

    public static DatabaseMetadata getDatabaseMetadata(Database db) {
        DatabaseMetadata dbMetadata = new DatabaseMetadata();
        dbMetadata.setDescription(db.getDescription());
        dbMetadata.setLocationUri(db.getLocationUri());
        dbMetadata.setName(db.getName());
        dbMetadata.setOwnerName(db.getOwnerName());
        dbMetadata.setParameters(db.getParameters());
        return dbMetadata;
    }

    public static TableMetadata getTableMetadata(Table table) {

        TableMetadata tableMetadata = new TableMetadata();

        tableMetadata.setTableName(table.getTableName());
        tableMetadata.setDbName(table.getDbName());
        tableMetadata.setOwner(table.getOwner());
        tableMetadata.setCreateTime(table.getCreateTime());
        tableMetadata.setLastAccessTime(table.getLastAccessTime());
        tableMetadata.setRetention(table.getRetention());

        StorageDescriptorMetadata sdMetadata = getStorageDescriptorMetadata(table);

        tableMetadata.setSd(sdMetadata);
        tableMetadata.setParameters(table.getParameters());
        return tableMetadata;
    }

    private static StorageDescriptorMetadata getStorageDescriptorMetadata(Table tableWithLocation) {
        StorageDescriptor sd = tableWithLocation.getSd();

        StorageDescriptorMetadata sdMetadata = new StorageDescriptorMetadata();
        sdMetadata.setLocation(sd.getLocation());

        List<FieldSchema> fieldsSchemas = sd.getCols();
        List<FieldSchemaMetadata> fieldsSchemaMetadata = getFieldSchemaMetadata(fieldsSchemas);

        sdMetadata.setCols(fieldsSchemaMetadata);
        return sdMetadata;
    }

    private static List<FieldSchemaMetadata> getFieldSchemaMetadata(List<FieldSchema> fieldsSchemas) {
        List<FieldSchemaMetadata> fieldsSchemaMetadata = new ArrayList<>();

        for (FieldSchema fieldSchema: fieldsSchemas) {
            FieldSchemaMetadata fieldSchemaMetadata = new FieldSchemaMetadata();
            fieldSchemaMetadata.setComment(fieldSchema.getComment());
            fieldSchemaMetadata.setName(fieldSchema.getName());
            fieldSchemaMetadata.setType(fieldSchema.getType());
            fieldsSchemaMetadata.add(fieldSchemaMetadata);
        }
        return fieldsSchemaMetadata;
    }

}
