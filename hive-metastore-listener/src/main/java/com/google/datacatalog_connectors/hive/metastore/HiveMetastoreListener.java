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


import com.google.datacatalog_connectors.hive.metastore.domain.CreateDatabaseEventRequest;
import com.google.datacatalog_connectors.hive.metastore.domain.DropDatabaseEventRequest;
import com.google.datacatalog_connectors.hive.metastore.gateways.PubSubWrapper;
import com.google.datacatalog_connectors.hive.metastore.domain.AlterTableEventRequest;
import com.google.datacatalog_connectors.hive.metastore.domain.CreateTableEventRequest;
import com.google.datacatalog_connectors.hive.metastore.domain.DatabaseMetadata;
import com.google.datacatalog_connectors.hive.metastore.domain.DropTableEventRequest;
import com.google.datacatalog_connectors.hive.metastore.domain.TableMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HiveMetastoreListener extends MetaStoreEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveMetastoreListener.class);

  // use the project id from env
  private static final String PROJECT_ID = System.getenv("DATACATALOG_PROJECT_ID");

  // use the topic id from env
  private static final String TOPIC_ID = System.getenv("DATACATALOG_TOPIC_ID");

  // use the host name from env, allows user to specify the name,
  // instead of relying on the OS hostname
  private static final String HOST_NAME = System.getenv("METASTORE_HOST_NAME");

  public HiveMetastoreListener(Configuration config) {
    super(config);
    LOGGER.info("[Thread: " + Thread.currentThread().getName() + "] | [version: 0.3.1] | " +
        "[method: " + Thread.currentThread().getStackTrace()[1].getMethodName() +
        " ] | HiveMetastoreListener created ");
  }

  @Override
  public void onAlterTable(AlterTableEvent event) throws MetaException {
    super.onAlterTable(event);
    Table oldTable = event.getOldTable();
    Table newTable = event.getNewTable();

    TableMetadata oldTableMetadata = HiveMetadataBuilder.getTableMetadata(oldTable);
    TableMetadata newTableMetadata = HiveMetadataBuilder.getTableMetadata(newTable);

    AlterTableEventRequest request = new AlterTableEventRequest(oldTableMetadata,
        newTableMetadata, HOST_NAME);

    sendToPubSub(request);
  }


  @Override
  public void onCreateTable(CreateTableEvent event) throws MetaException {
    super.onCreateTable(event);
    Table table = event.getTable();

    TableMetadata tableMetadata = HiveMetadataBuilder.getTableMetadata(table);

    CreateTableEventRequest request = new CreateTableEventRequest(tableMetadata, HOST_NAME);

    sendToPubSub(request);

  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent event) throws MetaException {
    super.onCreateDatabase(event);
    Database database = event.getDatabase();

    DatabaseMetadata databaseMetadata = HiveMetadataBuilder.getDatabaseMetadata(database);

    CreateDatabaseEventRequest request = new CreateDatabaseEventRequest(
        databaseMetadata, HOST_NAME);

    sendToPubSub(request);
  }

  @Override
  public void onDropTable(DropTableEvent event) throws MetaException {
    super.onDropTable(event);

    Table table = event.getTable();

    TableMetadata tableMetadata = HiveMetadataBuilder.getTableMetadata(table);

    DropTableEventRequest request = new DropTableEventRequest(tableMetadata, HOST_NAME);

    sendToPubSub(request);
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent event) throws MetaException {
    super.onDropDatabase(event);
    Database database = event.getDatabase();

    DatabaseMetadata databaseMetadata = HiveMetadataBuilder.getDatabaseMetadata(database);

    DropDatabaseEventRequest request = new DropDatabaseEventRequest(
        databaseMetadata, HOST_NAME);

    sendToPubSub(request);
  }

  private void sendToPubSub(Object request) {
    try {

      PubSubWrapper pubsub = new PubSubWrapper(PROJECT_ID);
      ObjectMapper objMapper = new ObjectMapper();

      String message = objMapper.writeValueAsString(request);
      List<String> messageIds = pubsub.publishMessage(TOPIC_ID,
          message);
      if (messageIds != null) {
        for (String messageId : messageIds) {
          LOGGER.info("Published with a message id: " + messageId);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Exception Publishing to Pub/Sub: ", e);
      throw new RuntimeException(e);
    }
  }
}
