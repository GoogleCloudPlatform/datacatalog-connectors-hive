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

package com.google.datacatalog_connectors.hive.metastore.gateways;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Lists;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PubSubWrapper {
    private static final Logger log = LoggerFactory.getLogger(PubSubWrapper.class);
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private final String project;
    private Pubsub pubsub;

    public PubSubWrapper(String project) {
        this.project = String.format("projects/%s", project);
        this.pubsub = createClient();
    }

    private Pubsub createClient(){
        try {
            HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
            GoogleCredential credential = GoogleCredential.getApplicationDefault(transport,
                JSON_FACTORY);

            if (credential.createScopedRequired()) {
                credential = credential.createScoped(PubsubScopes.all());
            }
            // Please use custom HttpRequestInitializer for automatic retry upon failures.
            //        HttpRequestInitializer initializer =
            //        new RetryHttpInitializerWrapper(credential);
            return pubsub = new Pubsub.Builder(transport, JSON_FACTORY, credential)
                    .setApplicationName("hiveMetastoreHook")
                    .build();
        } catch (Exception e) {
            log.error("Exception creating Pub/Sub instance: ", e);
            throw new RuntimeException(e);
        }

    }

    public List<String> publishMessage(String topicName, String data) throws IOException {
        List<PubsubMessage> messages = Lists.newArrayList();
        messages.add(new PubsubMessage().encodeData(data.getBytes("UTF-8")));
        PublishRequest publishRequest = new PublishRequest().setMessages(messages);
        PublishResponse publishResponse = pubsub.projects().topics()
                .publish(getTopic(topicName), publishRequest)
                .execute();
        return publishResponse.getMessageIds();
    }

    private String getTopic(String topicName) {
        return String.format("%s/topics/%s", project, topicName);
    }

}