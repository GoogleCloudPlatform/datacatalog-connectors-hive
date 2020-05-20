# hive-metastore-listener

Library that works as an agent running on your Hive Metastore, subscribes to some Metastore Events, currently we
support: Create/Alter Tables Events, for each Metastore Event the agent sends a message to Pub/Sub with the processed entity metadata.

**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Environment setup](#1-environment-setup)
  * [1.1. Get the code](#11-get-the-code)
  * [1.2. Auth credentials](#12-auth-credentials)
      - [1.2.1. Create a service account and grant it below roles](#121-create-a-service-account-and-grant-it-below-roles)
      - [1.2.2. Download a JSON key and save it as](#122-download-a-json-key-and-save-it-as)
  * [1.3. Environment](#13-environment)
      - [1.3.1. Install Java 8](#131-install-java-8)
      - [1.3.2. Install maven](#132-install-maven)
      - [1.3.3. Test your dependencies](#133-test-your-dependencies)
  * [1.4. Build a jar file with all the dependencies](#14-build-a-jar-file-with-all-the-dependencies)
  * [1.5. Send the jar file and the service account to your hive metastore machine.](#15-send-the-jar-file-and-the-service-account-to-your-hive-metastore-machine)
  * [1.6. Add Environment variables and add the jar file to your Hive Metastore classpath](#16-add-environment-variables-and-add-the-jar-file-to-your-hive-metastore-classpath)
  * [1.7. Linux environment:](#17-linux-environment)
  * [1.8. Restart your Hive Metastore](#18-restart-your-hive-metastore)

<!-- tocstop -->

-----

## 1. Environment setup

### 1.1. Get the code

````bash
git clone https://.../hive-metastore-listener.git
cd hive-metastore-listener
````

### 1.2. Auth credentials

##### 1.2.1. Create a service account and grant it below roles

- Pub/Sub Publisher

##### 1.2.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/hive-metastore-listener-credentials.json`

### 1.3. Environment

##### 1.3.1. Install Java 8

##### 1.3.2. Install maven

Make sure your JAVA_HOME environment variable is configured properly

##### 1.3.3. Test your dependencies
````bash
java -version
mvn -v
````

### 1.4. Build a jar file with all the dependencies

At the root folder of your repo, run: 
````bash
mvn assembly:assembly
````

A file named ./target/hive-metastore-hook-1.0-SNAPSHOT-jar-with-dependencies.jar should be generated.

### 1.5. Send the jar file and the service account to your hive metastore machine.
If your environment has a external hive metastore (Running in a separate process than the hive server),
 you must send the jar file to the machine that's running the hive metastore. Another option is to send the jar file to hdfs.

### 1.6. Add Environment variables and add the jar file to your Hive Metastore classpath
This step depends on your environment, once the jar file is inside the Hive Metastore, you have to edit the following files:

### 1.7. Linux environment:
At the end of the file, add the lines:
````bash
/opt/hive/conf/hive-env.sh
````

````bash
export HIVE_AUX_JARS_PATH=/hive-metastore-listener-1.0-SNAPSHOT-jar-with-dependencies.jar
export GOOGLE_APPLICATION_CREDENTIALS=/hive-metastore-listener-credentials.json
export DATACATALOG_PROJECT_ID=your-google-project
export DATACATALOG_TOPIC_ID=your-pubsub-topic
export METASTORE_HOST_NAME=your-metastore-host-name
````

At the end of the file, add the line:
````bash
/opt/hive/conf/hive-site.xml
````

````xml
Add the tag before the closing configuration tag </configuration>:

<property><name>hive.metastore.event.listeners</name><value>com.google.datacatalog_connectors.hive.metastore.HiveMetastoreListener</value></property>

````

### 1.8. Restart your Hive Metastore
Verify on the log that your Metastore Listener have been registered.
At the file /tmp/root/hive.log (if you use a different user to run your Hive Metastore, replace it to the correct one)

You should see a line like this:
````bash
2019-09-18T18:07:50,046 INFO  [main]: metastore.HiveMetastoreListener (HiveMetastoreListener.java:<init>(31)) - [Thread: main] | [version: 0.2] | [method: <init> ] | HiveMetastoreListener created
````