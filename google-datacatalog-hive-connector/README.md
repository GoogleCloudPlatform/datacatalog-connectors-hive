# google-datacatalog-hive-connector

Library for ingesting Hive metadata into Google Cloud Data Catalog. 
You are able to directly connect to your Hive Metastore or Consume message events using Cloud Run.

This connector is prepared to work with the Hive Metastore 2.3.0 version,
backed by a PostgreSQL database.
 
[![Python package][2]][2] [![PyPi][3]][4] [![License][5]][5] [![Issues][6]][7]

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
  * [1.3. Virtualenv](#13-virtualenv)
      - [1.3.1. Install Python 3.6+](#131-install-python-36)
      - [1.3.2. Create and activate a *virtualenv*](#132-create-and-activate-a-virtualenv)
      - [1.3.3. Install the dependencies](#133-install-the-dependencies)
      - [1.3.4. Set environment variables to connect to your Hive Metastore](#134-set-environment-variables-to-connect-to-your-hive-metastore)
  * [1.4. Docker](#14-docker)
- [2. Sample application entry point](#2-sample-application-entry-point)
  * [2.1. Run main.py to connect to your Hive Metastore](#21-run-mainpy-to-connect-to-your-hive-metastore)
- [3. Deploy Message Event Consumer on Cloud Run (Optional)](#3-deploy-message-event-consumer-on-cloud-run-optional)
  * [3.1. Set environment variables to deploy to Cloud Run](#31-set-environment-variables-to-deploy-to-cloud-run)
  * [3.2. Execute the deploy script](#32-execute-the-deploy-script)
  * [3.3. Create your Pub/Sub topic and subscription](#33-create-your-pubsub-topic-and-subscription)
    + [3.3.1 Set additional environment variables](#331-set-additional-environment-variables)
    + [3.3.2 Execute pubsub config script](#332-execute-pubsub-config-script)
    + [3.3.4 Send a message to your Pub/Sub topic to test](#334-send-a-message-to-your-pubsub-topic-to-test)
- [4. Tools (Optional)](#4-tools-optional)
  * [4.1. Clean up all entries on DataCatalog from the hive entrygroup](#41-clean-up-all-entries-on-datacatalog-from-the-hive-entrygroup)
  * [4.2. Sample of Hive2Datacatalog Library usage](#42-sample-of-hive2datacatalog-library-usage)
- [5. Developer environment](#5-developer-environment)
  * [5.1. Install and run Yapf formatter](#51-install-and-run-yapf-formatter)
  * [5.2. Install and run Flake8 linter](#52-install-and-run-flake8-linter)
  * [5.3. Run Tests](#53-run-tests)
- [6. Metrics](#6-metrics)
- [7. Connector Architecture](#7-connector-architecture)
- [8. Troubleshooting](#8-troubleshooting)

<!-- tocstop -->

-----

## 1. Installation

Install this library in a [virtualenv][1] using pip. [virtualenv][1] is a tool to
create isolated Python environments. The basic problem it addresses is one of
dependencies and versions, and indirectly permissions.

With [virtualenv][1], it's possible to install this library without needing system
install permissions, and without clashing with the installed system
dependencies. Make sure you use Python 3.7+.


### 1.1. Mac/Linux

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
source <your-env>/bin/activate
<your-env>/bin/pip install google-datacatalog-hive-connector
```

### 1.2. Windows

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
<your-env>\Scripts\activate
<your-env>\Scripts\pip.exe install google-datacatalog-hive-connector
```

### 1.3. Install from source

#### 1.3.1. Get the code

````bash
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/
cd datacatalog-connectors-hive/google-datacatalog-hive-connector
````

#### 1.3.2. Create and activate a *virtualenv*

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env> 
source <your-env>/bin/activate
```

## 2. Environment setup

### 2.1. Auth credentials

#### 2.1.1. Create a service account and grant it below roles

- Data Catalog Admin

#### 2.1.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/hive2dc-datacatalog-credentials.json`

> Please notice this folder and file will be required in next steps.

### 2.2. Set environment variables to connect to your Hive Metastore

Replace below values according to your environment:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=data_catalog_credentials_file

export HIVE2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
export HIVE2DC_DATACATALOG_LOCATION_ID=us-google_cloud_location_id
export HIVE2DC_HIVE_METASTORE_DB_HOST=hive_metastore_db_server
export HIVE2DC_HIVE_METASTORE_DB_USER=hive_metastore_db_user
export HIVE2DC_HIVE_METASTORE_DB_PASS=hive_metastore_db_pass
export HIVE2DC_HIVE_METASTORE_DB_NAME=hive_metastore_db_name
export HIVE2DC_HIVE_METASTORE_DB_TYPE=hive_metastore_db_type

```

## 3. Run entry point

### 3.1. Run Python entry point

- Virtualenv

```bash
google-datacatalog-hive-connector \
--datacatalog-project-id=$HIVE2DC_DATACATALOG_PROJECT_ID \
--datacatalog-location-id=$HIVE2DC_DATACATALOG_LOCATION_ID \
--hive-metastore-db-host=$HIVE2DC_HIVE_METASTORE_DB_HOST \
--hive-metastore-db-user=$HIVE2DC_HIVE_METASTORE_DB_USER \
--hive-metastore-db-pass=$HIVE2DC_HIVE_METASTORE_DB_PASS \
--hive-metastore-db-name=$HIVE2DC_HIVE_METASTORE_DB_NAME \
--hive-metastore-db-type=$HIVE2DC_HIVE_METASTORE_DB_TYPE    
```

### 3.2. Run Docker entry point
In case you have your Hive metastore DB running in your localhost environment, pass --network="host"

```bash
docker build -t hive2datacatalog .
docker run --network="host" --rm --tty -v data:/data hive2datacatalog --datacatalog-project-id=$HIVE2DC_DATACATALOG_PROJECT_ID --datacatalog-location-id=$HIVE2DC_DATACATALOG_LOCATION_ID --hive-metastore-db-host=$HIVE2DC_HIVE_METASTORE_DB_HOST --hive-metastore-db-user=$HIVE2DC_HIVE_METASTORE_DB_USER --hive-metastore-db-pass=$HIVE2DC_HIVE_METASTORE_DB_PASS --hive-metastore-db-name=$HIVE2DC_HIVE_METASTORE_DB_NAME --hive-metastore-db-type=$HIVE2DC_HIVE_METASTORE_DB_TYPE  
```

## 4. Deploy Message Event Consumer on Cloud Run (Optional)

### 4.1. Set environment variables to deploy to Cloud Run
Replace below values according to your environment:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=data_catalog_credentials_file

export HIVE2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
export HIVE2DC_DATACATALOG_LOCATION_ID=us-google_cloud_location_id

```
### 4.2. Execute the deploy script
```bash
source deploy.sh
```

If the deploy succeeded, you will be presented the Cloud Run endpoint, example:
https://hive-sync-example-uc.a.run.app

Save the endpoint which will be needed for the next step.

### 4.3. Create your Pub/Sub topic and subscription

#### 4.3.1 Set additional environment variables

Replace with your Cloud Run endpoint:
```bash
export HIVE2DC_DATACATALOG_TOPIC_ID=google_cloud_topic_id
export HIVE2DC_DATACATALOG_APP_ENDPOINT=https://hive-sync-example-uc.a.run.app
```

#### 4.3.2 Execute pubsub config script
```bash
source tools/create_pub_sub_run_invoker.sh
```

#### 4.3.4 Send a message to your Pub/Sub topic to test
You can look at valid message events examples on: tools/resources/*.json

## 4. Tools (Optional)

### 5.1. Clean up all entries on DataCatalog from the hive entrygroup
run ```python tools/cleanup_datacatalog.py```

### 5.2. Sample of Hive2Datacatalog Library usage
run ```python tools/hive2datacatalog_client_sample.py```


## 6. Developer environment

### 6.1. Install and run Yapf formatter

```bash
pip install --upgrade yapf

# Auto update files
yapf --in-place --recursive src tests

# Show diff
yapf --diff --recursive src tests

# Set up pre-commit hook
# From the root of your git project.
curl -o pre-commit.sh https://raw.githubusercontent.com/google/yapf/master/plugins/pre-commit.sh
chmod a+x pre-commit.sh
mv pre-commit.sh .git/hooks/pre-commit
```

### 6.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 6.3. Run Tests

```bash
python setup.py test
```

## 7. Metrics

[Metrics README.md](docs/Metrics.md)

## 8. Connector Architecture

[Architecture README.md](docs/Architecture.md)

## 9. Troubleshooting

In the case a connector execution hits Data Catalog quota limit, an error will be raised and logged with the following detailement, depending on the performed operation READ/WRITE/SEARCH: 
```
status = StatusCode.RESOURCE_EXHAUSTED
details = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'."
debug_error_string = 
"{"created":"@1587396969.506556000", "description":"Error received from peer ipv4:172.217.29.42:443","file":"src/core/lib/surface/call.cc","file_line":1056,"grpc_message":"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'.","grpc_status":8}"
```
For more info about Data Catalog quota, go to: [Data Catalog quota docs](https://cloud.google.com/data-catalog/docs/resources/quotas).

[1]: https://virtualenv.pypa.io/en/latest/
[2]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/workflows/Python%20package/badge.svg?branch=master
[3]: https://img.shields.io/pypi/v/google-datacatalog-hive-connector.svg
[4]: https://pypi.org/project/google-datacatalog-hive-connector/
[5]: https://img.shields.io/github/license/GoogleCloudPlatform/datacatalog-connectors-hive.svg
[6]: https://img.shields.io/github/issues/GoogleCloudPlatform/datacatalog-connectors-hive.svg
[7]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/issues