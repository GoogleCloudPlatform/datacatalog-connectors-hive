# google-datacatalog-apache-atlas-connector

Package for ingesting Apache Atlas metadata into Google Cloud Data Catalog, currently
supporting below asset types:
- Entity Types -> Each Entity Types is converted to a Data Catalog Template with their attribute metadata
- ClassificationDefs -> Each ClassificationDef is converted to a Data Catalog Template
- EntityDefs -> Each Entity is converted to a Data Catalog Entry

Entity attributes are converted to Data Catalog Tags, in case there are Table and Columns relashionships,
 Columns will be converted to Data Catalog Table schema.

Since even Columns are represented as Apache Atlas Entities, this connector, allows users to specify the Entity Types list
to be considered in the ingestion process. If you don't want any type to be created as Data Catalog Entries, use the Entity Types list
arg to provide only the types the connector should sync.

At this time Data Catalog does not support Lineage, so this connector does not use the Lineage information. We might
consider updating this if things change.

[![Python package][3]][3] [![PyPi][4]][5] [![License][6]][6] [![Issues][7]][8]

**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Installation](#1-installation)
  * [1.1. Mac/Linux](#11-maclinux)
  * [1.2. Windows](#12-windows)
  * [1.3. Install from source](#13-install-from-source)
    + [1.3.1. Get the code](#131-get-the-code)
    + [1.3.2. Create and activate a *virtualenv*](#132-create-and-activate-a-virtualenv)
- [2. Environment setup](#2-environment-setup)
  * [2.1. Auth credentials](#21-auth-credentials)
    + [2.1.1. Create a service account and grant it below roles](#211-create-a-service-account-and-grant-it-below-roles)
    + [2.1.2. Download a JSON key and save it as](#212-download-a-json-key-and-save-it-as)
  * [2.2. Set environment variables](#22-set-environment-variables)
- [3. Sample Sync application entry point](#3-sample-sync-application-entry-point)
  * [3.1. Run the google-datacatalog-apache-atlas-connector script](#31-run-the-google-datacatalog-apache-atlas-connector-script)
  * [3.2. Run Docker entry point](#32-run-docker-entry-point)
- [4. Sample Sync Hook application entry point](#4-sample-sync-hook-application-entry-point)
  * [4.1. Run the google-datacatalog-apache-atlas-connector script](#41-run-the-google-datacatalog-apache-atlas-connector-script)
  * [4.2. Run Docker entry point](#42-run-docker-entry-point)
- [5. Developer environment](#5-developer-environment)
  * [5.1. Install and run Yapf formatter](#51-install-and-run-yapf-formatter)
  * [5.2. Install and run Flake8 linter](#52-install-and-run-flake8-linter)
  * [5.3. Run Tests](#53-run-tests)
- [6. Metrics](#6-metrics)
- [7. Assumptions](#7-assumptions)
- [8. Troubleshooting](#8-troubleshooting)

<!-- tocstop -->

-----


## 1. Installation

Install this library in a [virtualenv][2] using pip. [virtualenv][2] is a tool to
create isolated Python environments. The basic problem it addresses is one of
dependencies and versions, and indirectly permissions.

With [virtualenv][2], it's possible to install this library without needing system
install permissions, and without clashing with the installed system
dependencies. Make sure you use Python 3.7+.


### 1.1. Mac/Linux

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
source <your-env>/bin/activate
<your-env>/bin/pip install google-datacatalog-apache-atlas-connector
```

### 1.2. Windows

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
<your-env>\Scripts\activate
<your-env>\Scripts\pip.exe install google-datacatalog-apache-atlas-connector
```

### 1.3. Install from source

#### 1.3.1. Get the code

````bash
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive.git
cd datacatalog-connectors-hive/google-datacatalog-apache-atlas-connector
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
- `<YOUR-CREDENTIALS_FILES_FOLDER>/apache-atlas2dc-credentials.json`

### 2.2. Set environment variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=datacatalog_credentials_file

export APACHE_ATLAS2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
export APACHE_ATLAS2DC_HOST=localhost
export APACHE_ATLAS2DC_PORT=21000
export APACHE_ATLAS2DC_USER=my-user
export APACHE_ATLAS2DC_PASS=my-pass
```

## 3. Sample Sync application entry point

### 3.1. Run the google-datacatalog-apache-atlas-connector sync script
Executes full scrape process in Apache Atlas and sync Data Catalog metadata creating/updating/deleting Entries and Tags.

- Virtualenv

```bash
google-datacatalog-apache-atlas-connector sync \
  --datacatalog-project-id=$APACHE_ATLAS2DC_DATACATALOG_PROJECT_ID \
  --atlas-host=$APACHE_ATLAS2DC_HOST \
  --atlas-port=$APACHE_ATLAS2DC_PORT \
  --atlas-user $APACHE_ATLAS2DC_USER \
  --atlas-pass $APACHE_ATLAS2DC_PASS \
  --atlas-entity-types DB,View,Table,hbase_table,hive_db (Optional)
```

### 3.2. Run Docker entry point
Executes incremental scrape process in Apache Atlas and sync Data Catalog metadata creating/updating/deleting Entries and Tags. This options listen to event changes on Apache Atlas event bus, which is Kafka.

```bash
docker build --rm --tag apache-atlas2datacatalog .
docker run --rm --tty -v <YOUR-CREDENTIALS_FILES_FOLDER>:/data \
  apache-atlas2datacatalog sync \ 
  --datacatalog-project-id=$APACHE_ATLAS2DC_DATACATALOG_PROJECT_ID \
  --atlas-host=$APACHE_ATLAS2DC_HOST \
  --atlas-port=$APACHE_ATLAS2DC_PORT \
  --atlas-user $APACHE_ATLAS2DC_USER \
  --atlas-pass $APACHE_ATLAS2DC_PASS \
  --atlas-entity-types DB,View,Table,hbase_table,hive_db (Optional)
```

## 4. Sample Sync Hook application entry point

### 4.1. Run the google-datacatalog-apache-atlas-connector event-hook script

- Virtualenv

```bash
google-datacatalog-apache-atlas-connector sync-event-hook \
  --datacatalog-project-id=$APACHE_ATLAS2DC_DATACATALOG_PROJECT_ID \
  --atlas-host=$APACHE_ATLAS2DC_HOST \
  --atlas-port=$APACHE_ATLAS2DC_PORT \
  --atlas-user $APACHE_ATLAS2DC_USER \
  --atlas-pass $APACHE_ATLAS2DC_PASS \
  --event-servers my-event-server \
  --event-consumer-group-id atlas-event-sync \
  --atlas-entity-types DB,View,Table,hbase_table,hive_db (Optional)
```

### 4.2. Run Docker entry point

```bash
docker build --rm --tag apache-atlas2datacatalog .
docker run --rm --tty -v <YOUR-CREDENTIALS_FILES_FOLDER>:/data \
  apache-atlas2datacatalog sync-event-hook \ 
  --datacatalog-project-id=$APACHE_ATLAS2DC_DATACATALOG_PROJECT_ID \
  --atlas-host=$APACHE_ATLAS2DC_HOST \
  --atlas-port=$APACHE_ATLAS2DC_PORT \
  --atlas-user $APACHE_ATLAS2DC_USER \
  --atlas-pass $APACHE_ATLAS2DC_PASS \
  --event-servers my-event-server \
  --event-consumer-group-id atlas-event-sync \  
  --atlas-entity-types DB,View,Table,hbase_table,hive_db (Optional)
```

## 5. Developer environment

### 5.1. Install and run Yapf formatter

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

### 5.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 5.3. Run Tests

```bash
python setup.py test
```

## 6. Metrics

[Metrics README.md](docs/README.md)

## 7. Assumptions

The connector uses simple authentication with user/pass provided credentials. And to provide live sync, the connector has an option to connect to the Apache Atlas backed Kafka instance, and listen to metadata event changes. It connects directly to Kafka's topic, so make sure it is executed in a secure network.

For stronger security, consider using Kerberos for authentication and Apache Ranger for authorization: [apache-atlas-security](https://atlas.apache.org/0.8.1/Authentication-Authorization.html). If you have this kind of usage, please open a feature request. 

## 8. Troubleshooting

In the case a connector execution hits Data Catalog quota limit, an error will
be raised and logged with the following detailment, depending on the performed
operation READ/WRITE/SEARCH: 

```
status = StatusCode.RESOURCE_EXHAUSTED
details = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'."
debug_error_string = 
"{"created":"@1587396969.506556000", "description":"Error received from peer ipv4:172.217.29.42:443","file":"src/core/lib/surface/call.cc","file_line":1056,"grpc_message":"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'.","grpc_status":8}"
```

For more information on Data Catalog quota, please refer to: [Data Catalog quota docs][1].

[1]: https://cloud.google.com/data-catalog/docs/resources/quotas
[2]: https://virtualenv.pypa.io/en/latest/
[3]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/workflows/Python%20package/badge.svg?branch=master
[4]: https://img.shields.io/pypi/v/google-datacatalog-apache-atlas-connector.svg
[5]: https://pypi.org/project/google-datacatalog-apache-atlas-connector/
[6]: https://img.shields.io/github/license/GoogleCloudPlatform/datacatalog-connectors-hive.svg
[7]: https://img.shields.io/github/issues/GoogleCloudPlatform/datacatalog-connectors-hive.svg
[8]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/issues
