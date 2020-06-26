# datacatalog-connectors-hive

This repository contains sample code with integration between Data Catalog and Hive and related data source.

**Disclaimer: This is not an officially supported Google product.**

![Python package](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/workflows/Python%20package/badge.svg?branch=master)

## **Breaking Changes in v0.5.0**

The package names were renamed, if you are still using the older version use the branch: [release-v0.0.0](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/tree/release-v0.0.0)

## Project structure

Each subfolder contains a Python package. Please check components' README files for
details.

The following components are available in this repo:

| Component | Description | Folder | Language | 
|-----------|-------------|--------|----------|
| google-datacatalog-hive-connector | Sample code for Hive data source. | [google-datacatalog-hive-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/tree/master/google-datacatalog-hive-connector) | Python |
| google-datacatalog-apache-atlas-connector | Sample code for Apache Atlas data source. | [google-datacatalog-apache-atlas-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/tree/master/google-datacatalog-apache-atlas-connector) | Python |
| hive-metastore-listener | Sample code enabling Hive Metastore and Data Catalog live sync. | [hive-metastore-listener](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/tree/master/hive-metastore-listener) | Java |
