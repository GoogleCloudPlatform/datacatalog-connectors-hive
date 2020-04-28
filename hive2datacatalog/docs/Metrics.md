[Back to README.md](../README.md)

# Metrics

This execution was collected from a Hive Metastore 2.3.0, backed by a PostgreSQL database instance populated with 993 tables distributed into 8 databases, running the hive2datacatalog connector to ingest
those entities into Data Catalog. This shows what the user might expect when running this connector.

The following metrics are not a guarantee, they are approximations that may change depending on the environment, network and execution.


| Metric                     | Description                                       | VALUE            |
| ---                        | ---                                               | ---              |
| **elapsed_time**           | Elapsed time from the execution.                  | 18 Minutes       |
| **entries_length**         | Number of entities ingested into Data Catalog.    | 1001             |
| **metadata_payload_bytes** | Amount of bytes processed from the source system. | 7346726 (7.4 MB) |
| **datacatalog_api_calls**  | Amount of Data Catalog API calls executed.        | 4029             |



### Data Catalog API calls drilldown

| Data Catalog API Method                                                 | Calls |
| ---                                                                     | ---   | 
| **google.cloud.datacatalog.v1beta1.DataCatalog.CreateEntry#200**        | 1001  | 
| **google.cloud.datacatalog.v1beta1.DataCatalog.CreateEntryGroup#200**   | 1     |
| **google.cloud.datacatalog.v1beta1.DataCatalog.CreateEntryGroup#409**   | 7     |  
| **google.cloud.datacatalog.v1beta1.DataCatalog.CreateTag#200**          | 1001  |
| **google.cloud.datacatalog.v1beta1.DataCatalog.CreateTagTemplate#200**  | 2     |
| **google.cloud.datacatalog.v1beta1.DataCatalog.CreateTagTemplate#409**  | 14    |
| **google.cloud.datacatalog.v1beta1.DataCatalog.GetEntry#403**           | 1001  | 
| **google.cloud.datacatalog.v1beta1.DataCatalog.ListTags#200**           | 1001  | 
| **google.cloud.datacatalog.v1beta1.DataCatalog.SearchCatalog#200**      | 1     |  
