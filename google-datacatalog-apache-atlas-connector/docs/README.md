[Back to README.md](../README.md)

# Metrics

This execution was collected from an Apache Atlas 1.0.0 instance populated with 1013 Tables, 1 StorageDesc, 3026 Columns, 2 Views, 3 Databases and 3 LoadProcesses, resulting in 4048 entities, running the apache-atlas2datacatalog connector to ingest those entities into Data Catalog. This shows what the user might expect when running this connector.

The following metrics are not a guarantee, they are approximations that may change depending on the environment, network and execution.


| Metric                     | Description                                       | VALUE             |
| ---                        | ---                                               | ---               |
| **elapsed_time**           | Elapsed time from the execution.                  | 90 Minutes        |
| **entries_length**         | Number of entities ingested into Data Catalog.    | 4048              |
| **metadata_payload_bytes** | Amount of bytes processed from the source system. | 4537882 (4.54 MB) |
| **datacatalog_api_calls**  | Amount of Data Catalog API calls executed.        | 17000             |

The elapsed time grows linearly, so for each 1000 entities should take more ~20 minute. Keep in mind that the memory/cpu consuption will also grown, since the Scrape process does a single call at the beginning to retrieve the metadata. 

For reference Data catalog provides a free tier of `1 million API calls` in a month, and `$10 per 100,000 API calls` over `1 million API calls`.

For the most up-to-date info about Data Catalog billing, go to: [Data Catalog billing docs](https://cloud.google.com/data-catalog/pricing).

