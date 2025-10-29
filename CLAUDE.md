## Data Model V3 Physical Schema Transformation - HP-12735

### Project Overview
The relevant Jira ticket for this work can be found [here](https://conviva.atlassian.net/browse/HP-12735). The goal here is to read the existing production data and transform it into the new data model schema format. Details on that are below.

The relevant Confluence documentation for this ticket can be found [here](https://conviva.atlassian.net/wiki/spaces/fusion/pages/3421864070/ECO+Evaluation).

The goal of this ticket is to load some data from the source database, perform transformations on it and then load the transformed data into the destination database into the relevant tables. We will only focus on the `eco_cross_page_flow_pt1m_local` table.

#### Background Context
##### Current Data Model (Production)

- Uses ClickHouse tables with Map columns for dynamic metrics storage
- Example table: eco_cross_page_flow_pt1m_local (90 columns)
- Map columns: metricIntGroup1-15, metricFloatGroup1-15 (30 total map columns)
- Each Map column contains multiple key-value pairs like {"31_1_failures": 1, "31_1_successes": 0}
- Query the data and fetch it by using something like 
  ```shell
  curl 'http://clickhouse-ds-00.us-east4.prod.gcp.conviva.com:8123/' \
  --data-binary "SELECT *
                 FROM default.eco_cross_page_flow_pt1m_local
                 LIMIT 100"
  ```

##### Target Data Model (New Physical Schema)

- Flattened primitive columns: int1, int2, int3, ..., float1, float2, float3, ...
- Each map key becomes its own dedicated column
- No more Map columns - all metrics stored in primitive types
- Same standard columns preserved: timestampMs, customerId, clientId, etc.
- Pre-aggregation must be done to sum up metrics across rows that fall within a minute boundary
- The `<clientId, sessionId>` becomes a composite primary key and any entries with the same combination of `<timestamp, clientId, sessionId>` become one row. The only aggregation function that will be applied is summing.
- The connection details for the destination database are express through the query below
  ```shell
  curl "http://rccp301-34a.iad6.prod.conviva.com:8123/?query=SELECT%201"
  ```
- The destination database has the following tables defined
  * eco_cross_page_flow_pt1m_local_20251008 - contains an exact copy of the source data for 20251008. The source database keeps data only for 2 weeks. This maintains a permanent copy for comparison
  * eco_cross_page_preagg_pt1m_test - contains preaggregated data for just one customer
  * eco_cross_page_preagg_pt1m_test_mul_cust - contains preaggregated data for 4 customers

### Project Requirements
#### Input Data Structure
Current production table structure:

```
sql-- Table: eco_cross_page_flow_pt1m_local
-- Sample columns visible:
-- metricIntGroup3: {"31_1_failures": 1, "31_1_successes": 0}
-- metricIntGroup4: {}
-- metricIntGroup5: {}
-- metricIntGroup6: {"35_1_failures": 1, "35_1_successes": 0}
-- metricIntGroup7: {}
-- metricIntGroup8: {"minute_of_dns_lookup_time_morethan_3000": 0, "minute_of_document_response_time_morethan_3000": 0}
```

Standard columns to preserve:

- timestampMs DateTime64(3)
- customerId Int32
- clientId String
- sessionId UInt256
- platform LowCardinality(String)
- countryIso LowCardinality(String)
- deviceName LowCardinality(String)
- And other metadata columns (exclude tagGroup* and flowId, flowStartTimeMs)

#### Output Data Structure
New pre-aggregated table with:

- All standard columns preserved
- Primitive columns: int1 Int32, int2 Int32, ..., intN Int32
- Primitive columns: float1 Float32, float2 Float32, ..., floatN Float32
- Default values of 0 for unused columns
- Sum up metrics with the same minute boundary across rows with the same clientId, sessionId combination

#### Key Mapping Strategy
Since no configuration mapping is available, create deterministic alphabetical ordering:

- Extract all unique keys from all Map columns across 1 day of data
- Sort integer metric keys alphabetically → assign to int1, int2, int3, ...
- Sort float metric keys alphabetically → assign to float1, float2, float3, ...
- Save mapping as JSON file for reference and validation

### Technical Implementation Requirements
#### Dependencies

- clickhouse-connect Python library for ClickHouse database interaction
- Standard Python libraries: json, collections
- anything else that is required

#### Database Connection

- Connect to ClickHouse cluster
- Source table: defined in src/config/settings.py
- Target table: defined in src/config/settings.py

#### Data Processing Approach

- Key Discovery Phase: Query production table to extract all unique map keys
- Mapping Generation: Create deterministic alphabetical key-to-column mapping
- Schema Creation: Generate new table DDL with appropriate number of primitive columns
- Data Transformation: Process data in batches, flatten map values to primitive columns
- Validation: Verify row counts and spot-check metric transformations

### Validation Requirements

- Compare total row count between source and target tables
- Spot-check known metrics: verify specific map key values match primitive column values
- Generate summary statistics on transformation success rate
- Save key mapping as JSON for future reference

### Success Criteria

- Complete transformation of 1 day of ECO cross_page data
- All map keys successfully mapped to primitive columns
- Row count preservation (source count = target count)
- Sample validation shows correct metric value mapping
- New table queryable with standard SQL operations
- Generated mapping file saved for documentation

### Dev Workflow
- Follow standard Python development practises like creating a virtual environment. Use `poetry` to manage all of this.
- Run the transformation pipeline using: `poetry run transform` or `poetry run python run_transformation_pipeline.py`
- Run the benchmark pipeline using: `poetry run benchmark` or `poetry run python run_benchmark_pipeline.py`
- Run the view benchmark pipeline using `poetry run view-benchmark` or `poetry run python run_view_benchmark_pipeline.py`
- When running queries, always use a reasonable timeout so that the connection doesn't hang while running queries on massive datasets.
