# Data Engineering Pipeline with dbt, Snowflake, and Airflow

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![dbt](https://img.shields.io/badge/dbt-1.11.0-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.0-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)

A modern ELT pipeline implementation using industry-standard tools to transform raw data into analytical models. This project demonstrates how to build a scalable data pipeline with dbt for transformations, Snowflake as the data warehouse, and Airflow for orchestration.

## Project Overview

This project demonstrates a modern ELT (Extract, Load, Transform) pipeline using dbt (data build tool), Snowflake, and Apache Airflow. It transforms raw TPCH sample data into a dimensional model suitable for analytics.

### Architecture

1. **Snowflake** - Cloud Data Warehouse
   - Handles data storage and computation
   - Implements role-based access control (RBAC)
   - Uses the TPCH sample dataset

2. **dbt (data build tool)** - Transformation Layer
   - Implements the transformation logic
   - Manages data modeling with staging, intermediate, and mart layers
   - Includes data quality tests and documentation

3. **Apache Airflow** - Orchestration
   - Schedules and monitors the data pipeline
   - Manages dependencies between tasks
   - Provides observability through the Airflow UI

### Pipeline Flow

1. **Source Data**
   - Uses Snowflake's TPCH sample data (`snowflake_sample_data.tpch_sf1`)
   - Includes tables: `orders` and `lineitem`

2. **Staging Layer**
   - `stg_tpch_orders`: Raw order data
   - `stg_tpch_line_items`: Raw line item data with surrogate key generation
   - Implements source freshness and data quality tests

3. **Intermediate Models**
   - `int_order_items`: Joins orders and line items
   - `int_order_items_summary`: Aggregates order metrics

4. **Data Marts**
   - `fct_orders`: Final fact table with business metrics
   - Implements business logic like discount calculations

### Data Quality

1. **Generic Tests**
   - Primary key uniqueness
   - Referential integrity
   - Accepted values for status codes

2. **Singular Tests**
   - Valid date ranges
   - Business logic validations
   - Custom SQL validations

### 🛠️ Technical Implementation

#### Snowflake Setup
Run the following SQL in your Snowflake account to set up the required resources:

```sql
-- Create warehouse, database, and role
USE ROLE accountadmin;

CREATE WAREHOUSE dbt_wh WITH WAREHOUSE_SIZE='x-small';
CREATE DATABASE IF NOT EXISTS dbt_db;
CREATE ROLE IF NOT EXISTS dbt_role;

-- Grant necessary permissions
GRANT ROLE dbt_role TO USER <your_username>;
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_role;
GRANT ALL ON DATABASE dbt_db TO ROLE dbt_role;

-- Create schema
USE ROLE dbt_role;
CREATE SCHEMA IF NOT EXISTS dbt_db.dbt_schema;
```

#### dbt Setup
```bash
dbt init

```
1. Install dbt:
   ```bash
   pip install dbt-snowflake
   ```

2. Configure your `profiles.yml`:
   ```yaml
   your_profile_name:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: <your_account>
         user: <your_username>
         password: <your_password>
         role: dbt_role
         database: dbt_db
         warehouse: dbt_wh
         schema: dbt_schema
         threads: 4
   ```
##### Key Features
- **Macros**: Reusable SQL components (e.g., `discounted_amount`)
- **Tests**: Data quality checks and validations
- **Documentation**: Self-documenting data models
- **Incremental Loading**: Efficient data processing


##### dbt Project Structure

```
data_engineering_pipeline/
├── analyses/         # Analysis files (e.g., Jupyter notebooks)
├── macros/           # Reusable SQL snippets and functions
│   └── pricing.sql   # Custom macros for pricing calculations
├── models/           # dbt models
│   ├── marts/        # Business-facing models
│   │   ├── fct_orders.sql
│   │   ├── int_order_items.sql
│   │   └── int_order_items_summary.sql
│   └── staging/      # Raw data transformations
│       ├── stg_tpch_lineitems.sql
│       ├── stg_tpch_orders.sql
│       └── tpch_sources.yml
├── seeds/            # Seed data files
├── snapshots/        # dbt snapshot definitions
└── tests/            # Custom data tests
    ├── fct_orders_date_valid.sql
    └── fct_orders_discount.sql
```
##### Running the Project

1. Install dependencies:
   ```bash
   dbt deps
   ```

2. Run the models:
   ```bash
   dbt run
   ```

3. Run tests:
   ```bash
   dbt test
   ```

4. Generate documentation:
   ```bash
   dbt docs generate
   dbt docs serve
   ```
#### Airflow Setup
1. Install Airflow:
   ```bash
   pip install apache-airflow
   ```
2. install astro:
```bash
pip install astro
```
3. Create reposity:
```bash
mkdir dbt-dag
cd dbt-dag
```
4. Initialize Astro:
```bash
astro dev init
```
4. Update Dockerfile:
```bash
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```
5. Update requirements.txt:
```bash
astronomer-cosmos
apache-airflow-providers-snowflake
```
6. Set connection in Airflow:
open http://localhost:8080

Add snowflake_conn in UI
```json
{
  "account": "<account_locator>-<account_name>",
  "warehouse": "dbt_wh",
  "database": "dbt_db",
  "role": "dbt_role",
  "insecure_mode": false
}
```
7. Create the DAG:
 Create the DAG file [dags/dbt_dag.py](cci:7://file://dbt-dag/dags/dbt_dag.py:0:0-0:0)


#####    Key Components

1. **DAG Definition** ([dags/dbt_dag.py](cci:7://file://dbt-dag/dags/dbt_dag.py:0:0-0:0))
   - Schedules and manages the dbt pipeline
   - Implements proper error handling and retries
   - Configures dbt project and profile settings

2. **Profile Configuration**
   - Uses `SnowflakeUserPasswordProfileMapping` to integrate with Airflow connections
   - Securely manages Snowflake credentials through Airflow's connection system

3. **Execution**
   - Runs dbt commands in a containerized environment
   - Tracks task status and logs in the Airflow UI
   - Supports both full refreshes and incremental loads

##### Viewing Results
- dbt Models: Check the dbt_schema in your Snowflake database
- Logs: View detailed execution logs in the Airflow UI
- Documentation: Access dbt docs at http://localhost:8080/dbt-docs/