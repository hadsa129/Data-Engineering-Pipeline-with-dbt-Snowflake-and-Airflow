import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Path to dbt project in the container
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/data_engineering_pipeline"

# Configure profile using Airflow connection
profile_config = ProfileConfig(
    profile_name="data_engineering_pipeline",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",  # This must match your Airflow connection ID
        profile_args={
            "database": "dbt_db",
            "schema": "dbt_schema",
            "warehouse": "dbt_warehouse",
            "role": "dbt_role"
        }
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_PATH,
        # Explicitly set the target path to avoid 'target-path' error
        project_name="data_engineering_pipeline"
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": True,
        "vars": '{"target_schema": "dbt_schema"}'
    },
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=os.path.join(
            os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"),
            "dbt_venv",
            "bin",
            "dbt"
        ),
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 11, 24),
    catchup=False,
    dag_id="dbt_dag",
)