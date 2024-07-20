from datetime import datetime
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

from airflow import DAG
from airflow.utils.dates import days_ago

from pathlib import Path

# Define your dbt project path
dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject")

# Define profile configuration for Snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "opportunity",
            "schema": "consumption_schema"
        }
    )
)

# Define dbt DAG configuration
dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(dbt_project_path),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"),
    dag_id="dbt_snowflake_dag",
    schedule_interval="@daily",  # Run daily
    start_date=datetime(2024, 7, 9),
    catchup=False
)

# Define your Airflow DAG


    # Optionally, define dependencies or other tasks in the DAG
