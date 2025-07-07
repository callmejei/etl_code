from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from cosmos import DbtDag,ProjectConfig,ProfileConfig,RenderConfig
from pathlib import Path
profile_config='/home/hadoop/.dbt/profiles.yml'

render_config=RenderConfig(
          select=["path:models/example","path:models/t0100_customer"]
        )
dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig("/home/hadoop/hive",
    ),
    profile_config = ProfileConfig(
        profile_name="hive",
        target_name="dev",
        profiles_yml_filepath="/home/hadoop/.dbt/profiles.yml",  # Local path to dbt profiles.yml
    ),
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    render_config=render_config,
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dag",
    default_args={"retries": 2},
)
