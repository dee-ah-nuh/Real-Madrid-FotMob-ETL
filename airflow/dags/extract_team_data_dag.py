from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import json
import glob

default_args = {
    "owner": "airflow",
    "retries": 1,
}

CONFIG_DIR = "/opt/airflow/config/teams"


def run_team_extraction(config_path, season):
    from extract.extract_fotmob_data import run_extraction
    return run_extraction(config_path, season)

with DAG(
    dag_id="fotmob-etl-extract_teams",
    default_args=default_args,
    schedule="0 6 * * *",  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fotmob", "extraction"],
) as dag:
    
    # Find all team config files
    config_files = glob.glob(f"{CONFIG_DIR}/*.json")
    
    for config_path in config_files:
        team_name = os.path.basename(config_path).replace(".json", "")
        
        # Load config to get seasons
        with open(config_path) as f:
            config = json.load(f)
        
        for season in config.get("seasons", []):
            season_str = season.replace("/", "_")
            
            PythonOperator(
                task_id=f"extract_{team_name}_{season_str}",
                python_callable=run_team_extraction,
                op_args=[config_path, season],
            )