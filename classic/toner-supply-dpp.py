import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email
from cumulus_libs.peptobase import PeptoDAG, PeptoDatabricksTaskList

namespace   = Variable.get("namespace")
source_vars = Variable.get("cumulus-toner_supply_dpp", deserialize_json=True)
common_vars = Variable.get("cumulus-common-variables", deserialize_json=True)

if Variable.get("namespace") == "airflow2-dev":
    envs = ["dailyCI"]
    env_for_deployment = "DAILY"
elif Variable.get("namespace") == "airflow2-itg":
    envs = ["ITG"]
    env_for_deployment = "ITG"
elif Variable.get("namespace") == "airflow2-prod":
    envs = ["PROD"]
    env_for_deployment = "PROD"

dag = PeptoDAG(
    dag_id="toner_supply_dpp",
    schedule_interval="0 10 * * *",
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=send_failure_email,
)

with dag:
    raw_task_list = PeptoDatabricksTaskList(
        process="dps_raw",
        super_package="raw_pepto",
        package="dps_raw_toner_pepto",
        pool=None,
        previous_task_ids=None,
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_TONER_SUPPLY_DPP_RAW_{env_for_deployment.upper()}",
    )
    raw_task_dict = raw_task_list.get_task_dict()

    stdraw_task_list = PeptoDatabricksTaskList(
        process="dps_stdraw",
        super_package="laser_stdraw_pepto",
        package="dps_stdraw_toner_pepto",
        pool=None,
        previous_task_ids=[raw_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_TONER_SUPPLY_DPP_STDRAW_{env_for_deployment.upper()}",
    )
    stdraw_task_dict = stdraw_task_list.get_task_dict()

    raw_task_dict["run_op"]     >> stdraw_task_dict["branch_op"]
    raw_task_dict["skipped_op"] >> stdraw_task_dict["branch_op"]