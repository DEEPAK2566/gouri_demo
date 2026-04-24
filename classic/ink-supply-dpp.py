import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email
from cumulus_libs.peptobase import PeptoDAG, PeptoDatabricksTaskList

namespace = Variable.get("namespace")
source_vars = Variable.get("cumulus-ink_supply_dpp", deserialize_json=True)

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
    dag_id="ink_supply_dpp",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=send_failure_email,
)

with dag:
    raw_task_list = PeptoDatabricksTaskList(
        process="dps_raw",
        super_package="raw_pepto",
        package="dps_raw_ink_pepto",
        pool=None,
        previous_task_ids=None,
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_INK_SUPPLY_DPP_RAW_{env_for_deployment.upper()}",
    )
    raw_task_dict = raw_task_list.get_task_dict()

    stdraw_task_list = PeptoDatabricksTaskList(
        process="dps_stdraw",
        super_package="laser_stdraw_pepto",
        package="dps_stdraw_ink_pepto",
        pool=None,
        previous_task_ids=[raw_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_INK_SUPPLY_DPP_STDRAW_{env_for_deployment.upper()}",
    )
    stdraw_task_dict = stdraw_task_list.get_task_dict()

    lake_task_list = PeptoDatabricksTaskList(
        process="dps_stdraw_delta_lake",
        super_package="laser_stdraw_pepto",
        package="dps_stdraw_delta_lake",
        pool=None,
        previous_task_ids=[stdraw_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_INK_SUPPLY_DPP_DELTA_LAKE_{env_for_deployment.upper()}",
    )
    lake_task_dict = lake_task_list.get_task_dict()

    raw_task_dict["run_op"]      >> stdraw_task_dict["branch_op"]
    raw_task_dict["skipped_op"]  >> stdraw_task_dict["branch_op"]
    stdraw_task_dict["run_op"]   >> lake_task_dict["branch_op"]
    stdraw_task_dict["skipped_op"] >> lake_task_dict["branch_op"]