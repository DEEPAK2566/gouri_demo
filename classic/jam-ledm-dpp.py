import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email
from cumulus_libs.peptobase import PeptoDAG, PeptoDatabricksTaskList

namespace = Variable.get("namespace")
cumulus_vars = Variable.get("cumulus-common-variables", deserialize_json=True)
source_vars  = Variable.get("cumulus-jam_ledm_dpp", deserialize_json=True)

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
    dag_id="jam_ledm_dpp",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=send_failure_email,
    on_success_callback=send_success_email,
)

with dag:
    raw_task_list = PeptoDatabricksTaskList(
        process="dps_raw",
        super_package="raw_pepto",
        package="dps_raw_jam_pepto",
        pool=None,
        previous_task_ids=None,
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_JAM_LEDM_DPP_RAW_{env_for_deployment.upper()}",
    )
    raw_task_dict = raw_task_list.get_task_dict()

    stdraw_task_list = PeptoDatabricksTaskList(
        process="dps_stdraw",
        super_package="laser_stdraw_pepto",
        package="dps_stdraw_laser_pepto",
        pool=None,
        previous_task_ids=[raw_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_JAM_LEDM_DPP_STDRAW_{env_for_deployment.upper()}",
    )
    stdraw_task_dict = stdraw_task_list.get_task_dict()

    rs_task_list = PeptoDatabricksTaskList(
        process="dps_redshiftloader",
        super_package=None,
        package=None,
        pool=None,
        previous_task_ids=[stdraw_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_JAM_LEDM_DPP_REDSHIFT_{env_for_deployment.upper()}",
    )
    rs_task_dict = rs_task_list.get_task_dict()

    ocv_task_list = PeptoDatabricksTaskList(
        process="dps_ocv",
        super_package="ocv_pepto",
        package="dps_ocv_pepto",
        pool="ocv_pool",
        previous_task_ids=[stdraw_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_JAM_LEDM_DPP_OCV_{env_for_deployment.upper()}",
    )
    ocv_task_dict = ocv_task_list.get_task_dict()

    raw_task_dict["run_op"]     >> stdraw_task_dict["branch_op"]
    raw_task_dict["skipped_op"] >> stdraw_task_dict["branch_op"]
    stdraw_task_dict["run_op"]     >> rs_task_dict["branch_op"]
    stdraw_task_dict["skipped_op"] >> rs_task_dict["branch_op"]
    stdraw_task_dict["run_op"]     >> ocv_task_dict["branch_op"]
    stdraw_task_dict["skipped_op"] >> ocv_task_dict["branch_op"]