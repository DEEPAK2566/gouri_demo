import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email
from cumulus_libs.peptobase import PeptoDAG, PeptoDatabricksTaskList

namespace   = Variable.get("namespace")
source_vars = Variable.get("cumulus-reporting_dpp", deserialize_json=True)

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
    dag_id="reporting_dpp",
    schedule_interval="0 12 * * *",
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=send_failure_email,
)

with dag:
    agg_task_list = PeptoDatabricksTaskList(
        process="dps_aggregation",
        super_package="reporting_pepto",
        package="dps_agg_reporting_pepto",
        pool=None,
        previous_task_ids=None,
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_REPORTING_AGG_{env_for_deployment.upper()}",
    )
    agg_task_dict = agg_task_list.get_task_dict()

    export_task_list = PeptoDatabricksTaskList(
        process="dps_export",
        super_package="reporting_pepto",
        package="dps_export_reporting_pepto",
        pool=None,
        previous_task_ids=[agg_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_REPORTING_EXPORT_{env_for_deployment.upper()}",
    )
    export_task_dict = export_task_list.get_task_dict()

    agg_task_dict["run_op"]     >> export_task_dict["branch_op"]
    agg_task_dict["skipped_op"] >> export_task_dict["branch_op"]