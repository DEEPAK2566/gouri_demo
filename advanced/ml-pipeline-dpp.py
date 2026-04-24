import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email
from cumulus_libs.peptobase import PeptoDAG, PeptoDatabricksTaskList

namespace   = Variable.get("namespace")
source_vars = Variable.get("cumulus-ml_pipeline_dpp", deserialize_json=True)

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
    dag_id="ml_pipeline_dpp",
    schedule_interval="0 2 * * 1",
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=send_failure_email,
    on_success_callback=send_success_email,
)

with dag:
    feature_task_list = PeptoDatabricksTaskList(
        process="dps_feature_engineering",
        super_package="ml_pepto",
        package="dps_feature_ml_pepto",
        pool="ml_pool",
        previous_task_ids=None,
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_ML_PIPELINE_FEATURES_{env_for_deployment.upper()}",
    )
    feature_task_dict = feature_task_list.get_task_dict()

    train_task_list = PeptoDatabricksTaskList(
        process="dps_model_training",
        super_package="ml_pepto",
        package="dps_train_ml_pepto",
        pool="ml_pool",
        previous_task_ids=[feature_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_ML_PIPELINE_TRAIN_{env_for_deployment.upper()}",
    )
    train_task_dict = train_task_list.get_task_dict()

    score_task_list = PeptoDatabricksTaskList(
        process="dps_model_scoring",
        super_package="ml_pepto",
        package="dps_score_ml_pepto",
        pool="ml_pool",
        previous_task_ids=[train_task_dict["run_op"].task_id],
        DATABRICKS_DEPLOYMENT_NAME=f"CUMULUS_ML_PIPELINE_SCORE_{env_for_deployment.upper()}",
    )
    score_task_dict = score_task_list.get_task_dict()

    feature_task_dict["run_op"]     >> train_task_dict["branch_op"]
    feature_task_dict["skipped_op"] >> train_task_dict["branch_op"]
    train_task_dict["run_op"]       >> score_task_dict["branch_op"]
    train_task_dict["skipped_op"]   >> score_task_dict["branch_op"]