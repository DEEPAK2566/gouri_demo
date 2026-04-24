"""
Pepto base classes for Cumulus DAGs.
Library file — not a DAG definition.
"""
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


class PeptoDAG(DAG):
    """Custom DAG subclass with HP-specific defaults."""
    pass


class PeptoDatabricksTaskList:
    """Wraps a Databricks job as a set of Airflow tasks."""

    def __init__(self, process, super_package, package, pool,
                 previous_task_ids, DATABRICKS_DEPLOYMENT_NAME):
        self.process = process
        self.super_package = super_package
        self.package = package
        self.pool = pool
        self.previous_task_ids = previous_task_ids
        self.deployment_name = DATABRICKS_DEPLOYMENT_NAME

    def get_task_dict(self):
        return {
            "run_op": DatabricksRunNowOperator(
                task_id=f"{self.process}_run",
                databricks_conn_id="databricks_default",
                job_name=self.deployment_name,
            ),
            "skipped_op": DatabricksRunNowOperator(
                task_id=f"{self.process}_skipped",
                databricks_conn_id="databricks_default",
                job_name=self.deployment_name,
            ),
            "branch_op": DatabricksRunNowOperator(
                task_id=f"{self.process}_branch",
                databricks_conn_id="databricks_default",
                job_name=self.deployment_name,
            ),
        }