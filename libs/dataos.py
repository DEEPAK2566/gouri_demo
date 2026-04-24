"""
Shared Airflow utility functions.
This is a library file, not a DAG — analyzer should flag HUMAN_REVIEW.
"""

def send_failure_email(context):
    """Send alert email on DAG failure."""
    dag_id = context.get("dag").dag_id
    print(f"[ALERT] DAG {dag_id} failed. Sending email...")

def send_success_email(context):
    """Send confirmation email on DAG success."""
    dag_id = context.get("dag").dag_id
    print(f"[OK] DAG {dag_id} succeeded. Sending email...")