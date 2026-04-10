from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.task.trigger_rule import TriggerRule

default_args = {
    "owner": "dylan",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

AWS_CONN_ID = "aws_default"
SLACK_CONN_ID = "slack_webhook_default"

S3_BUCKET = Variable.get("S3_BUCKET", default_var="dylan-ecommerce-data-pipeline")

EMR_APPLICATION_ID = Variable.get(
    "EMR_SERVERLESS_APPLICATION_ID",
    default_var="00g4avib4vajvb29",
)

EMR_EXECUTION_ROLE_ARN = Variable.get(
    "EMR_EXECUTION_ROLE_ARN",
    default_var="",
)

EMR_LOG_URI = Variable.get(
    "EMR_LOG_URI",
    default_var=f"s3://{S3_BUCKET}/logs/emr/",
)

SPARK_SCRIPT_URI = Variable.get(
    "SPARK_SCRIPT_URI",
    default_var=f"s3://{S3_BUCKET}/scripts/bronze_to_silver.py",
)

SPARK_LOCAL_SCRIPT = Variable.get(
    "SPARK_LOCAL_SCRIPT",
    default_var="scripts/spark/bronze_to_silver.py",
)

PROJECT_DIR = Variable.get("PROJECT_DIR", default_var="/opt/airflow/project")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", default_var="/opt/airflow/project")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", default_var="/opt/airflow/.dbt")

with DAG(
    dag_id="ecommerce_end_to_end_pipeline",
    description="S3 -> EMR Serverless -> Redshift -> dbt -> Slack",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "aws", "emr-serverless", "redshift", "dbt"],
) as dag:

    upload_raw_csvs_to_s3 = BashOperator(
        task_id="upload_raw_csvs_to_s3",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        python3 scripts/ingestion/upload_to_s3.py
        """,
    )

    upload_spark_script_to_s3 = BashOperator(
        task_id="upload_spark_script_to_s3",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        python3 scripts/ingestion/upload_script_to_s3.py
        """,
    )

    run_spark_bronze_to_silver_emr = EmrServerlessStartJobOperator(
        task_id="run_spark_bronze_to_silver_emr",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": SPARK_SCRIPT_URI,
                "entryPointArguments": [],
                "sparkSubmitParameters": (
                    "--conf spark.executor.memory=4g "
                    "--conf spark.driver.memory=2g "
                    "--conf spark.executor.cores=2 "
                    "--conf spark.driver.cores=1"
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": EMR_LOG_URI
                }
            }
        },
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
    )

    create_redshift_schemas_and_tables = BashOperator(
        task_id="create_redshift_schemas_and_tables",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        python3 scripts/loaders/run_redshift_sql.py --sql-file sql/schemas_and_tables_redshift.sql
        """,
    )

    load_silver_to_redshift = BashOperator(
        task_id="load_silver_to_redshift",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        python3 scripts/loaders/run_redshift_sql.py --sql-file sql/load_parquet_redshift.sql
        """,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt snapshot --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message="""
✅ Ecommerce pipeline succeeded.

Completed:
• Raw CSV ingestion to S3
• Spark script upload to S3
• Spark bronze-to-silver on EMR Serverless
• Redshift schema/table creation
• Silver-to-Redshift load
• dbt snapshot
• dbt run
• dbt test
        """,
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message="""
❌ Ecommerce pipeline failed.

Check:
• Airflow task logs
• EMR Serverless logs in S3
• Redshift SQL execution
• dbt logs
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    (
        upload_raw_csvs_to_s3
        >> upload_spark_script_to_s3
        >> run_spark_bronze_to_silver_emr
        >> create_redshift_schemas_and_tables
        >> load_silver_to_redshift
        >> dbt_snapshot
        >> dbt_run
        >> dbt_test
        >> notify_success
    )

    [
        upload_raw_csvs_to_s3,
        upload_spark_script_to_s3,
        run_spark_bronze_to_silver_emr,
        create_redshift_schemas_and_tables,
        load_silver_to_redshift,
        dbt_snapshot,
        dbt_run,
        dbt_test,
    ] >> notify_failure
