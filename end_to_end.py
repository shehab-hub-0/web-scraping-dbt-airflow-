from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from load_data import loader
import yagmail

# Email config 
sender_email = "shahbahmed56p@gmail.com"
sender_password = "maoy ctqd phsl taao"

recipient_email = ["shahbahmed56p@gmail.com"]

def send_email_notification():
    try:
        yag = yagmail.SMTP(user=sender_email, password=sender_password)
        subject = "✅ Data Pipeline Completed Successfully!"
        body = f"""
<h2>📊 Data Pipeline Report</h2>
<p><strong>Status:</strong> Success</p>
<p><strong>DAG:</strong> data_pipeline</p>
<p><strong>Run Time:</strong> {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}</p>
<p><strong>Description:</strong></p>
<ul>
<li>Data loaded from API into DuckDB</li>
<li>Transformations applied via SQL models</li>
<li>Final dataset ready in <code>aggregated_sales</code></li>
</ul>
<p><em>This message was sent automatically by Airflow.</em></p>
        """
        yag.send(to=recipient_email, subject=subject, contents=body)
        print(f"✅ Email sent successfully to: {', '.join(recipient_email)}")
    except Exception as e:
        print(f"❌ Failed to send email: {str(e)}")
        raise e

with DAG(
    dag_id="data_pipeline",
    description="Load data from API to DuckDB, transform with dbt, then send email notification",
    schedule=None,
    catchup=False,
    tags=["data_pipeline_with_email"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    start_date=datetime(2023, 1, 1)
) as dag:

    t1 = PythonOperator(
        task_id="load_data_from_api",
        python_callable=loader
    )

    t2 = BashOperator(
        task_id="run_dbt_models",
        cwd="/opt/airflow/dags/Amit_pr/amit_dbt_project",
        bash_command="dbt run --profiles-dir /opt/airflow/dags/Amit_pr/.dbt"
    )

    t3 = PythonOperator(
        task_id="send_email",
        python_callable=send_email_notification
    )


    
    t1 >> t2 >> t3
