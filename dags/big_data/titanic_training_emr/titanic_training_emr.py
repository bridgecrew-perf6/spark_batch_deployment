from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from big_data.titanic_training_emr.utils.functions import build_spark_submit, push_model_name

year_month_day_hour_minute_second = datetime.utcnow().strftime("%Y%d%m_%H%M%S")

with DAG(
        dag_id="titanic_training_emr",
        # schedule_interval="@monthly",
        schedule_interval="@once",
        default_args={
            "owner": "cesar_charalla_olazo",
            "retries": 1,
            # "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False,
) as dag:
    join = DummyOperator(
        task_id='join',
        trigger_rule='all_success'
    )

    push_model = PythonOperator(
        task_id="push_model",
        python_callable=push_model_name,
        op_kwargs={'model_name': year_month_day_hour_minute_second},
        provide_context=True,
        dag=dag
    )

    step_training = EmrAddStepsOperator(
        task_id="titanic_training",
        job_flow_id='j-127WDRGOEZ63V',
        aws_conn_id='s3_default',
        steps=build_spark_submit(pipeline_type="training",
                                 model_name="{{ task_instance.xcom_pull(key='current_titanic_model_name') }}"),
        dag=dag,
    )

    checker_training = EmrStepSensor(
        task_id="checker_titanic_training",
        job_flow_id='j-127WDRGOEZ63V',
        aws_conn_id='s3_default',
        step_id="{{ task_instance.xcom_pull(task_ids='titanic_training', key='return_value')["
                + str(0)
                + "] }}",
        dag=dag,
    )

    join >> push_model >> step_training >> checker_training
