import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from datetime import datetime, timedelta
import pandas as pd
from sklearn import datasets

from big_data.titanic_prediction_emr.utils.functions import build_spark_submit

year_month_day_hour_minute_second = datetime.utcnow().strftime("%Y%d%m_%H%M%S")

with DAG(
        dag_id="titanic_prediction_emr",
        # schedule_interval="@daily",
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

    step_prediction = EmrAddStepsOperator(
        task_id="titanic_prediction",
        job_flow_id='j-1KYXO6IJYDRMN',
        aws_conn_id='s3_default',
        steps=build_spark_submit(pipeline_type="prediction",
                                 model_name="{{ task_instance.xcom_pull(dag_id='titanic_training_emr', "
                                            "task_ids='push_model', key='current_titanic_model_name', "
                                            "include_prior_dates=True)}}"
                                 ),
        dag=dag,
    )

    checker_prediction = EmrStepSensor(
        task_id="checker_prediction",
        job_flow_id='j-1KYXO6IJYDRMN',
        aws_conn_id='s3_default',
        step_id="{{ task_instance.xcom_pull(task_ids='titanic_prediction', key='return_value')["
                + str(0)
                + "] }}",
        dag=dag,
    )

    join >> step_prediction >> checker_prediction
