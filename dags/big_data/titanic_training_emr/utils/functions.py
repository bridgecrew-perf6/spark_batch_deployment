def build_spark_submit(pipeline_type, model_name):
    spark_submit = [
        {
            "Name": f"titanic_app_step_{pipeline_type}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "client",
                    "--master", "yarn",
                    "--py-files", "s3://cesarcharalla-test2/spark_jobs/titanic_job/spark_artifacts/spark.zip",
                    "s3://cesarcharalla-test2/spark_jobs/titanic_job/spark_artifacts/run_modeling.py",
                    "--pipeline_type", pipeline_type,
                    "--model_name", model_name,
                    "--root_path", "s3://cesarcharalla-test2/spark_jobs/titanic_job/",
                    "--checkpoint_path", "s3://cesarcharalla-test2/spark_jobs/titanic_job/checkpoints/",
                    "--year_month", "202206",
                    "--country", "PE",
                    "--org", "lab",
                    "--app_env", "prod",
                    "--version", "1.0",
                ],
            },
        },
    ]

    return spark_submit


def push_model_name(model_name, **context):
    context["ti"].xcom_push(key="current_titanic_model_name", value=model_name)
