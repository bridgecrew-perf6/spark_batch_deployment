MODELING_STEP = [
    {
        "Name": "titanic_app_step",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "--master", "yarn",
                "--py-files", "s3://cesarcharalla-test2/spark_jobs/titanic_job/spark_artifacts/spark.zip",
                "s3://cesarcharalla-test2/spark_jobs/titanic_job/spark_artifacts/run_modeling.py",
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