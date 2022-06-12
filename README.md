## Spark Batch Deployment

This is an example of a big data pyspark pipeline deployment.

1. Build the Docker Compose

        docker-compose build

2. Run Docker Compose

        docker-compose up
        
3. Open Airflow at localhost:8080, go to Admin/Connections and set a new connection:
    - conn_id: 's3_default' 
    - S3 type connection
    - Login : your-access-key
    - Password : your-secret-key
    - Extra: {"region_name":"us-east-1"}

4. Assuming you've spun up your EMR cluster, go to 'titanic_training_emr' DAG, switch on and run    
5. Then, go to 'titanic_prediction_emr' DAG, switch on and run
    
6. Stop your Docker Compose and free resources

        docker-compose down