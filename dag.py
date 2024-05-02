from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                'owner': 'damirkalin',
                'start_date':datetime(2024, 4, 16),
                }

dag_spark = DAG(
                dag_id = "spark_test",
                default_args=default_args,
                schedule_interval=None,
                )

# объявляем задачу с помощью SparkSubmitOperator
project_sp7_users = SparkSubmitOperator(
        task_id='project_sp7_users',
        dag=dag_spark,
        application ='/lessons/dags/project_sp7_users.py',
        conn_id= 'yarn_spark',
        application_args = ["/user/damirkalin/data/geo/events/event_type=message", "2022-06-21", "28", "/user/damirkalin/data/geo/geo.csv", "/user/damirkalin/data/geo/timezone.csv", "/user/damirkalin/analytics/project_sp7_users_d28"],
        conf={"spark.driver.maxResultSize": "20g"},
        executor_cores = 1,
        executor_memory = '1g'
        )

project_sp7_zone = SparkSubmitOperator(
        task_id='project_sp7_zone',
        dag=dag_spark,
        application ='/lessons/dags/project_sp7_zone.py',
        conn_id= 'yarn_spark',
        application_args = ["/user/damirkalin/data/geo/events", "2022-02-28", "7", "/user/damirkalin/data/geo/geo.csv", "/user/damirkalin/analytics/project_sp7_zone_d28"],
        conf={"spark.driver.maxResultSize": "20g"},
        executor_cores = 1,
        executor_memory = '1g'
        )

project_sp7_recomendation = SparkSubmitOperator(
        task_id='project_sp7_recomendation',
        dag=dag_spark,
        application ='/lessons/dags/project_sp7_recomendation.py',
        conn_id= 'yarn_spark',
        application_args = ["/user/damirkalin/data/geo/events", "2022-02-28", "30", "/user/damirkalin/data/geo/geo.csv", "/user/damirkalin/data/geo/timezone.csv", "/user/damirkalin/analytics/project_sp7_recomendation_d30"],
        conf={"spark.driver.maxResultSize": "20g"},
        executor_cores = 1,
        executor_memory = '1g'
        )



project_sp7_users >> project_sp7_zone >> project_sp7_recomendation