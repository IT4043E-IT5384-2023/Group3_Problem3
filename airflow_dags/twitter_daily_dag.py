from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
  "owner": 'airflow',
  "depends_on_past": False,
  "start_date": datetime(2023, 6, 12),
  "retries": 3,
  "retry_delay": timedelta(minutes=10),
}

with DAG(dag_id="twitter_daily_dag",
         default_args=default_args,
         catchup=False,
         schedule="0 0 * * *"
         ) as dag:
    
    start_task = EmptyOperator(task_id="twitter_daily_dag_start")

    producer_1_task = BashOperator(task_id="producer_1_task",
                                 bash_command="cd ~/Documents/IT4043E_Group3_Problem3 && python3 kafka_jobs/twitter_producer.py --producer-id 1",
                                 retries=2,
                                 max_active_tis_per_dag=1)

    producer_2_task = BashOperator(task_id="producer_2_task",
                                 bash_command="cd ~/Documents/IT4043E_Group3_Problem3 && python3 kafka_jobs/twitter_producer.py --producer-id 2",
                                 retries=2,
                                 max_active_tis_per_dag=1)
    
    producer_3_task = BashOperator(task_id="producer_3_task",
                                 bash_command="cd ~/Documents/IT4043E_Group3_Problem3 && python3 kafka_jobs/twitter_producer.py --producer-id 3",
                                 retries=2,
                                 max_active_tis_per_dag=1)
    
    elasticsearch_consumer_task = BashOperator(task_id="elasticsearch_consumer_task",
                                              bash_command="cd ~/Documents/IT4043E_Group3_Problem3 && python3 kafka_jobs/consumer/elasticsearch_consumer.py",
                                              retries=2,
                                              max_active_tis_per_dag=1)

    gcs_consumer_task = BashOperator(task_id="gcs_consumer_task",
                                 bash_command="cd ~/Documents/IT4043E_Group3_Problem3 && python3 kafka_jobs/consumer/gcs_consumer.py",
                                 retries=2,
                                 max_active_tis_per_dag=1)

    end_task = EmptyOperator(task_id="twitter_daily_dag_done")
    
    start_task >> (producer_1_task, producer_2_task, producer_3_task) >> elasticsearch_consumer_task >> gcs_consumer_task >> end_task
