from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator


from os.path import expanduser
from pathlib import Path
home = expanduser("~")
airflow_dir = os.path.join(home, 'airflow')
Path(f"{airflow_dir}/dags").mkdir(parents=True, exist_ok=True)



default_args = {
    'owner': 'Ivan',
    'depends_on_past': False,
    'email': ['ivan@theaicore.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}



with DAG    (dag_id='0e2a0bfcc15_dag',  
            start_date=datetime(2023, 2, 2),
            # check out possible intervals, should be a string
            schedule_interval='@daily',
            catchup=False,
            default_args=default_args
            ) as dag:

        opr_submit_run = DatabricksSubmitRunOperator    (
                                                        task_id='submit_run',
                                                        # the connection we set-up previously
                                                        databricks_conn_id='databricks_default',
                                                        # which cluster do we use? spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
                                                        existing_cluster_id='CLUSTER_NAME',
                                                        notebook_task=notebook_task
                                                        )

        opr_submit_run  


with DAG(dag_id='0e2a0bfcc15_dag',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False,
         tags=['test']
         ) as dag:

    # Define the tasks. Here we are going to define only one bash operator
    test_task = BashOperator(
        task_id='write_date_file',
        bash_command='cd ~/Desktop && date >> ai_core.txt',
        dag=dag)

