# Way 1 : It's the bad way

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# dag = DAG(...)

# ta = PythonOperator(
#     task_id='ta',
#     dag=dag
# )

# tb = PythonOperator(
#     task_id='tb',
#     dag=dag
# )

# # Way 2 : It's much better using with context

# from airflow import DAG
# from airflow.operators.python import PythonOperator

# with DAG(...): # add the colon

#     ta = PythonOperator(
#         task_id='ta',
#     )

#     tb = PythonOperator(
#         task_id='tb',
#     )
    
# Way 3: Define using Dag Decorator

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime,duration
from include.datasets import DATASET_COCKTAIL, DATASET_MOCKTAIL



def _process_data():
    print("Processing data from datasets...")



@dag(
    start_date=datetime(2025, 3, 15),
    #schedule=[DATASET_COCKTAIL, DATASET_MOCKTAIL], #And conditional schedule
    schedule=(DATASET_COCKTAIL | DATASET_MOCKTAIL), #OR conditional schedule
    catchup=False,
    description="This DAG processes ecommerce data pipline",
    tags=(["team_a", "ecom", "pii"]),
    default_args={"retries": 2},
    dagrun_timeout=duration(minutes=20),
    max_consecutive_failed_dag_runs=2,
    # max_active_runs=1,
)
def ecom():
    ta = EmptyOperator(
        task_id='ta',
    )   
    

    tb = PythonOperator(
        task_id='tb',
        python_callable=_process_data,
    )

    ta >> tb

ecom()

#Backfill => Manually run DAG in specific day in previous, or debugging
#Catchup => Airflow run automatically for date interval at midnight