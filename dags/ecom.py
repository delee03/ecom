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
from pendulum import datetime,timedelta

@dag(
    start_date=datetime(2025,3,31),
    schedule='@daily',
    catchup=False,
    description="This DAG processes ecommerce data pipline",
    tags=(["team_a", "ecom", "pii"]),
    default_args={"retries": 1},
    dagrun_timeout=timedelta(minutes=20),
    max_consecutive_failed_dag_runs=2,
    # max_active_runs=1,
)
def ecom():
    ta = PythonOperator(
        task_id='ta',
    )

    tb = PythonOperator(    
        task_id='tb',
    )

ecom()

#Backfill => Manually run DAG in specific day in previous, or debugging
#Catchup => Airflow run automatically for date interval at midnight