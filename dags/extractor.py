from airflow.decorators import dag, task_group, task
from pendulum import datetime, duration
from airflow.operators.python import PythonOperator 
from include.datasets import DATASET_COCKTAIL, DATASET_MOCKTAIL
from include.tasks import _get_cocktail, _get_mocktail, _check_size, _validate_cocktail_data
from include.extractor.callbacks import _handle_failed_dagrun, _handle_check_size
import json
import json
import json
import json


 # Run this to check local and UI  : astro dev run dags test dag_id date_range

@dag(
    start_date=datetime(2025, 3, 3),
    schedule='@daily',
    catchup=False,
    tags=(["team_a", "extractor", "pii"]),
    default_args={
        "retries": 2, 
        "retry_delay": duration(seconds=2), 
    },
    on_failure_callback= _handle_failed_dagrun
)
def extractor():
    
    get_cocktail = PythonOperator(
        task_id="get_cocktail",
        python_callable=_get_cocktail,
        outlets=[DATASET_COCKTAIL],   
        retry_exponential_backoff = True, # allow progressive longer waits between retries first retries is 2s next 4s ... should using at API call or queries db
        max_retry_delay = duration(minutes=5),
    )
    #Can have using nested task_group
    @task_group(
        default_args={
            "retries": 2,
            "retry_delay": duration(seconds=2),
        },
    )
    def checks():
        check_size = PythonOperator(
            task_id="check_size",
            python_callable=_check_size,
            on_failure_callback= _handle_check_size
        )
    
        validate_cocktail_data = PythonOperator(
            task_id="validate_cocktail_data",
            python_callable=_validate_cocktail_data,
            # This task depends on the Dataset DATASET_COCKTAIL
            inlets=[DATASET_COCKTAIL],
        )
        
        check_size >> validate_cocktail_data
     
    @task.branch()
    def branch_cocktail_type():
        with open(DATASET_COCKTAIL.uri, 'r') as f:
            data = json.load(f)
            if data['drinks'][0]['strAlcoholic'] == 'Alcoholic':
                return "alcoholic_drink"
            return "non_alcoholic_drink"
    
    @task()
    def alcoholic_drink():
        print("Alcoholic drink")
    
    @task()
    def non_alcoholic_drink():
        print("Non-alcoholic drink")
    # get_mocktail = PythonOperator(
    #     task_id="get_mocktail",
    #     python_callable=_get_mocktail,
    #     outlets=[DATASET_MOCKTAIL],
    # )
    
    # def store_value(ti=None):
    #     ti.xcom_pull(key="abc", task_ids="checks.validate_cocktail_data") => syntax for using xcom pull from the task within task-group
    
  
    get_cocktail >> checks() >> branch_cocktail_type() >> [alcoholic_drink(), non_alcoholic_drink()]

extractor()
