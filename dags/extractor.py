from airflow.decorators import dag 
from pendulum import datetime, duration
from airflow.operators.python import PythonOperator 
from include.datasets import DATASET_COCKTAIL, DATASET_MOCKTAIL
from include.tasks import _get_cocktail, _get_mocktail, _check_size
from include.extractor.callbacks import _handle_failed_dagrun, _handle_check_size


 # Run this to check local and UI  : astro dev run dags test dag_id date_range

@dag(
    start_date=datetime(2025, 3, 3),
    schedule='@daily',
    catchup=False,
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
    
    # get_mocktail = PythonOperator(
    #     task_id="get_mocktail",
    #     python_callable=_get_mocktail,
    #     outlets=[DATASET_MOCKTAIL],
    # )
    
    check_size = PythonOperator(
        task_id="check_size",
        python_callable=_check_size,
        on_failure_callback= _handle_check_size
    )
    
    get_cocktail >> check_size

extractor()