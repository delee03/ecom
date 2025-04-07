from airflow.decorators import dag 
from pendulum import datetime
from airflow.operators.python import PythonOperator 
from airflow.datasets import Dataset
from include.datasets import DATASET_COCKTAIL, DATASET_MOCKTAIL

import logging

logger = logging.getLogger(__name__)

def _get_cocktail(ti=None):
    import requests
    api = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    response = requests.get(api)
    with open(DATASET_COCKTAIL.uri, 'wb') as f:
        f.write(response.content)
    logger.info(f"Successfully updated {DATASET_COCKTAIL.uri}")
    ti.xcom_push(key='request_size', value=len(response.content))

def _get_mocktail():
    import requests
    api = "https://www.thecocktaildb.com/api/json/v1/1/filter.php?a=Non_Alcoholic"
    response = requests.get(api)     
    with open(DATASET_MOCKTAIL.uri, 'wb') as f:
        f.write(response.content)
    logger.info(f"Updated {DATASET_MOCKTAIL.uri} successfully")
 
def _check_size(ti = None):
    size = ti.xcom_pull(key='request_size', task_ids='get_cocktail')
    logger.info(f"Loggin _ Size of request is {size}")
    # Run this to check local and UI  : astro dev run dags test dag_id date_range

@dag(
    start_date=datetime(2025, 3, 3),
    schedule='@daily',
    catchup=False,
)
def extractor():
    
    get_cocktail = PythonOperator(
        task_id="get_cocktail",
        python_callable=_get_cocktail,
        outlets=[DATASET_COCKTAIL],
    )
    
    # get_mocktail = PythonOperator(
    #     task_id="get_mocktail",
    #     python_callable=_get_mocktail,
    #     outlets=[DATASET_MOCKTAIL],
    # )
    
    check_size = PythonOperator(
        task_id="check_size",
        python_callable=_check_size,
    )
    
    get_cocktail >> check_size

extractor()