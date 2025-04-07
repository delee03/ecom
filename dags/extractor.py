from airflow.decorators import dag 
from pendulum import datetime
from airflow.operators.python import PythonOperator 
from airflow.datasets import Dataset
import logging

logger = logging.getLogger(__name__)

DATASET_COCKTAIL = Dataset('/tmp/cocktail.json')
DATASET_MOCKTAIL = Dataset('/tmp/mocktail.json')

def _get_cocktail():
    import requests
    api = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    response = requests.get(api)
    with open(DATASET_COCKTAIL.uri, 'wb') as f:
        f.write(response.content)
    logger.info(f"Successfully updated {DATASET_COCKTAIL.uri}")

def _get_mocktail():
    import requests
    api = "https://www.thecocktaildb.com/api/json/v1/1/filter.php?a=Non_Alcoholic"
    response = requests.get(api)
    with open(DATASET_MOCKTAIL.uri, 'wb') as f:
        f.write(response.content)
    logger.info(f"Updated {DATASET_MOCKTAIL.uri} successfully")
 

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
    
    get_mocktail = PythonOperator(
        task_id="get_mocktail",
        python_callable=_get_mocktail,
        outlets=[DATASET_MOCKTAIL],
    )

extractor()