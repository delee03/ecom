from include.datasets import DATASET_COCKTAIL, DATASET_MOCKTAIL
from airflow.exceptions import AirflowException
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
    raise AirflowException()