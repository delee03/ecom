from include.datasets import DATASET_COCKTAIL, DATASET_MOCKTAIL
from airflow.exceptions import AirflowException
import logging
import json

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
    if size <= 0:
        raise AirflowException()
        
def _validate_cocktail_data():
    """
    Validates if the JSON data stored at DATASET_COCKTAIL has the required fields.
    Raises an AirflowException if validation fails.
    """
    try:
        with open(DATASET_COCKTAIL.uri, 'r') as f:
            data = json.load(f)
            
        # Check if the data has the 'drinks' key and it's a list
        if 'drinks' not in data or not isinstance(data['drinks'], list) or len(data['drinks']) == 0:
            raise AirflowException("JSON data does not have the required 'drinks' array or it's empty")
            
        # Get the first drink
        drink = data['drinks'][0]
        
        # Required fields to check
        required_fields = [
            'idDrink', 'strDrink', 'strDrinkAlternate', 'strTags', 'strVideo',
            'strCategory', 'strIBA', 'strAlcoholic', 'strGlass', 'strInstructions',
            'strInstructionsES', 'strInstructionsDE', 'strInstructionsFR', 'strInstructionsIT',
            'strInstructionsZH-HANS', 'strInstructionsZH-HANT', 'strDrinkThumb'
        ]
        
        # Check ingredients and measures (1-15)
        for i in range(1, 16):
            required_fields.append(f'strIngredient{i}')
            required_fields.append(f'strMeasure{i}')
            
        # Additional fields
        additional_fields = [
            'strImageSource', 'strImageAttribution', 'strCreativeCommonsConfirmed', 'dateModified'
        ]
        required_fields.extend(additional_fields)
        
        # Check if all required fields exist
        missing_fields = [field for field in required_fields if field not in drink]
        
        if missing_fields:
            raise AirflowException(f"Missing required fields in cocktail data: {', '.join(missing_fields)}")
            
        logger.info("Cocktail data validation successful - all required fields are present")
        return True
        
    except json.JSONDecodeError:
        raise AirflowException("Invalid JSON data in cocktail dataset")
    except Exception as e:
        raise AirflowException(f"Error validating cocktail data: {str(e)}")
