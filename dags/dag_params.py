from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import logging

API_URL = Variable.get(
    "SD_API_URL", default_var="https://api.sportsdynamics.eu/graphql"
)
API_KEY = Variable.get("SD_API_KEY")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def _get_game_metadata(game_id: str) -> dict:
    logger.info(f"Fetching metadata for game ID: {game_id}")
    headers = {"x-sd-api-key": API_KEY, "Content-Type": "application/json"}
    query = """query game($id: ID!) {
        game(id: $id) {
            id
            customer {
                id
                name
            }
            jobs {
                currentState
                currentInputStore {
                    assetStore
                    commandArgs
                }
                currentOutputStore {
                    assetStore
                    taskArn
                    taskMetadata
                }
            }
        }
    }"""
    payload = {"query": query, "variables": {"id": game_id}}
    logger.debug(f"Payload: {payload}")
    logger.debug(f"Headers: {headers}")

    try:
        response = requests.request(
            "POST",
            API_URL,
            headers=headers,
            json=payload,  # Fixed missing variable
        )
        response.raise_for_status()
        logger.info(f"Successfully fetched metadata for game ID: {game_id}")
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching metadata for game ID: {game_id} : {e}")
        raise


def _print_results(game_id: str, customer_id: str, command_args: dict):
    logger.info("Printing results...")
    print(f"Game ID: {game_id}")
    print(f"Customer ID: {customer_id}")
    print(f"Command Args: {command_args}")


@dag(
    default_args={"owner": "airflow"},
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["example"],
    params={"game_id": "bab6ec2f-db53-4f7b-9031-e7d49f4738da"},
)
def dag_params():
    get_game_metadata = PythonOperator(
        task_id="get_game_metadata",
        python_callable=_get_game_metadata,
        op_args=["{{params.game_id}}"],
    )

    display_params = PythonOperator(
        task_id="display_param",
        python_callable=_print_results,
        op_args=[
            "{{params.game_id}}",
            "{{ ti.xcom_pull(task_ids='get_game_metadata')['data']['game']['customer']['id'] }}",
            "{{ ti.xcom_pull(task_ids='get_game_metadata')['data']['game']['jobs'][0]['currentInputStore']['commandArgs'] }}",
        ],
    )

    get_game_metadata >> display_params


dag_params()
