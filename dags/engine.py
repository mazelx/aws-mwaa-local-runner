from airflow.decorators import dag
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable, DagRun
import requests
import logging


# LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# VARIABLES
default_args = {
    'owner': 'xavier',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['xavier@sportsdynamics.eu'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ENGINE_WORKING_DIR = '/home/ubuntu/sportsdynamics/app/engine/src'
ENGINE_INPUT_DATA_DIR = '/home/ubuntu/sportsdynamics/app/engine/input_data'
ACTIVATE_ENV_CMD  = f"cd {ENGINE_WORKING_DIR}/ECDO  && source ~/anaconda3/etc/profile.d/conda.sh && conda activate SD"

API_URL = Variable.get(
    "SD_API_URL", default_var="https://api.sportsdynamics.eu/graphql"
)
API_KEY = Variable.get("SD_API_KEY")
S3_BUCKET = Variable.get("S3_BUCKET", default_var="assets-20200820085407743800000001")


# FUNCTIONS
def _parse_game_metadata(raw_game_metadata: dict) -> dict:
    game_metadata = {}
    game_metadata['game_id'] = raw_game_metadata['data']['game']['id']
    game_metadata['game_name'] = raw_game_metadata['data']['game']['localClub']['name'] + raw_game_metadata['data']['game']['remoteClub']['name']
    game_metadata['customer_id'] = raw_game_metadata['data']['game']['customer']['id']
    game_metadata['customer_name'] = "SportsDynamics" # TODO : understand how to get the customer name (local mapping)
    game_metadata['commandArgs'] = " ".join(raw_game_metadata['data']['game']['jobs'][0]['currentInputStore']['commandArgs'])
    return game_metadata

def _get_s3_path(command_args:dict) -> dict:
    args_dict = {}
    for i, elem in enumerate(command_args):
        if elem.startswith('--'):
            args_dict[elem[2:]] = command_args[i + 1]
    return args_dict


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
            localClub { name }
            remoteClub { name }
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


# DAG DEFINITION
@dag(
    dag_id='dag_engine',
    default_args=default_args,
    description='Run Engine pipeline on EC2 instance',
    schedule_interval=None,  # ou None si lancement manuel
    start_date=days_ago(1),
    catchup=False, 
    tags=['pipeline', 'ec2', 'ssh'],
    params={
        "game_id": "bab6ec2f-db53-4f7b-9031-e7d49f4738da"
    },
    render_template_as_native_obj=True, # to get dict instead of string in xcom_pull
    )
def dag_engine():

    get_game_metadata = PythonOperator(
        task_id="get_game_metadata",
        python_callable=_get_game_metadata,
        op_args=["{{params.game_id}}"],
    )

    parse_game_metadata = PythonOperator(
        task_id="parse_game_metadata",
        python_callable=_parse_game_metadata,
        op_args=["{{ task_instance.xcom_pull(task_ids='get_game_metadata') }}"],
    )

    download_s3_input_files = SSHOperator(
        task_id='ssh_download_s3_input_files',
        ssh_conn_id='ec2_ssh_conn',
        # TODO : get the folder (SaintEtienneLyon for bab6ec2f-db53-4f7b-9031-e7d49f4738da for example)
        command=ACTIVATE_ENV_CMD  + 
            " && AWS_PROFILE=sd-production aws s3 cp s3://" + S3_BUCKET + "/{{ti.xcom_pull(task_ids='parse_game_metadata')['customer_id']}}/{{ti.xcom_pull(task_ids='parse_game_metadata')['game_id']}}/input " + 
            ENGINE_INPUT_DATA_DIR + "/{{ti.xcom_pull(task_ids='parse_game_metadata')['customer_name']}}/{{ti.xcom_pull(task_ids='parse_game_metadata')['game_name'] }} --recursive",
        conn_timeout=600,
        cmd_timeout=600,
    )

    run_pipeline = SSHOperator(
        task_id='ssh_run_pipeline',
        ssh_conn_id='ec2_ssh_conn',
        command = "mkdir -p " + ENGINE_INPUT_DATA_DIR + "/{{ti.xcom_pull(task_ids='parse_game_metadata')['customer_name']}}/{{ti.xcom_pull(task_ids='parse_game_metadata')['game_name'] }}"
        + " &&" + ACTIVATE_ENV_CMD + 
        " && python launcher.py  {{ti.xcom_pull(task_ids='parse_game_metadata')['commandArgs']}}",
        conn_timeout=3600,
        cmd_timeout=3600,
    )

    get_game_metadata >> parse_game_metadata >>  download_s3_input_files >> run_pipeline


# Run the DAG
dag_engine()