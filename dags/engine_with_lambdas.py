import logging
import json
from datetime import timedelta

from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from graphql_http_operator import GraphQLHttpOperator

# AWS ECS CONTEXT
class ECSRunContext:
    CLUSTER = "ecs-cluster-for-airflow-dev"
    TASK_DEFINITION = "task-for-airflow-dev"
    LAUNCH_TYPE = "FARGATE"
    PLATFORM_VERSION = "LATEST"
    NETWORK_CONFIGURATION = {
        "awsvpcConfiguration": {
            "subnets": ["subnet-d523538f", "subnet-3b616773", "subnet-e901368f"],
            "securityGroups": ["sg-8a6114c8"],
            "assignPublicIp": "ENABLED",
        },
    }
    AWSLOGS_GROUP = "/ecs/task-for-airflow-dev"
    AWSLOGS_STREAM_PREFIX = "ecs/task-container-for-airflow-dev"
    AWSLOGS_REGION = "eu-west-1"


# LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# VARIABLES
default_args = {
    "owner": "xavier",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": ["xavier@sportsdynamics.eu"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# DAG DEFINITION
@dag(
    dag_id="engine_with_lambdas",
    default_args=default_args,
    description="Run Engine pipeline on EC2 instance",
    schedule_interval=None,  # ou None si lancement manuel
    start_date=days_ago(1),
    catchup=False,
    tags=["pipeline", "ec2", "ssh"],
    params={"game_id": "8186f8e5-a83e-4bc9-8449-5007172fc198"},
    render_template_as_native_obj=True,  # to get dict instead of string in xcom_pull
)
def engine_with_lambdas():
    """
    DAG :
    """
    # TASKS

    get_game_info = GraphQLHttpOperator(
            task_id="get_game_info",
            http_conn_id="sd_legacy_api_conn",
            endpoint="graphql",
            query="""query($id: ID!) {
                game(id: $id) {
                    id
                    customer {
                    id
                    name
                    }
                        lastJobRequest {
                    id
                    state
                    inputStore {
                        assetStore
                        commandArgs
                    }
                    outputStore {
                        assetStore
                        taskArn
                        taskMetadata
                    }
                    }
                }
            }""",
            variables = """{ "id": "{{ params.game_id}}" } """,
            keys_check={'data.game.id':str, 'data.game.customer.id': str},
            log_response=True,
        )

    wait_for_provider_input_files = HttpSensor(
        task_id="wait_for_provider_input_files",
        http_conn_id="sd_api_conn",
        endpoint="/provider_input_files",
        method="GET",
        request_params={},
        response_check=lambda response: response.status_code == 200,
        timeout=60 * 5,  # total timeout (10 minutes in prod)
        poke_interval=2,  # checks interval (15 minutes in prod)
        mode="poke",  # In prod, use reschedule mode to avoid blocking the scheduler
    )

    get_provider_input_files = PythonOperator(
        task_id="get_provider_input_files",
        python_callable=lambda: logger.info("get_provider_input_files..."),
    )

    set_customer_game_to_not_started = PythonOperator(
        task_id="set_customer_game_to_not_started",
        python_callable=lambda: logger.info("run set_customer_game_to_not_started..."),
    )


    create_job_request = GraphQLHttpOperator(
        task_id="create_job_request",
        http_conn_id="sd_api_conn",
        endpoint="graphql",
        query="""mutation ($data: JobRequestCreateInput!, $gameId: ID!) {
            createJobRequestByGameId(data: $data, gameId: $gameId) {
                id
            }
        }""",
        variables ="""
                "data": {
                    "inputStore": {
                        "assetStore": "{{params.game_id}}/{{ ti.xcom_pull(task_ids='get_game_info')['data']['game']['id']}}/{{ ti.xcom_pull(task_ids='get_game_info')['data']['game']['customer']['id']}}/input",
                        "commandArgs": ["--toto test"]
                    },
                    "outputStore": {
                        "assetStore": "{{params.game_id}}/{{ ti.xcom_pull(task_ids='get_game_info')['data']['game']['id']}}/{{ ti.xcom_pull(task_ids='get_game_info')['data']['game']['customer']['id']}}/output"
                    }
                },
                "gameId": "{{params.game_id}}"
            }""",
        log_response=True,
    )

    get_customer_metrics = PythonOperator(
        task_id="get_customer_metrics",
        python_callable=lambda: logger.info("run get_customer_metrics..."),
    )

    load_customer_metrics = PythonOperator(
        task_id="load_customer_metrics",
        python_callable=lambda: logger.info("run load_customer_metrics..."),
    )

    build_command_args = PythonOperator(
        task_id="build_command_args",
        python_callable=lambda: logger.info("run build_command_args..."),
    )

    run_engine_on_ecs = EcsRunTaskOperator(
        task_id="run_data_prep",
        cluster=ECSRunContext.CLUSTER,
        task_definition=ECSRunContext.TASK_DEFINITION,
        launch_type=ECSRunContext.LAUNCH_TYPE,
        overrides={
            "containerOverrides": [
                {
                    "name": "task-container-for-airflow-dev",  # name of the container in the task definition
                    "command": ["echo", "hello", "world"],
                },
            ],
        },
        platform_version=ECSRunContext.PLATFORM_VERSION,
        network_configuration=ECSRunContext.NETWORK_CONFIGURATION,
        awslogs_group=ECSRunContext.AWSLOGS_GROUP,
        awslogs_stream_prefix=ECSRunContext.AWSLOGS_STREAM_PREFIX,
        awslogs_region=ECSRunContext.AWSLOGS_REGION,
        # wait_for_completion=False,
        deferrable=True,
    )

    set_customer_game_to_processing = PythonOperator(
        task_id="set_customer_game_to_processing",
        python_callable=lambda: logger.info("run set_customer_game_to_processing..."),
    )

    (
        get_game_info 
        >> wait_for_provider_input_files
        >> get_provider_input_files
        >> set_customer_game_to_not_started
        >> create_job_request
        >> get_customer_metrics
        >> load_customer_metrics
        >> build_command_args
        >> run_engine_on_ecs
        >> set_customer_game_to_processing
    )


engine_with_lambdas()
