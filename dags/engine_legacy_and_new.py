import logging
from datetime import timedelta

from airflow.decorators import dag, task_group
from airflow.utils.dates import days_ago
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

# AWS ECS CONTEXT
class ECSRunContext:
    CLUSTER = 'ecs-cluster-for-airflow-dev'
    TASK_DEFINITION = 'task-for-airflow-dev'
    LAUNCH_TYPE = 'FARGATE'
    PLATFORM_VERSION = 'LATEST'
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
    'owner': 'xavier',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['xavier@sportsdynamics.eu'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# DAG DEFINITION
@dag(
    dag_id='dag_engine_legacy_and_new',
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
def dag_engine_legacy_and_new():
    """
    DAG : 
    """
    # TASKS
    wait_for_provider_input_files = HttpSensor(
        task_id='wait_for_provider_input_files',
        http_conn_id='sd_api_conn',
        endpoint='/provider_input_files',
        method="GET",
        request_params={},
        response_check=lambda response: response.status_code == 200,
        timeout=60*5, # total timeout (10 minutes in prod)
        poke_interval=2, # checks interval (15 minutes in prod)
        mode='poke', # In prod, use reschedule mode to avoid blocking the scheduler
    )

    get_provider_input_files = PythonOperator(
        task_id="get_provider_input_files",
        python_callable=lambda: logger.info("get_provider_input_files..."),
    )

    run_data_prep = EcsRunTaskOperator(
        task_id="run_data_prep",
        cluster=ECSRunContext.CLUSTER,
        task_definition=ECSRunContext.TASK_DEFINITION,
        launch_type=ECSRunContext.LAUNCH_TYPE,
        overrides={
            "containerOverrides": [
                {
                    "name": "task-container-for-airflow-dev", # name of the container in the task definition
                    "command": ["echo", "hello", "world"],
                },
            ],
        },
        platform_version = ECSRunContext.PLATFORM_VERSION,
        network_configuration = ECSRunContext.NETWORK_CONFIGURATION,
        awslogs_group = ECSRunContext.AWSLOGS_GROUP,
        awslogs_stream_prefix = ECSRunContext.AWSLOGS_STREAM_PREFIX,
        awslogs_region = ECSRunContext.AWSLOGS_REGION,
    )

    run_possession_detection = PythonOperator(
        task_id="run_possession_detection",
        python_callable=lambda: logger.info("run_possession_detection..."),
    )

    run_old_engine_interface = PythonOperator(
        task_id="run_old_engine_interface",
        python_callable=lambda: logger.info("run_old_engine_interface..."),
    )
    
    get_customer_games = HttpOperator(
        task_id="get_customer_games",
        http_conn_id="sd_api_conn",
        endpoint="/customer_games",
        method="GET",
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: response.json()["customer_games_id"],
        log_response=True,
        do_xcom_push=True,
    )

    @task_group(group_id="customer_game_engine_group")
    def customer_game_engine_group(customer_game_id:str):
        """
        Task group for customer game engine
        """
    
        get_metric_fulfillments = PythonOperator(
            task_id="get_metric_fulfillments",
            python_callable=lambda: logger.info("get_metric_fulfillments for {{ params.customer_game_id }} ... "),
        )

        run_customer_game_engine = PythonOperator(
            task_id="run_customer_game_engine",
            python_callable=lambda: logger.info("run_customer_game_engine for {{ params.customer_game_id }} ..."),
        )

        wait_for_customer_engine = HttpSensor(
            task_id='wait_for_customer_engine',
            http_conn_id='sd_api_conn',
            endpoint='/customer_game_engine',
            method="GET",
            request_params={},
            response_check=lambda response: response.status_code == 200,
            timeout=60*5, # total timeout (10 minutes in prod)
            poke_interval=2, # checks interval (15 minutes in prod)
            mode='poke', # In prod, use reschedule mode to avoid blocking the scheduler
        )

        get_metric_fulfillments >> run_customer_game_engine >> wait_for_customer_engine   

    customer_games_engines = customer_game_engine_group.expand(customer_game_id=get_customer_games.output)

    wait_for_provider_input_files \
        >> get_provider_input_files \
        >> run_data_prep \
        >> run_possession_detection \
        >> run_old_engine_interface \
        >> get_customer_games \
        >> customer_games_engines

dag_engine_legacy_and_new = dag_engine_legacy_and_new()