import logging
from datetime import timedelta

from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from graphql_http_operator import GraphQLHttpOperator
from utils import engine_helpers

class EngineContext:
    CMD = "bin/engine/run-job-request.sh"


class AWSContext:
    CLUSTER = "skynet"
    TASK_DEFINITION = "engine"
    LAUNCH_TYPE = "EC2"
    PLATFORM_VERSION = "LATEST"
    CONTAINER_NAME = "engine"
    NETWORK_CONFIGURATION = {
        "awsvpcConfiguration": {
            "subnets": [
                "subnet-062188a41839490f5",
                "subnet-01a8f8ef563fac8e2",
                "subnet-0191dc555146fc5ba",
                "subnet-0d6369771c7ffe5b9",
                "subnet-bec0a9e4",
                "subnet-c9351aaf",
                "subnet-3c83bc74",
            ],
            "securityGroups": ["sg-1d49625c"],
            "assignPublicIp": "ENABLED",
        },
    }
    AWSLOGS_GROUP = "skynet"
    AWSLOGS_STREAM_PREFIX = "engine/engine"
    AWSLOGS_REGION = "eu-west-1"
    BUCKET_NAME = "assets-20200803082231804400000001"

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
    params={
        "game_id": "8186f8e5-a83e-4bc9-8449-5007172fc198",
        "command_args": [
            "--home_team_color_rgb",
            "[1,0,0]",
            "--away_team_color_rgb",
            "[0,0,1]",
        ],
    },
    render_template_as_native_obj=True,  # to get dict instead of string in xcom_pull
)
def engine_with_lambdas():
    """Run Engine pipeline on AWS ECS with GraphQL API calls and S3 operations.
    This DAG fetches game metadata, waits for provider input files, creates a job request,
    retrieves customer metrics, and runs the engine on ECS.
    It also includes steps to set the game state and load customer metrics into S3.
    """

    # Get game metadata from GraphQL API
    # Input : game_id
    # Output : game_id, customer_id, customer_name, commandArgs, assetStore
    get_game_metadata = GraphQLHttpOperator(
        task_id="get_game_metadata",
        http_conn_id="sd_legacy_api_conn",
        endpoint="graphql",
        query="""query($id: ID!) {
                game(id: $id) {
                    id
                    customer {
                        id
                        name
                        languageCode
                    }
                    lastJobRequest {
                        id
                        state
                    }
                }
            }""",
        variables="""{ "id": "{{ params.game_id}}" } """,
    )

    # Parse game metadata to extract useful information as a dictionary
    # Input : raw game metadata from GraphQL API
    # Output : dict with keys: game_id, customer_id, customer_name, commandArgs, assetStore
    parse_game_metadata = PythonOperator(
        task_id="parse_game_metadata",
        python_callable=engine_helpers.parse_game_metadata,
        op_args=["{{ task_instance.xcom_pull(task_ids='get_game_metadata') }}"],
    )

    # Build command arguments for the ECS task
    build_command_args = PythonOperator(
        task_id="build_command_args",
        python_callable=engine_helpers.build_command_args,
        op_args=[
            EngineContext.CMD,
            AWSContext.BUCKET_NAME,
            "{{ params.command_args }}",
            "{{ ti.xcom_pull(task_ids='parse_game_metadata')['game_id'] }}",
            "{{ ti.xcom_pull(task_ids='parse_game_metadata')['customer_id'] }}",
            "{{ ti.xcom_pull(task_ids='parse_game_metadata')['assetStore'] }}",
        ],
    )

    # TODO : We should probably check for specific files instead of just checking for the existence of any file in the input directory.
    # Check that files are available in the asset store
    # Input : None
    # Output : None
    check_for_provider_input_files = S3KeySensor(
        task_id="check_for_provider_input_files",
        bucket_name=AWSContext.BUCKET_NAME,
        bucket_key=[
            "{{ ti.xcom_pull(task_ids='parse_game_metadata')['assetStore'] }}/input/*"
        ],
        wildcard_match=True,
        deferrable=True,
        poke_interval=60,  # Check every 60 seconds
        timeout=60*15, 
    )

    # Create job request for the game through GraphQL API
    # Input : game_id, customer_id, commandArgs, assetStore
    # Output : job request ID
    create_job_request = GraphQLHttpOperator(
        task_id="create_job_request",
        http_conn_id="sd_legacy_api_conn",
        endpoint="graphql",
        query="""mutation ($data: JobRequestCreateInput!, $gameId: ID!) {
            createJobRequestByGameId(data: $data, gameId: $gameId) {
                id
            }
        }""",
        variables={
            "data": {
                "inputStore": {
                    "assetStore": "{{ ti.xcom_pull(task_ids='parse_game_metadata')['assetStore'] }}/input",
                    "commandArgs": "{{ ti.xcom_pull(task_ids='parse_game_metadata')['commandArgs'] }}",
                },
                "outputStore": {
                    "assetStore": "{{ ti.xcom_pull(task_ids='parse_game_metadata')['assetStore'] }}/output"
                },
            },
            "gameId": "{{ ti.xcom_pull(task_ids='parse_game_metadata')['game_id']}}",
        },
    )

    # Get customer metrics from GraphQL API
    # Input : customer_id, language
    # Output : list of game metric fulfilments (JSON format)
    get_customer_metrics = GraphQLHttpOperator(
        task_id="get_customer_metrics",
        http_conn_id="sd_legacy_api_conn",
        endpoint="graphql",
        query="""
            query getCompiledGameMetricFulfilments(
                $customerId: UUID!
                $language: AppLanguage!
            ) {
            getCompiledGameMetricFulfilments(
                customerId: $customerId
                language: $language
                ) {
                    gmfid
                    name
                    disabled
                    config
                }
            }
        """,
        variables={
            "customerId": "{{ ti.xcom_pull(task_ids='parse_game_metadata')['customer_id']}}",
            "language": "{{ ti.xcom_pull(task_ids='parse_game_metadata')['language']}}",
        },
        post_process=engine_helpers.remove_disabled_metrics,
    )

    # Load customer metrics into S3 bucket
    # Input : list of game metric fulfilments (JSON format)
    # Output : S3 object with key "<assetStore>/input/gmf_list.json"
    load_customer_metrics = S3CreateObjectOperator(
        task_id="load_customer_metrics",
        s3_bucket=AWSContext.BUCKET_NAME,
        s3_key="{{ ti.xcom_pull(task_ids='parse_game_metadata')['assetStore']}}/input/gmf_list.json",
        data="{{ ti.xcom_pull(task_ids='get_customer_metrics') | tojson }}",
        replace=True,
    )

    # Set the customer game status to "processing"
    # Input : game_id
    # Output : None
    set_customer_game_to_processing = GraphQLHttpOperator(
        task_id="set_customer_game_to_processing",
        http_conn_id="sd_legacy_api_conn",
        endpoint="graphql",
        query="""mutation($partialEntity: JobRequestUpdateInput!, $id: ID!) {
                    updateJobRequest(partialEntity: $partialEntity, id: $id) {
                        id
                        state
                }
            }""",
        variables={
            "partialEntity": {"state": "RUNNING"},
            "id": "{{ ti.xcom_pull(task_ids='create_job_request')['data']['createJobRequestByGameId']['id'] }}",
        },
    )

    # TODO : replace the mocked operation with the real one
    # Run the engine on ECS using the EcsRunTaskOperator
    # Input : command arguments, game_id, customer_id
    # Output : ECS task execution
    run_engine_on_ecs = EcsRunTaskOperator(
        task_id="run_data_prep",
        cluster=AWSContext.CLUSTER,
        task_definition=AWSContext.TASK_DEFINITION,
        launch_type=AWSContext.LAUNCH_TYPE,
        overrides={
            "containerOverrides": [
                {
                    "name": AWSContext.CONTAINER_NAME,  # name of the container in the task definition
                    "command": ["echo", "hello", "world"],
                },
            ],
        },
        # platform_version=AWSContext.PLATFORM_VERSION,
        # network_configuration=AWSContext.NETWORK_CONFIGURATION,
        awslogs_group=AWSContext.AWSLOGS_GROUP,
        awslogs_stream_prefix=AWSContext.AWSLOGS_STREAM_PREFIX,
        awslogs_region=AWSContext.AWSLOGS_REGION,
        # wait_for_completion=False,
        deferrable=True,
    )

    # Set the customer game status to "processed"
    # Input : game_id
    # Output : None
    set_customer_game_to_processed = GraphQLHttpOperator(
        task_id="set_customer_game_to_processed",
        http_conn_id="sd_legacy_api_conn",
        endpoint="graphql",
        query="""mutation($partialEntity: JobRequestUpdateInput!, $id: ID!) {
                    updateJobRequest(partialEntity: $partialEntity, id: $id) {
                        id
                        state
                }
            }""",
        variables={
            "partialEntity": {"state": "FINISHED"},
            "id": "{{ ti.xcom_pull(task_ids='create_job_request')['data']['createJobRequestByGameId']['id'] }}",
        },
    )

    (
        get_game_metadata
        >> parse_game_metadata
        >> build_command_args
        >> check_for_provider_input_files
        >> create_job_request
        >> get_customer_metrics
        >> load_customer_metrics
        >> set_customer_game_to_processing
        >> run_engine_on_ecs
        >> set_customer_game_to_processed
    )


engine_with_lambdas()
