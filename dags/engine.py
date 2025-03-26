from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.param import Param

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

# Retrieve S3 path from DAG argument or Airflow Variable

with DAG(
    dag_id='run_pipeline_on_ec2',
    default_args=default_args,
    description='Run Engine pipeline on EC2 instance',
    schedule_interval=None,  # ou None si lancement manuel
    start_date=datetime(2025, 1, 1),
    catchup=False, 
    tags=['pipeline', 'ec2', 'ssh'],
    # todo schema validation (https://airflow.apache.org/docs/apache-airflow/2.10.1/core-concepts/params.html#json-schema-validation)
    params={
        "dict_param": {"key": "value"},
        "game_metadata": {
            "cpu": "16384",
            "tags": [],
            "group": "family:engine",
            "memory": "16384",
            "taskArn": "arn:aws:ecs:eu-west-1:715329755018:task/skynet/79994a95c0eb4e08966f6c99be2b27be",
            "version": 4,
            "stopCode": "EssentialContainerExited",
            "createdAt": "2025-04-21T08:25:12.083Z",
            "overrides": {
                "containerOverrides": [
                {
                    "name": "engine",
                    "command": [
                    "bin/engine/run-job-request.sh",
                    "--game_id",
                    "bab6ec2f-db53-4f7b-9031-e7d49f4738da",
                    "--customer_id",
                    "acc8d283-b7a0-412f-8daa-ae2e3ccdf08f",
                    "--language",
                    "EN",
                    "--generate_dynamical_maps",
                    "no",
                    "--home_team_id",
                    "286b1082-46aa-449f-9c6b-cf98f5435417",
                    "--away_team_id",
                    "79ad5c0c-4926-4105-8c0d-2118efd4c601",
                    "--game_info_file",
                    "metadata.json",
                    "--home_team_color_rgb",
                    "[1,0,0]",
                    "--away_team_color_rgb",
                    "[0,0,1]",
                    "--ball_color_rgb",
                    "[1,0,0.984313725490196]",
                    "--tracking_file",
                    "tracking.jsonl",
                    "--event_file",
                    "insights_35ce0ab7-17ff-4210-8ca1-70c65dfc6aaa.jsonl",
                    "--retry",
                    "no",
                    "--s3_bucket_name",
                    "assets-20200820085407743800000001",
                    "--s3_input_path",
                    "acc8d283-b7a0-412f-8daa-ae2e3ccdf08f/bab6ec2f-db53-4f7b-9031-e7d49f4738da/input",
                    "--s3_output_path",
                    "acc8d283-b7a0-412f-8daa-ae2e3ccdf08f/bab6ec2f-db53-4f7b-9031-e7d49f4738da/output"
                    ]
                }
                ],
                "inferenceAcceleratorOverrides": []
            },
            "startedAt": "2025-04-21T08:25:21.073Z",
            "stoppedAt": "2025-04-21T08:53:13.754Z",
            "attributes": [
                {
                "name": "ecs.cpu-architecture",
                "value": "arm64"
                }
            ],
            "clusterArn": "arn:aws:ecs:eu-west-1:715329755018:cluster/skynet",
            "containers": [
                {
                "cpu": "16384",
                "name": "engine",
                "image": "715329755018.dkr.ecr.eu-west-1.amazonaws.com/engine:production-arm64",
                "taskArn": "arn:aws:ecs:eu-west-1:715329755018:task/skynet/79994a95c0eb4e08966f6c99be2b27be",
                "exitCode": 0,
                "runtimeId": "75d34dde4dcd50f6e723e9291fc82344a38be4508011a6c86dafe835458f0e29",
                "lastStatus": "STOPPED",
                "imageDigest": "sha256:0484cbd1882a7894e99460626f8c8a1045ec042fe464ab6b6294f0b13fc0e8dd",
                "containerArn": "arn:aws:ecs:eu-west-1:715329755018:container/skynet/79994a95c0eb4e08966f6c99be2b27be/f4b2c4c0-4b16-437c-a63d-fba10838f164",
                "healthStatus": "UNKNOWN",
                "networkBindings": [],
                "memoryReservation": "16384",
                "networkInterfaces": []
                }
            ],
            "lastStatus": "STOPPED",
            "launchType": "EC2",
            "stoppingAt": "2025-04-21T08:53:13.754Z",
            "attachments": [],
            "connectivity": "CONNECTED",
            "healthStatus": "UNKNOWN",
            "desiredStatus": "STOPPED",
            "pullStartedAt": "2025-04-21T08:25:20.296Z",
            "pullStoppedAt": "2025-04-21T08:25:20.349Z",
            "stoppedReason": "Essential container in task exited",
            "connectivityAt": "2025-04-21T08:25:12.083Z",
            "availabilityZone": "eu-west-1a",
            "taskDefinitionArn": "arn:aws:ecs:eu-west-1:715329755018:task-definition/engine:51",
            "executionStoppedAt": "2025-04-21T08:53:13.734Z",
            "capacityProviderName": "skynet-ecs_provider",
            "containerInstanceArn": "arn:aws:ecs:eu-west-1:715329755018:container-instance/skynet/cc140843b7e14a9398c2c6e637e86b93",
            "enableExecuteCommand": False
            }            
    },
        
) as dag:
    
    activate_env_cmd  = f"cd {ENGINE_WORKING_DIR}/ECDO  && source ~/anaconda3/etc/profile.d/conda.sh && conda activate SD"

    prepare_environment = SSHOperator(
        task_id='ssh_prepare_environment',
        ssh_conn_id='ec2_ssh_conn',
        command=activate_env_cmd + " && AWS_PROFILE=sd-production aws s3 cp s3://{{ params.game_metadata | fromjson['overrides']['containerOverrides'][0]['s3_bucket_name'] }}/{{ params.game_metadata | fromjson['overrides']['containerOverrides'][0]['s3_input_path'] }} " + ENGINE_INPUT_DATA_DIR + " --recursive",
        conn_timeout=3600,
        cmd_timeout=3600,
    )

    run_pipeline = SSHOperator(
        task_id='ssh_run_pipeline',
        ssh_conn_id='ec2_ssh_conn',
        command = activate_env_cmd + """ && python launcher.py \
        --game_id {{ params.get_metadata['game_id'] }} \
        --customer_id {{ params.get_metadata['customer_id'] }} \
        --language {{ params.get_metadata['language'] }} \
        --generate_dynamical_maps {{ params.get_metadata['generate_dynamical_maps'] }} \
        --home_team_id {{ params.get_metadata['home_team_id'] }} \
        --away_team_id {{ params.get_metadata['away_team_id'] }} \
        --game_info_file {{ params.get_metadata['game_info_file'] }} \
        --home_team_color_rgb {{ params.get_metadata['home_team_color_rgb'] }} \
        --away_team_color_rgb {{ params.get_metadata['away_team_color_rgb'] }} \
        --ball_color_rgb {{ params.get_metadata['ball_color_rgb'] }} \
        --tracking_file  {{ params.get_metadata['tracking_file'] }} \
        --event_file {{ params.get_metadata['event_file'] }} \
        --retry {{ params.get_metadata['retry'] }} \
    """,
        conn_timeout=3600,
        cmd_timeout=3600,
    )

    prepare_environment >> run_pipeline
