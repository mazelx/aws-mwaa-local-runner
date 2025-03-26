from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.utils.dates import days_ago


def get_engine_params(game_metadata:dict) -> dict:
    args_dict = {}
    command_list = game_metadata['overrides']['containerOverrides'][0]['command']
    for i, elem in enumerate(command_list):
        if elem.startswith('--'):
            args_dict[elem[2:]] = command_list[i + 1]
    return args_dict

@dag(
        default_args={'owner': 'airflow'},
        schedule_interval="@daily",
        start_date=days_ago(2),
        tags=['example'],
        params={"game_metadata": {"cpu": "16384","tags": [],"group": "family:engine","memory": "16384","taskArn": "arn:aws:ecs:eu-west-1:715329755018:task/skynet/79994a95c0eb4e08966f6c99be2b27be","version": 4,"stopCode": "EssentialContainerExited","createdAt": "2025-04-21T08:25:12.083Z","overrides": {  "containerOverrides": [    {      "name": "engine",      "command": [        "bin/engine/run-job-request.sh",        "--game_id",        "bab6ec2f-db53-4f7b-9031-e7d49f4738da",        "--customer_id",        "acc8d283-b7a0-412f-8daa-ae2e3ccdf08f",        "--language",        "EN",        "--generate_dynamical_maps",        "no",        "--home_team_id",        "286b1082-46aa-449f-9c6b-cf98f5435417",        "--away_team_id",        "79ad5c0c-4926-4105-8c0d-2118efd4c601",        "--game_info_file",        "metadata.json",        "--home_team_color_rgb",        "[1,0,0]",        "--away_team_color_rgb",        "[0,0,1]",        "--ball_color_rgb",        "[1,0,0.984313725490196]",        "--tracking_file",        "tracking.jsonl",        "--event_file",        "insights_35ce0ab7-17ff-4210-8ca1-70c65dfc6aaa.jsonl",        "--retry",        "no",        "--s3_bucket_name",        "assets-20200820085407743800000001",        "--s3_input_path",        "acc8d283-b7a0-412f-8daa-ae2e3ccdf08f/bab6ec2f-db53-4f7b-9031-e7d49f4738da/input",        "--s3_output_path",        "acc8d283-b7a0-412f-8daa-ae2e3ccdf08f/bab6ec2f-db53-4f7b-9031-e7d49f4738da/output"      ]    }  ],  "inferenceAcceleratorOverrides": []},"startedAt": "2025-04-21T08:25:21.073Z","stoppedAt": "2025-04-21T08:53:13.754Z","attributes": [  {    "name": "ecs.cpu-architecture",    "value": "arm64"  }],"clusterArn": "arn:aws:ecs:eu-west-1:715329755018:cluster/skynet","containers": [  {    "cpu": "16384",    "name": "engine",    "image": "715329755018.dkr.ecr.eu-west-1.amazonaws.com/engine:production-arm64",    "taskArn": "arn:aws:ecs:eu-west-1:715329755018:task/skynet/79994a95c0eb4e08966f6c99be2b27be",    "exitCode": 0,    "runtimeId": "75d34dde4dcd50f6e723e9291fc82344a38be4508011a6c86dafe835458f0e29",    "lastStatus": "STOPPED",    "imageDigest": "sha256:0484cbd1882a7894e99460626f8c8a1045ec042fe464ab6b6294f0b13fc0e8dd",    "containerArn": "arn:aws:ecs:eu-west-1:715329755018:container/skynet/79994a95c0eb4e08966f6c99be2b27be/f4b2c4c0-4b16-437c-a63d-fba10838f164",    "healthStatus": "UNKNOWN",    "networkBindings": [],    "memoryReservation": "16384",    "networkInterfaces": []  }],"lastStatus": "STOPPED","launchType": "EC2","stoppingAt": "2025-04-21T08:53:13.754Z","attachments": [],"connectivity": "CONNECTED","healthStatus": "UNKNOWN","desiredStatus": "STOPPED","pullStartedAt": "2025-04-21T08:25:20.296Z","pullStoppedAt": "2025-04-21T08:25:20.349Z","stoppedReason": "Essential container in task exited","connectivityAt": "2025-04-21T08:25:12.083Z","availabilityZone": "eu-west-1a","taskDefinitionArn": "arn:aws:ecs:eu-west-1:715329755018:task-definition/engine:51","executionStoppedAt": "2025-04-21T08:53:13.734Z","capacityProviderName": "skynet-ecs_provider","containerInstanceArn": "arn:aws:ecs:eu-west-1:715329755018:container-instance/skynet/cc140843b7e14a9398c2c6e637e86b93","enableExecuteCommand": False},},
)
def dag_params():
    parse_params = PythonOperator(
        task_id="parse_param",
        python_callable=get_engine_params,
        op_args=["{{ params.game_metadata}}"], 
    )

    display_params = PythonOperator(
        task_id="display_param",
        python_callable=lambda x: print(x['game_id']),
        op_args=["{{ task_instance.xcom_pull('push_task') }}"]
    )

    parse_params >> display_params

dag_params()