from dags.utils.engine_helpers import parse_game_metadata, remove_disabled_metrics, build_command_args

def test_parse_game_metadata():
    raw = {
        "data": {
            "game": {
                "id": "game-123",
                "customer": {
                    "id": "cust-456",
                    "name": "CustomerName",
                    "languageCode": "EN"
                }
            }
        }
    }
    result = parse_game_metadata(raw)
    assert result["game_id"] == "game-123"
    assert result["customer_id"] == "cust-456"
    assert result["customer_name"] == "SportsDynamics"
    assert result["language"] == "EN"
    assert result["assetStore"] == "game-123/cust-456"
    assert "commandArgs" in result

def test_remove_disabled_metrics():
    data = {
        "data": {
            "getCompiledGameMetricFulfilments": [
                {"gmfid": "1", "disabled": False},
                {"gmfid": "2", "disabled": True},
                {"gmfid": "3"},  # default to not disabled
            ]
        }
    }
    filtered = remove_disabled_metrics(data)
    gmfs = filtered["data"]["getCompiledGameMetricFulfilments"]
    assert len(gmfs) == 2
    assert all(gmf["gmfid"] != "2" for gmf in gmfs)

def test_build_command_args():
    command = "run-job-request.sh"
    bucket = "bucket-x"
    command_args = ["--arg1", "val1", "--arg2", "val2", "--s3_input_path", "/input_overriden"]
    game_id = "game-x"
    customer_id = "cust-x"
    assetStore = "game-x/cust-x"
    result = build_command_args(command, bucket, command_args, game_id, customer_id, assetStore)
    # Should start with the CMD
    assert result[0].endswith("run-job-request.sh")
    # Should contain all expected keys/values
    assert "--game_id" in result and "game-x" in result
    assert "--customer_id" in result and "cust-x" in result
    assert "--arg1" in result and "val2" in result
    assert "--arg2" in result and "val2" in result
    assert "--s3_input_path" in result and "/input_overriden" in result
    assert "--s3_output_path" in result and assetStore + "/output" in result

