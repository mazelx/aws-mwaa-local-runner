from dags.utils.engine_helpers import parse_game_metadata, clean_metrics_dict, build_command_args

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
    assert result["assetStore"] == "cust-456/game-123"
    assert "commandArgs" in result

def test_clean_metrics_dict():
    data = {
        "data": {
            "getCompiledGameMetricFulfilments": [
                {"gmfid": "1", "disabled": False, "name": "metric1"},
                {"gmfid": "2", "disabled": True, "name": "metric2"},
                {"gmfid": "3", "name": "metric3"},  # default to not disabled
                {"gmfid": "1", "disabled": False, "name": "metric1_duplicate"},  # duplicate gmfid
                {"gmfid": "4", "disabled": False, "name": "metric4"},
            ]
        }
    }
    gmfs = clean_metrics_dict(data)
    
    # Should have 3 items: gmfid "1" (first occurrence), "3", and "4"
    # gmfid "2" removed (disabled), gmfid "1" duplicate removed
    assert len(gmfs) == 3
    
    # Check that disabled metric is removed
    assert all(gmf["gmfid"] != "2" for gmf in gmfs)
    
    # Check that duplicates are removed - only first occurrence of gmfid "1" should remain
    gmfid_1_items = [gmf for gmf in gmfs if gmf["gmfid"] == "1"]
    assert len(gmfid_1_items) == 1
    assert gmfid_1_items[0]["name"] == "metric1"  # first occurrence kept
    
    # Check that all remaining gmfids are unique
    gmfids = [gmf["gmfid"] for gmf in gmfs]
    assert len(gmfids) == len(set(gmfids))

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

