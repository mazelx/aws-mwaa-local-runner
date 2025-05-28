def parse_game_metadata(raw_game_metadata: dict) -> dict:
    """Parses the raw game metadata from the GraphQL API response and extracts useful information."""
    game_metadata = {}
    game_metadata["game_id"] = raw_game_metadata["data"]["game"]["id"]
    game_metadata["customer_id"] = raw_game_metadata["data"]["game"]["customer"]["id"]
    game_metadata["customer_name"] = (
        "SportsDynamics"  # TODO : understand how to get the customer name (local mapping)
    )
    game_metadata["language"] = raw_game_metadata["data"]["game"]["customer"][
        "languageCode"
    ]
    game_metadata["assetStore"] = (
        f"{game_metadata['game_id']}/{game_metadata['customer_id']}"
    )
    game_metadata["commandArgs"] = " "

    return game_metadata


def remove_disabled_metrics(data: dict) -> dict:
    """
    Removes items with 'disabled': True from the 'getCompiledGameMetricFulfilments' list in the given dict.
    Returns a new dict with only enabled items.
    """
    if (
        "data" in data
        and "getCompiledGameMetricFulfilments" in data["data"]
        and isinstance(data["data"]["getCompiledGameMetricFulfilments"], list)
    ):
        filtered = [
            item
            for item in data["data"]["getCompiledGameMetricFulfilments"]
            if not item.get("disabled", False)
        ]
        # Return a new dict with the filtered list
        return {
            **data,
            "data": {
                **data["data"],
                "getCompiledGameMetricFulfilments": filtered,
            },
        }
    return data


def build_command_args(command:str, bucket_name:str, command_args: list[str], game_id: str, customer_id, assetStore: str) -> str:
    """Builds the command arguments for the ECS task."""

    # Parse command arguments into a dictionary
    args_dict = {}
    i = 0
    while i < len(command_args):
        if command_args[i].startswith("--"):
            # Check if next element exists and is not a flag
            if i + 1 < len(command_args) and not command_args[i + 1].startswith("--"):
                args_dict[command_args[i][2:]] = command_args[i + 1]
                i += 2
            else:
                args_dict[command_args[i][2:]] = None
                i += 1
        else:
            i += 1

    # Override specific arguments with values from the game metadata
    args_dict = {
        "game_id": game_id,
        "customer_id": customer_id,
        "s3_bucket_name": bucket_name,
        "s3_input_path": assetStore + "/input",
        "s3_output_path": assetStore + "/output",
    } | args_dict

    # Build the command array
    command_args = [command]
    command_args.extend(
        [
            item
            for key, value in args_dict.items()
            if value is not None
            for item in [f"--{key}", value]
        ]
    )
    return command_args
