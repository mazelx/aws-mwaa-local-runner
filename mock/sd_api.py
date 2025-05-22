from fastapi import FastAPI, HTTPException, Request
from uuid import uuid4
import random

app = FastAPI()

@app.post("/graphql")
async def graphql_proxy(request: Request):
    body = await request.body()
    headers = request.headers
    print(body)
    print(headers)  
    return body


@app.get("/customer_games")
def get_random_customer_games():
    n = random.randint(4, 8)
    uuids = [str(uuid4()) for _ in range(n)]
    return {"customer_games_id": uuids}

@app.get("/provider_input_files")
def get_provider_input_files():
    n = random.randint(1, 10)
    if n != 1:
        raise HTTPException(status_code=404, detail="No provider input files found")
    return {
        "input_files": [
            {
                "file_name": "tracking_file.csv",
                "file_path": "/path/to/tracking_file.csv"
            },
            {
                "file_name": "event_file.csv",
                "file_path": "/path/to/event_file.csv",
            }
        ]
    }

@app.get("/customer_game_engine")
def get_customer_game_engine():
    n = random.randint(1, 70)
    if n != 1:
        raise HTTPException(status_code=404, detail="Customer game engine run not finished")
    return {
        "engine_status": "success",
        "metrics": [
            {
                "metric_name": "metric_1",
                "metric_value": random.uniform(0, 100)
            },
            {
                "metric_name": "metric_2",
                "metric_value": random.uniform(0, 100)
            }
        ]
    }