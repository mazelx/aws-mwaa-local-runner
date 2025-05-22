# SportsDynamics Mock API

This directory contains a mock implementation of the SportsDynamics API using FastAPI and Uvicorn. It is intended for local development and testing purposes.

## Features

- Simulates the SportsDynamics GraphQL API endpoints.
- Allows local testing of workflows that depend on the SportsDynamics API.
- Easily extensible for new endpoints or behaviors.

## Usage

1. **Install dependencies** (from the root of your project):

   ```bash
   pip install -e ./mock
   ```

2. **Run the mock server**:

   ```bash
   uvicorn mock.sd_api:app --reload --port 8000
   ```

   By default, the API will be available at [http://localhost:8000](http://localhost:8000).

3. **Configure your application** to use the mock API endpoint (e.g., `http://localhost:8000/graphql`).

## Project Structure

- `sd_api.py`: Main FastAPI application for the mock API.
- `pyproject.toml`: Python project configuration and dependencies.

## Extending

To add new endpoints or mock behaviors, edit `sd_api.py` and define additional routes or logic as needed.

## License

This mock API is for development and testing only.
