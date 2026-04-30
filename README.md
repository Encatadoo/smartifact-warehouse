# SMARTifact Warehouse

This repository contains the Docker-based runtime used for the SMARTifact project developed as part of a Master’s thesis at the Technical University of Denmark.

The platform is built from multiple SMARTifact modules that are started together with Docker Compose. Before running the stack, make sure the following repositories are available in the project root alongside this file:

- `egsm-aggregator`
- `egsm-frontend`
- `egsm-worker`
- `egsm-common`
- `artifact-emulator`

You can download them from the page at:

https://github.com/meronig?tab=repositories

## Repository Layout

The Compose file expects the module folders to be available as sibling directories, for example:

```text
smartifact-warehouse/
  docker-compose.yml
  egsm-aggregator/
  egsm-frontend/
  egsm-worker/
  egsm-common/
  artifact-emulator/
  egsm-supervisor/
```

The `egsm-supervisor` module in this repository has been modified so that startup also creates the required DynamoDB tables, creates the necessary Kinesis streams, and binds the supported tables to streaming destinations during initialization.

## What The Stack Starts

The current Docker Compose setup starts the following services:

- `mosquitto` on port `1883`
- `localstack` on port `4566` with DynamoDB, DynamoDB Streams, and Kinesis enabled
- `dynamodb-admin` on port `8001`
- `postgres` on port `5432`
- `pgadmin` on port `5050`
- `aws-endpoint-proxy` for Kinesis endpoint resolution inside the Docker network
- `spark-master` on ports `7078` and `8082`
- `spark-worker`
- `spark-submit`
- `egsm-supervisor` on port `8081`
- `egsm-worker` on ports `9001-9200`
- `egsm-aggregator` on ports `9201-9400`

The `egsm-frontend` service is present in the Compose file but currently commented out.

## Prerequisites

- Docker Desktop or another Docker Engine installation
- Docker Compose
- The SMARTifact module repositories listed above

The supervisor configuration in `egsm-supervisor/config/config.xml` points to the local services used by the Compose setup:

- MQTT broker: `mosquitto:1883`
- DynamoDB / LocalStack: `localstack:4566`
- Kinesis / LocalStack: `localstack:4566`

## Setup

1. Download or clone the required module repositories into the root of this project.
2. Make sure each module folder contains its own `Dockerfile` and application files.
3. Verify that the supervisor configuration matches your environment if you change hostnames or ports.

## Run The Platform

1. Start the full stack:

	```bash
	docker compose up --build
	```

2. Wait for LocalStack and Spark to become healthy.
3. Open the service endpoints you need, for example:
	- Supervisor: `http://localhost:8081`
	- DynamoDB Admin: `http://localhost:8001`
	- pgAdmin: `http://localhost:5050`
	- Spark UI: `http://localhost:8082`

When the supervisor container starts, it runs `db-init-tables.js` before launching `main.js`. That initialization step creates the required tables and Kinesis streams and binds the relevant tables to stream destinations automatically.

## Supervisor Details

The supervisor boot sequence is defined in `egsm-supervisor/run.sh` and `egsm-supervisor/main.js`.

- `db-init-tables.js` initializes the database schema and Kinesis bindings.
- `main.js` loads `config/config.xml`, connects to MQTT and DynamoDB, and exports the process library to the database.

The tables that are currently bound to Kinesis streaming are:

- `ARTIFACT_EVENT`
- `PROCESS_DEVIATIONS`
- `STAGE_EVENT`
- `PROCESS_INSTANCE`
- `PROCESS_TYPE`

## Notes

- The stack is configured for a local development environment backed by LocalStack.
- If you want to reset the schema manually, the supervisor folder also contains `db-delete-tables.js`.
- This project was prepared for a Master’s thesis at the Technical University of Denmark.
