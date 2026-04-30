KINESIS_REGION = "us-east-1"
KINESIS_ENDPOINT = "http://kinesis.us-east-1.amazonaws.com"

KINESIS_STREAMS = {
    "artifact_event": "artifact_event_kinesis",
    "stage_event": "stage_event_kinesis",
    "process_instance": "process_instance_kinesis",
    "process_deviations": "process_deviations_kinesis",
    "process_type": "process_type_kinesis",
}

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "appdb",
    "user": "appuser",
    "password": "apppassword",
}

CHECKPOINT_BASE = "/opt/spark/checkpoints"

