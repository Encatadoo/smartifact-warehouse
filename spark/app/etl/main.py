from pyspark.sql import SparkSession
from etl.config import KINESIS_ENDPOINT, KINESIS_REGION, KINESIS_STREAMS, CHECKPOINT_BASE
from etl.utils.dynamo_parser import parse_kinesis_payload
from etl.utils.time_utils import normalize_to_seconds
from etl.processors.process_type_processor import process_process_type_record
from etl.processors.process_instance_processor import process_process_instance_record
from etl.processors.artifact_event_processor import process_artifact_event_record
from etl.processors.stage_event_processor import process_stage_event_record
from etl.processors.process_deviations_processor import process_process_deviations_record


# ===================== SPARK SESSION =====================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SMARTifact_DW_ETL")
        .getOrCreate()
    )


# ===================== KINESIS READER =====================

def read_kinesis_stream(spark, stream_name):
    """Read a single Kinesis stream and return a streaming DataFrame with payload column."""
    return (
        spark.readStream
        .format("aws-kinesis")
        .option("kinesis.streamName", stream_name)
        .option("kinesis.endpointUrl", KINESIS_ENDPOINT)
        .option("kinesis.region", KINESIS_REGION)
        .option("kinesis.consumerType", "GetRecords")
        .option("kinesis.startingposition", "TRIM_HORIZON")
        .load()
        .selectExpr("CAST(data AS STRING) AS payload")
    )


# ===================== BATCH PROCESSOR =====================

def make_batch_processor(processor_fn):
    """Generic batch processor for streams that don't need ordering."""
    def _process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        for row in batch_df.collect():
            parsed = parse_kinesis_payload(row["payload"])
            if parsed["event_name"] == "REMOVE":
                continue
            processor_fn(parsed["record"], parsed["event_name"])
    return _process_batch


def make_sorted_stage_batch_processor():
    """
    Stage-event-specific batch processor that sorts events by timestamp
    before processing, ensuring correct deduplication across events
    that may arrive in different order within or across batches.
    """
    def _process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # parse all events in the batch
        events = []
        for row in batch_df.collect():
            parsed = parse_kinesis_payload(row["payload"])
            if parsed["event_name"] == "REMOVE":
                continue
            record = parsed["record"]
            ts = normalize_to_seconds(record.get("TIMESTAMP"))
            events.append((ts if ts is not None else 0, record, parsed["event_name"]))

        # sort by timestamp ascending — guarantees correct dedup ordering
        events.sort(key=lambda x: x[0])

        for _, record, event_name in events:
            process_stage_event_record(record, event_name)

    return _process_batch


# ------------------------------------------------------------------ #
#  STREAM WIRING                                                       #
# ------------------------------------------------------------------ #

def start_stream(spark, stream_key, batch_processor):
    stream_name = KINESIS_STREAMS[stream_key]
    df = read_kinesis_stream(spark, stream_name)
    return (
        df.writeStream
        .foreachBatch(batch_processor)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{stream_key}")
        .start()
    )


def main():
    spark = create_spark_session()

    queries = [
        start_stream(spark, "process_type",
                     make_batch_processor(process_process_type_record)),
        start_stream(spark, "process_instance",
                     make_batch_processor(process_process_instance_record)),
        start_stream(spark, "artifact_event",
                     make_batch_processor(process_artifact_event_record)),
        start_stream(spark, "stage_event",
                     make_sorted_stage_batch_processor()),          # ← sorted
        start_stream(spark, "process_deviations",
                     make_batch_processor(process_process_deviations_record)),
    ]

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()