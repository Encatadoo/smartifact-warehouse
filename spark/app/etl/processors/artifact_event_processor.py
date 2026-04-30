from etl.utils.time_utils import normalize_to_seconds, build_dim_time_record, ts_str_to_unix
from etl.db.postgres_handler import (
    get_connection,
    upsert_dim_artifact,
    upsert_dim_time,
    ensure_dim_process_exists,
    upsert_fact_artifact_attach,
    update_fact_artifact_detach,
    get_last_detach_timestamp,
    get_attach_timestamp,
)


def process_artifact_event_record(record, event_name):
    """
    Handle ARTIFACT_EVENT CDC record.
    
    On "attached": upsert FACT_ARTIFACT_ENGAGEMENT (INSERT or UPDATE on re-attach).
    On "detached": update the open engagement row.
    """
    artifact_name = record.get("ARTIFACT_NAME")
    artifact_type, artifact_id = artifact_name.split("/")

    process_id_raw = record.get("PROCESS_ID")
    process_id = process_id_raw.split("__")[0]
    process_type = record.get("PROCESS_TYPE")

    artifact_state = record.get("ARTIFACT_STATE")

    utc_seconds = normalize_to_seconds(record.get("UTC_TIME"))
    time_record = build_dim_time_record(utc_seconds)

    conn = get_connection()
    try:
        cur = conn.cursor()

        upsert_dim_artifact(cur, artifact_id, artifact_type)
        ensure_dim_process_exists(cur, process_id, process_type)
        time_id = upsert_dim_time(cur, time_record)

        if artifact_state == "attached":
            _handle_attach(cur, artifact_id, process_id, time_id, utc_seconds)
        elif artifact_state == "detached":
            _handle_detach(cur, artifact_id, process_id, time_id, utc_seconds)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def _handle_attach(cur, artifact_id, process_id, time_id, utc_seconds):
    """
    Upsert engagement row.
    First attach: idle = None.
    Re-attach: idle = current_attach_time - previous_detach_time.
    """
    idle = None
    last_detach_ts = get_last_detach_timestamp(cur, artifact_id, process_id)
    if last_detach_ts and utc_seconds:
        idle = utc_seconds - ts_str_to_unix(last_detach_ts)

    upsert_fact_artifact_attach(
        cur,
        artifact_id=artifact_id,
        process_id=process_id,
        time_attached_id=time_id,
        idle_since_last_engagement=idle,
    )


def _handle_detach(cur, artifact_id, process_id, time_id, utc_seconds):
    """Close current engagement with duration calculation."""
    duration = None
    attach_ts = get_attach_timestamp(cur, artifact_id, process_id)
    if attach_ts and utc_seconds:
        duration = utc_seconds - ts_str_to_unix(attach_ts)

    update_fact_artifact_detach(
        cur,
        artifact_id=artifact_id,
        process_id=process_id,
        time_detached_id=time_id,
        engagement_duration=duration,
    )