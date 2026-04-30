import json

from etl.utils.time_utils import normalize_to_seconds, build_dim_time_record, ts_str_to_unix
from etl.db.postgres_handler import (
    get_connection,
    upsert_dim_time,
    get_or_create_dim_stage,
    insert_new_dim_stage,
    insert_fact_stage_execution,
    update_fact_stage_close,
    get_last_close_timestamp,
    get_open_fact_key,
    check_and_update_dedup,
    check_fact_pk_exists,
    generate_combination_id,
    get_process_artifact_ids,
    upsert_bridge_artifact_stage,
    ensure_dim_process_exists,
    get_or_create_sentinel_time_id,
    get_stored_deviations,
    get_stage_is_leaf,
    resolve_parent_stage_id,
    resolve_pending_parent_ids,
    set_parent_id,
    recalculate_bridge_artifact_weights
)
from processors.process_deviations_processor import link_deviations_to_stages


def process_stage_event_record(record, event_name):
    process_name = record.get("PROCESS_NAME")
    stage_name = record.get("STAGE_NAME")
    compliance = record.get("STAGE_COMPLIANCE")
    stage_state = record.get("STAGE_STATE")
    ts_raw = record.get("TIMESTAMP")

    process_type, instance_part = process_name.split("/")
    process_id = instance_part.split("__")[0]

    ts_seconds = normalize_to_seconds(ts_raw)
    time_record = build_dim_time_record(ts_seconds)

    conn = get_connection()
    try:
        cur = conn.cursor()

        if not check_and_update_dedup(
            cur, process_name, stage_name, compliance, stage_state, ts_seconds
        ):
            conn.commit()
            return

        ensure_dim_process_exists(cur, process_id, process_type)

        if stage_state == "opened":
            time_id = upsert_dim_time(cur, time_record)
            _handle_open(cur, process_type, process_id, stage_name, time_id, ts_seconds)

        elif stage_state == "closed":
            time_id = upsert_dim_time(cur, time_record)
            _handle_close(cur, process_type, process_id, stage_name, time_id, ts_seconds)

        elif stage_state == "unopened":
            _handle_unopened(cur, process_type, process_id, stage_name)

        # backfill any NULL parent_ids that can now be resolved
        resolve_pending_parent_ids(cur, process_type, process_id)

        _relink_pending_deviations(cur, process_id)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# ------------------------------------------------------------------ #
#  PARENT RESOLUTION HELPER                                            #
# ------------------------------------------------------------------ #

def _resolve_and_set_parent(cur, process_type, process_id, stage_name, stage_id):
    parent_stage_id = resolve_parent_stage_id(cur, process_type, process_id, stage_name)
    if parent_stage_id is not None:
        set_parent_id(cur, stage_id, parent_stage_id)


# ------------------------------------------------------------------ #
#  OPENED                                                              #
# ------------------------------------------------------------------ #

def _handle_open(cur, process_type, process_id, stage_name, time_id, ts_seconds):
    is_leaf = get_stage_is_leaf(cur, process_type, stage_name)
    stage_id = get_or_create_dim_stage(cur, process_type, stage_name, "opened", is_leaf=is_leaf)

    if check_fact_pk_exists(cur, stage_id, process_id, time_id):
        stage_id = insert_new_dim_stage(cur, process_type, stage_name, "opened", is_leaf=is_leaf)

    waiting_time = None
    last_close_ts = get_last_close_timestamp(cur, process_id)
    if last_close_ts and ts_seconds:
        waiting_time = ts_seconds - ts_str_to_unix(last_close_ts)

    artifact_combination_id = None
    artifact_ids = get_process_artifact_ids(cur, process_id)
    if artifact_ids:
        artifact_combination_id = generate_combination_id(process_id, stage_id)
        for aid in artifact_ids:
            upsert_bridge_artifact_stage(cur, artifact_combination_id, aid, 1.0)  # placeholder

        # recalculate weights from actual DB count
        recalculate_bridge_artifact_weights(cur, artifact_combination_id)

    insert_fact_stage_execution(
        cur,
        stage_id=stage_id,
        process_id=process_id,
        time_open_id=time_id,
        time_close_id=None,
        artifact_combination_id=artifact_combination_id,
        deviation_combination_id=None,
        execution_time=None,
        waiting_time=waiting_time,
    )

    _resolve_and_set_parent(cur, process_type, process_id, stage_name, stage_id)


# ------------------------------------------------------------------ #
#  CLOSED                                                              #
# ------------------------------------------------------------------ #

def _handle_close(cur, process_type, process_id, stage_name, time_id, ts_seconds):
    open_stage_id, open_time_id, open_ts_str = get_open_fact_key(cur, process_id, stage_name)

    if not open_stage_id or not open_time_id:
        return  # no unclosed row to close

    is_leaf = get_stage_is_leaf(cur, process_type, stage_name)
    closed_stage_id = get_or_create_dim_stage(cur, process_type, stage_name, "closed", is_leaf=is_leaf)

    # PK conflict check: would (closed_stage_id, process_id, open_time_id) collide?
    if check_fact_pk_exists(cur, closed_stage_id, process_id, open_time_id):
        closed_stage_id = insert_new_dim_stage(cur, process_type, stage_name, "closed", is_leaf=is_leaf)

    execution_time = None
    if open_ts_str and ts_seconds:
        execution_time = ts_seconds - ts_str_to_unix(open_ts_str)

    update_fact_stage_close(
        cur, process_id,
        open_stage_id, open_time_id,
        closed_stage_id, time_id, execution_time,
    )

    _resolve_and_set_parent(cur, process_type, process_id, stage_name, closed_stage_id)


# ------------------------------------------------------------------ #
#  UNOPENED                                                            #
# ------------------------------------------------------------------ #

def _handle_unopened(cur, process_type, process_id, stage_name):
    is_leaf = get_stage_is_leaf(cur, process_type, stage_name)
    stage_id = insert_new_dim_stage(cur, process_type, stage_name, "unopened", is_leaf=is_leaf)

    sentinel_time_id = get_or_create_sentinel_time_id(cur)

    insert_fact_stage_execution(
        cur,
        stage_id=stage_id,
        process_id=process_id,
        time_open_id=sentinel_time_id,
        time_close_id=None,
        artifact_combination_id=None,
        deviation_combination_id=None,
        execution_time=None,
        waiting_time=None,
    )

    _resolve_and_set_parent(cur, process_type, process_id, stage_name, stage_id)


# ------------------------------------------------------------------ #
#  DEVIATION RE-LINKING                                                #
# ------------------------------------------------------------------ #

def _relink_pending_deviations(cur, process_id):
    stored = get_stored_deviations(cur, process_id)
    if not stored:
        return
    for _perspective, raw_json in stored:
        deviations = json.loads(raw_json)
        if deviations:
            link_deviations_to_stages(cur, process_id, deviations)