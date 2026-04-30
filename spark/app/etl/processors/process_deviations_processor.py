import hashlib
import json

from db.postgres_handler import (
    get_connection,
    get_or_create_dim_deviation,
    upsert_bridge_stage_deviation,
    delete_bridge_deviations_for_combination,
    get_stage_ids_by_name_and_process,
    generate_combination_id,
    update_fact_deviation_combination,
    get_old_deviation_combination_id,
    store_raw_deviations,
    get_deviations_hash,
    recalculate_bridge_deviation_weights
)


def _parse_deviations(raw_deviations):
    """
    Normalize DEVIATIONS from its raw form.
    
    Three possible shapes after DynamoDB parsing:
      1. str  → entire array is a JSON string: '[{"type":"OVERLAP",...}]'
      2. list of str → each element is a JSON string: ['{"type":"OVERLAP",...}', ...]
      3. list of dict → already parsed: [{"type":"OVERLAP",...}, ...]
    """
    if not raw_deviations:
        return []

    # Level 1: entire field may be a JSON string
    if isinstance(raw_deviations, str):
        raw_deviations = json.loads(raw_deviations, strict=False)

    # Level 2: individual elements may still be JSON strings
    return [
        json.loads(d, strict=False) if isinstance(d, str) else d
        for d in raw_deviations
    ]


def process_process_deviations_record(record, event_name):
    """
    Handle PROCESS_DEVIATIONS CDC record.
    """
    instance_id = record.get("INSTANCE_ID")
    process_id = instance_id
    process_type = record.get("PROCESS_TYPE")
    perspective = record.get("PERSPECTIVE")

    # ---- normalize: parse string elements to dicts ----
    deviations = _parse_deviations(record.get("DEVIATIONS", []))

    # hash and store the normalized version
    raw_json = json.dumps(deviations, sort_keys=True)
    current_hash = hashlib.md5(raw_json.encode()).hexdigest()

    conn = get_connection()
    try:
        cur = conn.cursor()

        existing_hash = get_deviations_hash(cur, process_id, perspective)
        if existing_hash == current_hash:
            conn.commit()
            return

        # store normalized JSON — _relink_pending_deviations will
        # json.loads() this and get dicts directly, no double-parsing
        store_raw_deviations(cur, process_id, perspective, current_hash, raw_json)

        if deviations:
            link_deviations_to_stages(cur, process_id, deviations)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def link_deviations_to_stages(cur, process_id, deviations):
    combo_stage_map = {}

    for deviation in deviations:
        dev_type = deviation.get("type")
        block_a = deviation.get("block_a", [])
        block_b = deviation.get("block_b")
        iteration_index = deviation.get("iterationIndex")

        involved_names = list(block_a) if isinstance(block_a, list) else [block_a]
        if block_b:
            involved_names.append(block_b)

        for stage_name in involved_names:
            stage_ids = get_stage_ids_by_name_and_process(cur, process_id, stage_name)
            for sid in stage_ids:
                combo_id = generate_combination_id(process_id, sid)
                if combo_id not in combo_stage_map:
                    combo_stage_map[combo_id] = {"stage_id": sid, "deviations": []}
                combo_stage_map[combo_id]["deviations"].append(
                    (dev_type, iteration_index)
                )

    if not combo_stage_map:
        return

    for combo_id, info in combo_stage_map.items():
        old_combo = get_old_deviation_combination_id(cur, process_id, info["stage_id"])
        if old_combo and old_combo != combo_id:
            delete_bridge_deviations_for_combination(cur, old_combo)
        delete_bridge_deviations_for_combination(cur, combo_id)

        for dev_type, iteration_index in info["deviations"]:
            deviation_id = get_or_create_dim_deviation(cur, dev_type)
            upsert_bridge_stage_deviation(
                cur, combo_id, deviation_id, 1.0, iteration_index,  # placeholder weight
            )

        # recalculate weights from actual DB count
        recalculate_bridge_deviation_weights(cur, combo_id)

        update_fact_deviation_combination(cur, process_id, info["stage_id"], combo_id)