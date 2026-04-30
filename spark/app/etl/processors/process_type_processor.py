import json

from etl.utils.egsm_parser import parse_egsm_hierarchy
from etl.db.postgres_handler import get_connection, store_hierarchy


def process_process_type_record(record, event_name):
    """
    Handle PROCESS_TYPE CDC record.
    
    PROCESS_INFO arrives as a JSON-encoded string (DynamoDB S type),
    not a Map. We parse it with strict=False to tolerate \r\n
    control characters embedded in the EGSM XML.
    """
    process_type_name = record.get("PROCESS_TYPE_NAME")
    process_info_raw = record.get("PROCESS_INFO")

    # ---- parse PROCESS_INFO string → dict ----
    process_info = _parse_process_info(process_info_raw)
    if not process_info:
        return

    perspectives = process_info.get("perspectives", [])
    if not perspectives:
        return

    conn = get_connection()
    try:
        cur = conn.cursor()

        for perspective in perspectives:
            egsm_xml = perspective.get("egsm_model")
            if not egsm_xml:
                continue

            # \r\n are now actual control chars in the string —
            # ElementTree handles them fine
            hierarchy = parse_egsm_hierarchy(egsm_xml)
            store_hierarchy(cur, process_type_name, hierarchy)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def _parse_process_info(process_info_raw):
    """
    Parse PROCESS_INFO from its raw form.
    
    Cases:
      - str  → JSON-encoded string from DynamoDB S type, needs json.loads
      - dict → already parsed (e.g., came through M type), use as-is
      - None → return empty dict
    """
    if process_info_raw is None:
        return {}

    if isinstance(process_info_raw, dict):
        # already a dict (came through DynamoDB M type or pre-parsed)
        return process_info_raw

    if isinstance(process_info_raw, str):
        return json.loads(process_info_raw, strict=False)