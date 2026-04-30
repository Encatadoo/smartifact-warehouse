from etl.db.postgres_handler import get_connection, upsert_dim_process


def process_process_instance_record(record, event_name):
    """
    Handle PROCESS_INSTANCE CDC record.
    
    Populates DIM_PROCESS with SCD Type 1 (overwrite status/outcome on MODIFY).
    """
    instance_id_raw = record.get("INSTANCE_ID")        # "p14__Truck"
    process_id = instance_id_raw.split("__")[0]          # "p14"

    process_type = record.get("PROCESS_TYPE_NAME")       # "LHR-AMS"
    status = record.get("STATUS")                        # "ongoing"
    outcome = record.get("OUTCOME")                      # "NA"

    stakeholders = record.get("STAKEHOLDERS", [])
    stakeholder_name = stakeholders[0] if stakeholders else None

    conn = get_connection()
    try:
        cur = conn.cursor()
        upsert_dim_process(cur, process_id, status, outcome, process_type, stakeholder_name)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()