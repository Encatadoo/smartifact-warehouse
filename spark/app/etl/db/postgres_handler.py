import hashlib
import psycopg2
from etl.config import POSTGRES_CONFIG


# ===================== CONNECTION =====================

def get_connection():
    """Get a new PostgreSQL connection."""
    return psycopg2.connect(**POSTGRES_CONFIG)


# ===================== DIM_TIME =====================

def upsert_dim_time(cur, time_record):
    """Insert DIM_TIME row if not exists. Returns time_id. Concurrent-safe."""
    if time_record is None:
        return None

    cur.execute(
        """INSERT INTO dim_time
               (full_timestamp, minute, hour, day, week, month, year)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (full_timestamp) DO NOTHING
           RETURNING time_id""",
        (
            time_record["full_timestamp"],
            time_record["minute"],
            time_record["hour"],
            time_record["day"],
            time_record["week"],
            time_record["month"],
            time_record["year"],
        ),
    )
    result = cur.fetchone()
    if result:
        return result[0]

    # another transaction inserted it first — fetch existing
    cur.execute(
        "SELECT time_id FROM dim_time WHERE full_timestamp = %s",
        (time_record["full_timestamp"],),
    )
    return cur.fetchone()[0]


# ===================== DIM_ARTIFACT =====================

def upsert_dim_artifact(cur, artifact_id, artifact_type):
    """Insert DIM_ARTIFACT if not exists."""
    cur.execute(
        """INSERT INTO dim_artifact (artifact_id, artifact_type)
           VALUES (%s, %s)
           ON CONFLICT (artifact_id) DO NOTHING""",
        (artifact_id, artifact_type),
    )


# ===================== DIM_PROCESS =====================

def upsert_dim_process(cur, process_id, status, outcome, process_type, stakeholder_name):
    """Insert or SCD-Type-1 update DIM_PROCESS."""
    cur.execute(
        """INSERT INTO dim_process 
               (process_id, status, outcome, process_type, stakeholder_name)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (process_id) DO UPDATE
               SET status = EXCLUDED.status,
                   outcome = EXCLUDED.outcome""",
        (process_id, status, outcome, process_type, stakeholder_name),
    )


# ===================== DIM_STAGE =====================

def get_or_create_dim_stage(cur, process_type, name, state, is_leaf=False):
    """
    Return existing stage_id for (process_type, name, state) or create new.
    Only used for 'opened' and 'closed' — 'unopened' uses insert_new_dim_stage.
    """
    cur.execute(
        """SELECT stage_id FROM dim_stage
           WHERE process_type = %s AND name = %s AND state = %s""",
        (process_type, name, state),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """INSERT INTO dim_stage (process_type, name, state, parent_id, is_leaf)
           VALUES (%s, %s, %s, NULL, %s)
           RETURNING stage_id""",
        (process_type, name, state, is_leaf),
    )
    return cur.fetchone()[0]

def insert_new_dim_stage(cur, process_type, name, state, is_leaf=False):
    """
    Always insert a new DIM_STAGE row (new stage_id).
    Used for 'unopened' stages where each occurrence needs its own identity.
    """
    cur.execute(
        """INSERT INTO dim_stage (process_type, name, state, parent_id, is_leaf)
           VALUES (%s, %s, %s, NULL, %s)
           RETURNING stage_id""",
        (process_type, name, state, is_leaf),
    )
    return cur.fetchone()[0]

def check_fact_pk_exists(cur, stage_id, process_id, time_open_id):
    """Check if a fact_stage_execution row with this PK already exists."""
    cur.execute(
        """SELECT 1 FROM fact_stage_execution
           WHERE stage_id = %s AND process_id = %s AND time_open_id = %s""",
        (stage_id, process_id, time_open_id),
    )
    return cur.fetchone() is not None


def resolve_pending_parent_ids(cur, process_type, process_id):
    """
    Re-resolve parent_id for all DIM_STAGE rows used by this process
    where parent_id is still NULL and the hierarchy says there IS a parent.
    
    Called after each stage event to fill gaps when parents
    arrive after their children.
    """
    # find stages with NULL parent_id that should have a parent
    cur.execute(
        """SELECT DISTINCT ds.stage_id, ds.name
           FROM fact_stage_execution fse
           JOIN dim_stage ds ON fse.stage_id = ds.stage_id
           WHERE fse.process_id = %s
             AND ds.process_type = %s
             AND ds.parent_id IS NULL""",
        (process_id, process_type),
    )
    rows = cur.fetchall()

    for stage_id, stage_name in rows:
        # check if hierarchy says this stage has a parent
        parent_stage_id = resolve_parent_stage_id(cur, process_type, process_id, stage_name)
        if parent_stage_id is not None:
            set_parent_id(cur, stage_id, parent_stage_id)


# ===================== PARENT_ID RESOLUTION =====================

def resolve_parent_stage_id(cur, process_type, process_id, stage_name):
    """
    Resolve parent's stage_id from:
      1. egsm_hierarchy_cache → parent_stage_name (within process_type)
      2. fact_stage_execution → parent's stage_id (within process_type AND process_id)
    """
    cur.execute(
        """SELECT parent_stage_name FROM egsm_hierarchy_cache
           WHERE process_type = %s AND stage_name = %s""",
        (process_type, stage_name),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return None

    parent_stage_name = row[0]

    cur.execute(
        """SELECT fse.stage_id
           FROM fact_stage_execution fse
           JOIN dim_stage ds ON fse.stage_id = ds.stage_id
           WHERE fse.process_id = %s
             AND ds.name = %s
             AND ds.process_type = %s
           ORDER BY fse.time_open_id DESC
           LIMIT 1""",
        (process_id, parent_stage_name, process_type),
    )
    row = cur.fetchone()
    return row[0] if row else None


def set_parent_id(cur, stage_id, parent_id):
    """Update parent_id on a DIM_STAGE row."""
    cur.execute(
        "UPDATE dim_stage SET parent_id = %s WHERE stage_id = %s",
        (parent_id, stage_id),
    )


def get_open_fact_key(cur, process_id, stage_name):
    """
    Find the full PK (stage_id, time_open_id) and the open timestamp
    of the most recent unclosed fact row for this stage in this process.
    
    Returns (stage_id, time_open_id, full_timestamp) or (None, None, None).
    """
    cur.execute(
        """SELECT fse.stage_id, fse.time_open_id, dt.full_timestamp
           FROM fact_stage_execution fse
           JOIN dim_stage ds ON fse.stage_id = ds.stage_id
           JOIN dim_time dt ON fse.time_open_id = dt.time_id
           WHERE fse.process_id = %s
             AND ds.name = %s
             AND ds.state = 'opened'
             AND fse.time_close_id IS NULL
           ORDER BY fse.time_open_id DESC
           LIMIT 1""",
        (process_id, stage_name),
    )
    row = cur.fetchone()
    return (row[0], row[1], row[2]) if row else (None, None, None)

def get_stage_is_leaf(cur, process_type, stage_name):
    """Look up is_leaf from the EGSM hierarchy cache."""
    cur.execute(
        "SELECT is_leaf FROM egsm_hierarchy_cache WHERE process_type = %s AND stage_name = %s",
        (process_type, stage_name),
    )
    row = cur.fetchone()
    return row[0] if row is not None else False

def resolve_parent_id(cur, process_type, process_id, stage_name):
    """
    At DIM_STAGE insert time, check if the parent stage has already closed
    for this process instance. If so, return its closed stage_id immediately.
    
    This handles the case where a child arrives AFTER its parent has closed
    (update_children_parent_id already ran and won't revisit this child).
    
    Returns:
        int (parent's closed stage_id) or None (parent hasn't closed yet / root stage)
    """
    # look up parent name from hierarchy cache
    cur.execute(
        """SELECT parent_stage_name FROM egsm_hierarchy_cache
           WHERE process_type = %s AND stage_name = %s""",
        (process_type, stage_name),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None  # root stage or hierarchy not cached

    parent_name = row[0]

    # find the most recent closed stage_id for the parent in this process
    cur.execute(
        """SELECT ds.stage_id
           FROM dim_stage ds
           JOIN fact_stage_execution fse ON ds.stage_id = fse.stage_id
           WHERE ds.process_type = %s
             AND ds.name = %s
             AND ds.state = 'closed'
             AND fse.process_id = %s
           ORDER BY ds.stage_id DESC
           LIMIT 1""",
        (process_type, parent_name, process_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


# ===================== DIM_DEVIATION =====================

def get_or_create_dim_deviation(cur, type_name):
    """
    Return the deviation_id for a given type_name.
    Creates a new row only if the type doesn't exist yet.
    """
    cur.execute(
        """INSERT INTO dim_deviation (type_name)
           VALUES (%s)
           ON CONFLICT (type_name) DO NOTHING""",
        (type_name,),
    )
    cur.execute(
        "SELECT deviation_id FROM dim_deviation WHERE type_name = %s",
        (type_name,),
    )
    return cur.fetchone()[0]


# ===================== FACT_STAGE_EXECUTION =====================

def insert_fact_stage_execution(cur, stage_id, process_id, time_open_id,
                                time_close_id=None, artifact_combination_id=None,
                                deviation_combination_id=None,
                                execution_time=None, waiting_time=None):
    """Insert a new FACT_STAGE_EXECUTION row."""
    cur.execute(
        """INSERT INTO fact_stage_execution
               (stage_id, process_id, time_open_id, time_close_id,
                artifact_combination_id, deviation_combination_id,
                execution_time, waiting_time)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (stage_id, process_id, time_open_id, time_close_id,
         artifact_combination_id, deviation_combination_id,
         execution_time, waiting_time),
    )


def update_fact_stage_close(cur, process_id, open_stage_id, open_time_id,
                            closed_stage_id, time_close_id, execution_time):
    """
    Close exactly ONE fact row identified by its full PK
    (open_stage_id, process_id, open_time_id).
    """
    cur.execute(
        """UPDATE fact_stage_execution
           SET stage_id = %s,
               time_close_id = %s,
               execution_time = %s
           WHERE stage_id = %s
             AND process_id = %s
             AND time_open_id = %s""",
        (closed_stage_id, time_close_id, execution_time,
         open_stage_id, process_id, open_time_id),
    )


def get_last_close_timestamp(cur, process_id):
    """Return full_timestamp of the most recently closed stage in a process."""
    cur.execute(
        """SELECT dt.full_timestamp
           FROM fact_stage_execution fse
           JOIN dim_time dt ON fse.time_close_id = dt.time_id
           WHERE fse.process_id = %s
             AND fse.time_close_id IS NOT NULL
           ORDER BY dt.full_timestamp DESC
           LIMIT 1""",
        (process_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_stage_ids_by_name_and_process(cur, process_id, stage_name):
    """Find all current stage_ids for a stage name within a process (for deviation linking)."""
    cur.execute(
        """SELECT DISTINCT fse.stage_id
           FROM fact_stage_execution fse
           JOIN dim_stage ds ON fse.stage_id = ds.stage_id
           WHERE fse.process_id = %s AND ds.name = %s""",
        (process_id, stage_name),
    )
    return [r[0] for r in cur.fetchall()]


def update_fact_deviation_combination(cur, process_id, stage_id, deviation_combination_id):
    """Set deviation_combination_id on a FACT_STAGE_EXECUTION row."""
    cur.execute(
        """UPDATE fact_stage_execution
           SET deviation_combination_id = %s
           WHERE process_id = %s AND stage_id = %s""",
        (deviation_combination_id, process_id, stage_id),
    )

def ensure_dim_process_exists(cur, process_id, process_type=None):
    """
    Create a minimal DIM_PROCESS placeholder if it doesn't exist yet.
    The real status/outcome/stakeholder will be filled by SCD-1 update
    when PROCESS_INSTANCE arrives.
    """
    cur.execute(
        """INSERT INTO dim_process 
               (process_id, status, outcome, process_type, stakeholder_name)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (process_id) DO NOTHING""",
        (process_id, "unknown", "unknown", process_type, None),
    )

# ===================== DEVIATION RAW STORAGE =====================

def store_raw_deviations(cur, process_id, perspective, deviations_hash, raw_json):
    """Store the full deviations JSON array for later re-linking."""
    cur.execute(
        """INSERT INTO deviation_process_tracker
               (process_id, perspective, last_deviations_hash, raw_deviations)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (process_id, perspective)
               DO UPDATE SET last_deviations_hash = EXCLUDED.last_deviations_hash,
                             raw_deviations = EXCLUDED.raw_deviations""",
        (process_id, perspective, deviations_hash, raw_json),
    )


def get_stored_deviations(cur, process_id):
    """Retrieve all cached deviation arrays for a process (across perspectives)."""
    cur.execute(
        """SELECT perspective, raw_deviations
           FROM deviation_process_tracker
           WHERE process_id = %s AND raw_deviations IS NOT NULL""",
        (process_id,),
    )
    return cur.fetchall()


def get_deviations_hash(cur, process_id, perspective):
    """Get the stored hash for idempotency check."""
    cur.execute(
        """SELECT last_deviations_hash FROM deviation_process_tracker
           WHERE process_id = %s AND perspective = %s""",
        (process_id, perspective),
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_old_deviation_combination_id(cur, process_id, stage_id):
    """Get the current deviation_combination_id from a fact row (for cleanup)."""
    cur.execute(
        """SELECT deviation_combination_id FROM fact_stage_execution
           WHERE process_id = %s AND stage_id = %s""",
        (process_id, stage_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


# ===================== FACT_ARTIFACT_ENGAGEMENT =====================

def upsert_fact_artifact_attach(cur, artifact_id, process_id,
                                time_attached_id,
                                idle_since_last_engagement=None):
    """
    Handle an artifact attach event.
    
    First attach  → INSERT new row.
    Re-attach     → UPDATE existing row (new attached time, clear detach fields,
                    record idle_since_last_engagement from previous detach).
    """
    cur.execute(
        """INSERT INTO fact_artifact_engagement
               (artifact_id, process_id, time_attached_id,
                time_detached_id, engagement_duration,
                idle_since_last_engagement)
           VALUES (%s, %s, %s, NULL, NULL, %s)
           ON CONFLICT (artifact_id, process_id)
               DO UPDATE SET time_attached_id = EXCLUDED.time_attached_id,
                             time_detached_id = NULL,
                             engagement_duration = NULL,
                             idle_since_last_engagement = EXCLUDED.idle_since_last_engagement""",
        (artifact_id, process_id, time_attached_id, idle_since_last_engagement),
    )


def update_fact_artifact_detach(cur, artifact_id, process_id,
                                time_detached_id, engagement_duration):
    """Close the current engagement (only if currently attached)."""
    cur.execute(
        """UPDATE fact_artifact_engagement
           SET time_detached_id = %s,
               engagement_duration = %s
           WHERE artifact_id = %s
             AND process_id = %s
             AND time_detached_id IS NULL""",
        (time_detached_id, engagement_duration, artifact_id, process_id),
    )


def get_last_detach_timestamp(cur, artifact_id, process_id):
    """Return full_timestamp of the current detach (for idle calculation on re-attach)."""
    cur.execute(
        """SELECT dt.full_timestamp
           FROM fact_artifact_engagement fae
           JOIN dim_time dt ON fae.time_detached_id = dt.time_id
           WHERE fae.artifact_id = %s
             AND fae.process_id = %s
             AND fae.time_detached_id IS NOT NULL""",
        (artifact_id, process_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_attach_timestamp(cur, artifact_id, process_id):
    """Return full_timestamp of the current open engagement (for duration calculation)."""
    cur.execute(
        """SELECT dt.full_timestamp
           FROM fact_artifact_engagement fae
           JOIN dim_time dt ON fae.time_attached_id = dt.time_id
           WHERE fae.artifact_id = %s
             AND fae.process_id = %s
             AND fae.time_detached_id IS NULL""",
        (artifact_id, process_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


# ===================== BRIDGE_ARTIFACT_STAGE =====================

def generate_combination_id(process_id, stage_id):
    """Deterministic hash of (process_id, stage_id)."""
    raw = f"{process_id}_{stage_id}"
    return hashlib.md5(raw.encode()).hexdigest()


def get_process_artifact_ids(cur, process_id):
    """Return all distinct artifact_ids ever engaged in a process."""
    cur.execute(
        "SELECT DISTINCT artifact_id FROM fact_artifact_engagement WHERE process_id = %s",
        (process_id,),
    )
    return [r[0] for r in cur.fetchall()]


def upsert_bridge_artifact_stage(cur, artifact_combination_id, artifact_id, weight):
    """Insert or update a BRIDGE_ARTIFACT_STAGE row."""
    cur.execute(
        """INSERT INTO bridge_artifact_stage
               (artifact_combination_id, artifact_id, weight)
           VALUES (%s, %s, %s)
           ON CONFLICT (artifact_combination_id, artifact_id)
               DO UPDATE SET weight = EXCLUDED.weight""",
        (artifact_combination_id, artifact_id, weight),
    )


# ===================== BRIDGE_STAGE_DEVIATION =====================

def upsert_bridge_stage_deviation(cur, deviation_combination_id,
                                  deviation_id, weight, iteration_index):
    """Insert or update a BRIDGE_STAGE_DEVIATION row."""
    cur.execute(
        """INSERT INTO bridge_stage_deviation
               (deviation_combination_id, deviation_id, weight, iteration_index)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (deviation_combination_id, deviation_id)
               DO UPDATE SET weight = EXCLUDED.weight,
                             iteration_index = EXCLUDED.iteration_index""",
        (deviation_combination_id, deviation_id, weight, iteration_index),
    )


def delete_bridge_deviations_for_combination(cur, deviation_combination_id):
    """Remove all bridge rows for a given combination (before re-creating)."""
    cur.execute(
        "DELETE FROM bridge_stage_deviation WHERE deviation_combination_id = %s",
        (deviation_combination_id,),
    )


# ===================== EGSM HIERARCHY CACHE =====================

def store_hierarchy(cur, process_type, hierarchy):
    """Persist parsed EGSM hierarchy (with leaf info) to the cache table."""
    for stage_name, info in hierarchy.items():
        cur.execute(
            """INSERT INTO egsm_hierarchy_cache
                   (process_type, stage_name, parent_stage_name, is_leaf)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (process_type, stage_name)
                   DO UPDATE SET parent_stage_name = EXCLUDED.parent_stage_name,
                                 is_leaf = EXCLUDED.is_leaf""",
            (process_type, stage_name, info["parent"], info["is_leaf"]),
        )


# ===================== STAGE EVENT DEDUPLICATION =====================

def check_and_update_dedup(cur, process_name, stage_name, compliance, state, timestamp):
    """
    Timestamp-aware deduplication.
    
    Rules:
      1. First event for this (process_name, stage_name): always process
      2. Late arrival (timestamp < last processed): skip
      3. No change in (compliance, state): skip
      4. Otherwise: process and update tracker
    """
    cur.execute(
        """SELECT last_compliance, last_state, last_timestamp
           FROM stage_event_dedup_tracker
           WHERE process_name = %s AND stage_name = %s""",
        (process_name, stage_name),
    )
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """INSERT INTO stage_event_dedup_tracker
                   (process_name, stage_name, last_compliance, last_state, last_timestamp)
               VALUES (%s, %s, %s, %s, %s)""",
            (process_name, stage_name, compliance, state, timestamp),
        )
        return True

    last_compliance, last_state, last_timestamp = row

    # skip late arrivals
    if (timestamp is not None and last_timestamp is not None
            and timestamp < last_timestamp):
        return False

    # skip if no change
    if last_compliance == compliance and last_state == state:
        return False

    cur.execute(
        """UPDATE stage_event_dedup_tracker
           SET last_compliance = %s, last_state = %s, last_timestamp = %s
           WHERE process_name = %s AND stage_name = %s""",
        (compliance, state, timestamp, process_name, stage_name),
    )
    return True

# ===================== SENTINEL TIME =====================

def get_or_create_sentinel_time_id(cur):
    """
    Return the time_id for the sentinel timestamp (1970-01-01 00:00:00).
    Creates the DIM_TIME row if it doesn't exist yet.
    """
    from utils.time_utils import build_sentinel_time_record
    sentinel_record = build_sentinel_time_record()
    return upsert_dim_time(cur, sentinel_record)

# ===================== BRIDGE WEIGHT RECALCULATION =====================

def recalculate_bridge_deviation_weights(cur, deviation_combination_id):
    """Set weight = 1/n for all rows in this deviation combination."""
    cur.execute(
        """UPDATE bridge_stage_deviation bsd
           SET weight = 1.0 / sub.cnt
           FROM (
               SELECT COUNT(*) AS cnt
               FROM bridge_stage_deviation
               WHERE deviation_combination_id = %s
           ) sub
           WHERE bsd.deviation_combination_id = %s""",
        (deviation_combination_id, deviation_combination_id),
    )


def recalculate_bridge_artifact_weights(cur, artifact_combination_id):
    """Set weight = 1/n for all rows in this artifact combination."""
    cur.execute(
        """UPDATE bridge_artifact_stage bas
           SET weight = 1.0 / sub.cnt
           FROM (
               SELECT COUNT(*) AS cnt
               FROM bridge_artifact_stage
               WHERE artifact_combination_id = %s
           ) sub
           WHERE bas.artifact_combination_id = %s""",
        (artifact_combination_id, artifact_combination_id),
    )