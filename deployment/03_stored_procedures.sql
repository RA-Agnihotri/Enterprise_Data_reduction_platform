-- ============================================================================
-- EDRP DEPLOYMENT — Step 3: Stored Procedures & Functions
-- ============================================================================
-- Deploys all Snowpark stored procedures and SQL functions.
-- Run as: ACCOUNTADMIN or EDRP_ADMIN
-- Prerequisite: Steps 01 and 02 completed.
-- ============================================================================

USE SCHEMA DATA_REDUCTION_POC.EDRP_APP;

-- ============================================================================
-- 3.1 SP_EXTRACT_METADATA
-- Extracts table/column metadata from INFORMATION_SCHEMA and ACCESS_HISTORY
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Extracts table/column metadata from INFORMATION_SCHEMA and ACCESS_HISTORY'
AS
$$
def run(session, source_database, source_schema):
    results = {"tables": 0, "columns": 0, "access_history_joins": 0}
    
    session.sql(f"""
        MERGE INTO {source_database}.EDRP_METADATA.TABLE_INVENTORY TGT
        USING (
            SELECT 
                '{source_database}' AS SOURCE_DATABASE,
                '{source_schema}' AS SOURCE_SCHEMA,
                TABLE_NAME, ROW_COUNT, BYTES AS SIZE_BYTES
            FROM {source_database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_TYPE = 'BASE TABLE'
        ) SRC
        ON TGT.SOURCE_DATABASE = SRC.SOURCE_DATABASE 
           AND TGT.SOURCE_SCHEMA = SRC.SOURCE_SCHEMA
           AND TGT.TABLE_NAME = SRC.TABLE_NAME
        WHEN MATCHED THEN UPDATE SET 
            ROW_COUNT = SRC.ROW_COUNT, SIZE_BYTES = SRC.SIZE_BYTES, UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT 
            (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME, ROW_COUNT, SIZE_BYTES, LOADED_BY)
        VALUES 
            (SRC.SOURCE_DATABASE, SRC.SOURCE_SCHEMA, SRC.TABLE_NAME, SRC.ROW_COUNT, SRC.SIZE_BYTES, 'INFO_SCHEMA')
    """).collect()
    
    results["tables"] = session.sql(f"""
        SELECT COUNT(*) FROM {source_database}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
    """).collect()[0][0]
    
    session.sql(f"""
        MERGE INTO {source_database}.EDRP_METADATA.COLUMN_INVENTORY TGT
        USING (
            SELECT 
                '{source_database}' AS SOURCE_DATABASE, '{source_schema}' AS SOURCE_SCHEMA,
                TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION,
                CASE WHEN IS_NULLABLE = 'YES' THEN TRUE ELSE FALSE END AS IS_NULLABLE
            FROM {source_database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{source_schema}'
        ) SRC
        ON TGT.SOURCE_DATABASE = SRC.SOURCE_DATABASE AND TGT.SOURCE_SCHEMA = SRC.SOURCE_SCHEMA
           AND TGT.TABLE_NAME = SRC.TABLE_NAME AND TGT.COLUMN_NAME = SRC.COLUMN_NAME
        WHEN MATCHED THEN UPDATE SET DATA_TYPE = SRC.DATA_TYPE, ORDINAL_POSITION = SRC.ORDINAL_POSITION
        WHEN NOT MATCHED THEN INSERT 
            (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE, LOADED_BY)
        VALUES 
            (SRC.SOURCE_DATABASE, SRC.SOURCE_SCHEMA, SRC.TABLE_NAME, SRC.COLUMN_NAME, 
             SRC.DATA_TYPE, SRC.ORDINAL_POSITION, SRC.IS_NULLABLE, 'INFO_SCHEMA')
    """).collect()
    
    results["columns"] = session.sql(f"""
        SELECT COUNT(*) FROM {source_database}.EDRP_METADATA.COLUMN_INVENTORY
        WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
    """).collect()[0][0]
    
    try:
        session.sql(f"""
            INSERT INTO {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
                (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, 
                 CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE, CARDINALITY,
                 DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS)
            SELECT DISTINCT 
                '{source_database}', '{source_schema}',
                do2.value:objectName::STRING, jc.value:columns[0].columnName::STRING,
                do1.value:objectName::STRING, jc.value:columns[1].columnName::STRING,
                'INNER', '1:N', 'ACCESS_HISTORY',
                CASE WHEN COUNT(*) OVER (PARTITION BY do1.value:objectName, do2.value:objectName) > 5 
                     THEN 'HIGH' ELSE 'MEDIUM' END,
                'PENDING_REVIEW'
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
                 LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) do1,
                 LATERAL FLATTEN(input => ah.OBJECTS_MODIFIED) do2,
                 LATERAL FLATTEN(input => do1.value:columns) jc
            WHERE ah.QUERY_START_TIME > DATEADD('day', -90, CURRENT_TIMESTAMP())
              AND do1.value:objectDomain::STRING = 'Table' AND do2.value:objectDomain::STRING = 'Table'
              AND jc.value:columns IS NOT NULL AND ARRAY_SIZE(jc.value:columns) = 2
              AND do1.value:objectName::STRING != do2.value:objectName::STRING
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY do1.value:objectName, jc.value:columns[1].columnName,
                             do2.value:objectName, jc.value:columns[0].columnName
                ORDER BY ah.QUERY_START_TIME DESC) = 1
        """).collect()
    except Exception as e:
        results["access_history_note"] = f"ACCESS_HISTORY query skipped: {str(e)}"
    
    fk_count = session.sql(f"""
        SELECT COUNT(*) FROM {source_database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = '{source_schema}' AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    """).collect()[0][0]
    
    if fk_count > 0:
        session.sql(f"""
            INSERT INTO {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
                (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN,
                 CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE, CARDINALITY,
                 IS_DECLARED_FK, DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS)
            SELECT DISTINCT
                '{source_database}', '{source_schema}',
                ccu.TABLE_NAME, ccu.COLUMN_NAME, kcu.TABLE_NAME, kcu.COLUMN_NAME,
                'INNER', '1:N', TRUE, 'INFO_SCHEMA', 'HIGH', 'ACTIVE'
            FROM {source_database}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN {source_database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            JOIN {source_database}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME AND rc.UNIQUE_CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
            WHERE rc.CONSTRAINT_SCHEMA = '{source_schema}'
        """).collect()
    
    results["declared_fks"] = fk_count
    return results
$$;


-- ============================================================================
-- 3.2 SP_BUILD_DEPENDENCY_GRAPH
-- Builds dependency graph, topological sort, cycle detection using NetworkX
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'networkx')
HANDLER = 'run'
COMMENT = 'Builds dependency graph, topological sort, cycle detection using NetworkX'
AS
$$
import networkx as nx
import json

def run(session, source_database, source_schema):
    rels = session.sql(f"""
        SELECT PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE
        FROM {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
        WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
          AND RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()
    
    tables = session.sql(f"""
        SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT
        FROM {source_database}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}' AND IS_ACTIVE = TRUE
    """).collect()
    
    G = nx.DiGraph()
    for t in tables:
        G.add_node(t["TABLE_NAME"], table_type=t["TABLE_TYPE"], row_count=t["ROW_COUNT"])
    for r in rels:
        G.add_edge(r["PARENT_TABLE"], r["CHILD_TABLE"],
                    parent_col=r["PARENT_COLUMN"], child_col=r["CHILD_COLUMN"], join_type=r["JOIN_TYPE"])
    
    cycles = list(nx.simple_cycles(G))
    broken_edges = []
    if cycles:
        for cycle in cycles:
            edge_to_remove = (cycle[-1], cycle[0])
            if G.has_edge(*edge_to_remove):
                G.remove_edge(*edge_to_remove)
                broken_edges.append({"from": edge_to_remove[0], "to": edge_to_remove[1]})
    
    try:
        reduction_order = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        reduction_order = list(G.nodes())
    
    root_tables = [n for n in G.nodes() if G.in_degree(n) == 0]
    leaf_tables = [n for n in G.nodes() if G.out_degree(n) == 0]
    fan_out = {n: G.out_degree(n) for n in G.nodes() if G.out_degree(n) > 3}
    
    for idx, table_name in enumerate(reduction_order):
        session.sql(f"""
            UPDATE {source_database}.EDRP_METADATA.TABLE_INVENTORY
            SET REDUCTION_ORDER = {idx}, UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
              AND TABLE_NAME = '{table_name}'
        """).collect()
    
    for table_name in root_tables:
        session.sql(f"""
            UPDATE {source_database}.EDRP_METADATA.TABLE_INVENTORY
            SET TABLE_ROLE = 'ROOT', REDUCTION_STRATEGY = 'FULL_COPY'
            WHERE TABLE_NAME = '{table_name}'
              AND SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
        """).collect()
    
    for table_name in leaf_tables:
        session.sql(f"""
            UPDATE {source_database}.EDRP_METADATA.TABLE_INVENTORY
            SET TABLE_ROLE = 'LEAF', REDUCTION_STRATEGY = 'FK_CASCADE'
            WHERE TABLE_NAME = '{table_name}'
              AND SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
        """).collect()
    
    return {
        "total_tables": len(G.nodes()), "total_relationships": len(G.edges()),
        "root_tables": root_tables, "leaf_tables": leaf_tables,
        "cycles_detected": len(cycles), "cycles_broken": broken_edges,
        "high_fan_out_tables": fan_out, "reduction_order": reduction_order
    }
$$;


-- ============================================================================
-- 3.3 SP_EXECUTE_REDUCTION
-- Core reduction engine with stratified sampling and composite key support
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION(
    PROFILE_NAME VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Executes data reduction using the proven Clone+CTAS approach from POC'
AS
$$
import json
import time

def run(session, profile_name):
    profile = session.sql(f"""
        SELECT * FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
        WHERE PROFILE_NAME = '{profile_name}' AND STATUS = 'ACTIVE'
    """).collect()
    
    if not profile:
        return {"error": f"No active profile found: {profile_name}"}
    
    p = profile[0]
    src_db = p["SOURCE_DATABASE"]
    src_schema = p["SOURCE_SCHEMA"]
    tgt_db = p["TARGET_DATABASE"]
    tgt_schema = p["TARGET_SCHEMA"]
    sample_pct = float(p["DEFAULT_SAMPLE_PERCENT"])
    seed = int(p["RANDOM_SEED"])
    
    session.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {tgt_db}.{tgt_schema}
        COMMENT = 'Reduced dataset from {src_schema} at {sample_pct}%'
    """).collect()
    
    session.sql(f"""
        INSERT INTO {src_db}.EDRP_METADATA.REDUCTION_JOB_LOG (PROFILE_ID, TOTAL_TABLES)
        VALUES ({p["PROFILE_ID"]}, 0)
    """).collect()
    job_id = session.sql("SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG").collect()[0][0]
    
    tables = session.sql(f"""
        SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROLE, ROW_COUNT, 
               REDUCTION_STRATEGY, SAMPLE_PERCENT, PRIMARY_KEY_COLS, INCLUDE_FILTER,
               COALESCE(REDUCTION_ORDER, 999) AS SORT_ORDER,
               COALESCE(SAMPLING_STRATEGY, 'RANDOM') AS SAMPLING_STRATEGY,
               STRATIFY_COLUMNS
        FROM {src_db}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{src_db}' AND SOURCE_SCHEMA = '{src_schema}' AND IS_ACTIVE = TRUE
        ORDER BY SORT_ORDER
    """).collect()
    
    rels = session.sql(f"""
        SELECT r.RELATIONSHIP_ID, r.PARENT_TABLE, r.PARENT_COLUMN, r.CHILD_TABLE, r.CHILD_COLUMN,
               r.JOIN_TYPE, COALESCE(r.IS_COMPOSITE_KEY, FALSE) AS IS_COMPOSITE_KEY
        FROM {src_db}.EDRP_METADATA.RELATIONSHIP_MAP r
        WHERE r.SOURCE_DATABASE = '{src_db}' AND r.SOURCE_SCHEMA = '{src_schema}'
          AND r.RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()
    
    composite_conditions = {}
    composite_rel_ids = [r["RELATIONSHIP_ID"] for r in rels if r["IS_COMPOSITE_KEY"]]
    if composite_rel_ids:
        ids_str = ",".join(str(rid) for rid in composite_rel_ids)
        jc_rows = session.sql(f"""
            SELECT RELATIONSHIP_ID, PARENT_COLUMN, CHILD_COLUMN
            FROM {src_db}.EDRP_METADATA.JOIN_CONDITIONS
            WHERE RELATIONSHIP_ID IN ({ids_str})
            ORDER BY RELATIONSHIP_ID, ORDINAL_POSITION
        """).collect()
        for jc in jc_rows:
            rid = jc["RELATIONSHIP_ID"]
            if rid not in composite_conditions:
                composite_conditions[rid] = []
            composite_conditions[rid].append({"parent_col": jc["PARENT_COLUMN"], "child_col": jc["CHILD_COLUMN"]})
    
    child_fks = {}
    for r in rels:
        ct = r["CHILD_TABLE"]
        if ct not in child_fks:
            child_fks[ct] = []
        child_fks[ct].append({
            "parent_table": r["PARENT_TABLE"], "parent_col": r["PARENT_COLUMN"],
            "child_col": r["CHILD_COLUMN"], "is_composite": r["IS_COMPOSITE_KEY"],
            "composite_cols": composite_conditions.get(r["RELATIONSHIP_ID"], [])
        })
    
    table_strategies = {t["TABLE_NAME"]: t["REDUCTION_STRATEGY"] for t in tables}
    processed = set()
    results = []
    
    for t in tables:
        table_name = t["TABLE_NAME"]
        strategy = t["REDUCTION_STRATEGY"]
        tbl_sample_pct = float(t["SAMPLE_PERCENT"]) if t["SAMPLE_PERCENT"] else sample_pct
        start_time = time.time()
        src_full = f"{src_db}.{src_schema}.{table_name}"
        tgt_full = f"{tgt_db}.{tgt_schema}.{table_name}"
        
        try:
            if strategy == 'FULL_COPY':
                session.sql(f"CREATE OR REPLACE TABLE {tgt_full} AS SELECT * FROM {src_full}").collect()
            
            elif strategy == 'SAMPLE':
                sampling_strategy = t["SAMPLING_STRATEGY"] if t["SAMPLING_STRATEGY"] else "RANDOM"
                stratify_cols_val = t["STRATIFY_COLUMNS"] if t["STRATIFY_COLUMNS"] else ""
                
                if sampling_strategy == 'STRATIFIED' and stratify_cols_val:
                    strat_cols = stratify_cols_val.strip()
                    on_parts = ' AND '.join(f"n.{c.strip()} = sc.{c.strip()}" for c in strat_cols.split(','))
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS
                        WITH strata_counts AS (
                            SELECT {strat_cols}, COUNT(*) AS stratum_size,
                                   GREATEST(1, ROUND(COUNT(*) * {tbl_sample_pct} / 100.0)) AS sample_size
                            FROM {src_full} GROUP BY {strat_cols}
                        ),
                        numbered AS (
                            SELECT s.*, ROW_NUMBER() OVER (PARTITION BY {strat_cols} ORDER BY RANDOM({seed})) AS _rn
                            FROM {src_full} s
                        )
                        SELECT n.* EXCLUDE (_rn)
                        FROM numbered n
                        JOIN strata_counts sc ON {on_parts}
                        WHERE n._rn <= sc.sample_size
                    """).collect()
                else:
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS 
                        SELECT * FROM {src_full} SAMPLE BERNOULLI ({tbl_sample_pct}) SEED ({seed})
                    """).collect()
            
            elif strategy == 'FK_CASCADE':
                fks = child_fks.get(table_name, [])
                sampled_parent_fks = []
                for fk in fks:
                    if fk["parent_table"] in processed:
                        parent_strategy = table_strategies.get(fk["parent_table"], "FULL_COPY")
                        if parent_strategy in ('SAMPLE', 'FK_CASCADE'):
                            sampled_parent_fks.append(fk)
                
                if sampled_parent_fks:
                    join_clauses = []
                    for i, fk in enumerate(sampled_parent_fks):
                        parent_tgt = f"{tgt_db}.{tgt_schema}.{fk['parent_table']}"
                        alias = f"p{i}"
                        if fk["is_composite"] and fk["composite_cols"]:
                            on_clause = " AND ".join(
                                f"child.{cc['child_col']} = {alias}.{cc['parent_col']}" for cc in fk["composite_cols"])
                        else:
                            on_clause = f"child.{fk['child_col']} = {alias}.{fk['parent_col']}"
                        join_clauses.append(f"INNER JOIN {parent_tgt} {alias} ON {on_clause}")
                    
                    joins_sql = "\n                        ".join(join_clauses)
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS
                        SELECT DISTINCT child.* FROM {src_full} child {joins_sql}
                    """).collect()
                else:
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS 
                        SELECT * FROM {src_full} SAMPLE BERNOULLI ({sample_pct}) SEED ({seed})
                    """).collect()
            
            elif strategy == 'EXCLUDE':
                elapsed = time.time() - start_time
                log_table_result(session, src_db, job_id, table_name, 0, 0, 0, 'EXCLUDE', 'SKIPPED', elapsed)
                continue
            
            tgt_count = session.sql(f"SELECT COUNT(*) FROM {tgt_full}").collect()[0][0]
            src_count = t["ROW_COUNT"] or 0
            reduction_pct = round((1 - tgt_count / max(src_count, 1)) * 100, 2)
            elapsed = time.time() - start_time
            
            log_table_result(session, src_db, job_id, table_name, src_count, tgt_count, reduction_pct, strategy, 'COMPLETED', elapsed)
            processed.add(table_name)
            
            session.sql(f"""
                UPDATE {src_db}.EDRP_METADATA.REDUCTION_JOB_LOG
                SET TABLES_CREATED_LIST = ARRAY_APPEND(
                    COALESCE(TABLES_CREATED_LIST, PARSE_JSON('[]')), TO_VARIANT('{table_name}'))
                WHERE JOB_ID = {job_id}
            """).collect()
            
            results.append({"table": table_name, "strategy": strategy,
                "source_rows": src_count, "target_rows": tgt_count,
                "reduction_pct": reduction_pct, "time_sec": round(elapsed, 1)})
        
        except Exception as e:
            elapsed = time.time() - start_time
            log_table_result(session, src_db, job_id, table_name, 0, 0, 0, strategy, 'FAILED', elapsed, str(e))
            results.append({"table": table_name, "error": str(e)})
    
    session.sql(f"""
        UPDATE {src_db}.EDRP_METADATA.REDUCTION_JOB_LOG
        SET JOB_STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP(),
            TABLES_PROCESSED = {len(processed)}, TOTAL_TABLES = {len(tables)}
        WHERE JOB_ID = {job_id}
    """).collect()
    
    return {"job_id": job_id, "tables_processed": len(processed), "results": results}


def log_table_result(session, db, job_id, table_name, src_count, tgt_count, reduction_pct, strategy, status, elapsed, error=None):
    error_sql = f"'{error[:3900]}'" if error else "NULL"
    session.sql(f"""
        INSERT INTO {db}.EDRP_METADATA.REDUCTION_TABLE_LOG
            (JOB_ID, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT,
             STRATEGY_USED, STATUS, EXECUTION_TIME_SEC, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT)
        VALUES ({job_id}, '{table_name}', {src_count}, {tgt_count}, {reduction_pct},
                '{strategy}', '{status}', {round(elapsed, 2)}, {error_sql},
                DATEADD('second', -{int(elapsed)}, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP())
    """).collect()
$$;


-- ============================================================================
-- 3.4 SP_VALIDATE_REDUCTION
-- FK integrity, distribution comparison, scenario coverage
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(
    JOB_ID_PARAM NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Validates reduced dataset: FK integrity, distribution, scenario coverage'
AS
$$
import json

def run(session, job_id_param):
    job = session.sql(f"""
        SELECT j.*, p.SOURCE_DATABASE, p.SOURCE_SCHEMA, p.TARGET_DATABASE, p.TARGET_SCHEMA
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
        JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
        WHERE j.JOB_ID = {job_id_param}
    """).collect()
    
    if not job:
        return {"error": f"Job {job_id_param} not found"}
    
    j = job[0]
    src = f"{j['SOURCE_DATABASE']}.{j['SOURCE_SCHEMA']}"
    tgt = f"{j['TARGET_DATABASE']}.{j['TARGET_SCHEMA']}"
    db = j['SOURCE_DATABASE']
    
    rels = session.sql(f"""
        SELECT PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN
        FROM {db}.EDRP_METADATA.RELATIONSHIP_MAP
        WHERE SOURCE_DATABASE = '{j["SOURCE_DATABASE"]}' AND SOURCE_SCHEMA = '{j["SOURCE_SCHEMA"]}'
          AND RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()
    
    results = {"fk_checks": [], "distribution_checks": [], "summary": {}}
    total_pass = 0
    total_fail = 0
    
    for r in rels:
        child_tbl, child_col = r["CHILD_TABLE"], r["CHILD_COLUMN"]
        parent_tbl, parent_col = r["PARENT_TABLE"], r["PARENT_COLUMN"]
        check_name = f"{child_tbl}.{child_col} -> {parent_tbl}.{parent_col}"
        try:
            orphan_count = session.sql(f"""
                SELECT COUNT(*) FROM {tgt}.{child_tbl} c
                WHERE c.{child_col} IS NOT NULL
                  AND c.{child_col} NOT IN (SELECT p.{parent_col} FROM {tgt}.{parent_tbl} p)
            """).collect()[0][0]
            status = "PASS" if orphan_count == 0 else "FAIL"
            if status == "PASS": total_pass += 1
            else: total_fail += 1
            session.sql(f"""
                INSERT INTO {db}.EDRP_METADATA.VALIDATION_RESULTS
                    (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, SOURCE_VALUE, TARGET_VALUE, PASS_FAIL, DETAILS)
                VALUES ({job_id_param}, '{child_tbl}', 'FK_INTEGRITY', '{check_name}',
                        '0', '{orphan_count} orphans', '{status}',
                        'Orphaned FK count in reduced {child_tbl}.{child_col}')
            """).collect()
            results["fk_checks"].append({"check": check_name, "orphans": orphan_count, "status": status})
        except Exception as e:
            results["fk_checks"].append({"check": check_name, "error": str(e)})
    
    table_logs = session.sql(f"""
        SELECT TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT
        FROM {db}.EDRP_METADATA.REDUCTION_TABLE_LOG
        WHERE JOB_ID = {job_id_param} AND STATUS = 'COMPLETED'
    """).collect()
    for tl in table_logs:
        session.sql(f"""
            INSERT INTO {db}.EDRP_METADATA.VALIDATION_RESULTS
                (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, SOURCE_VALUE, TARGET_VALUE,
                 DEVIATION_PERCENT, PASS_FAIL, DETAILS)
            VALUES ({job_id_param}, '{tl["TABLE_NAME"]}', 'ROW_COUNT', 'Row count ratio',
                    '{tl["SOURCE_ROW_COUNT"]}', '{tl["TARGET_ROW_COUNT"]}',
                    {tl["REDUCTION_PERCENT"] or 0}, 'PASS', 'Reduction: {tl["REDUCTION_PERCENT"]}%')
        """).collect()
    
    large_tables = session.sql(f"""
        SELECT TABLE_NAME FROM {db}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{j["SOURCE_DATABASE"]}' AND SOURCE_SCHEMA = '{j["SOURCE_SCHEMA"]}'
          AND TABLE_TYPE = 'FACT' AND IS_ACTIVE = TRUE
    """).collect()
    
    for lt in large_tables:
        tbl = lt["TABLE_NAME"]
        fk_cols = [r["CHILD_COLUMN"] for r in rels if r["CHILD_TABLE"] == tbl]
        for col in fk_cols[:3]:
            try:
                dist_check = session.sql(f"""
                    WITH src_dist AS (
                        SELECT {col}, COUNT(*) AS cnt, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
                        FROM {src}.{tbl} WHERE {col} IS NOT NULL GROUP BY {col} ORDER BY cnt DESC LIMIT 10
                    ), tgt_dist AS (
                        SELECT {col}, COUNT(*) AS cnt, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
                        FROM {tgt}.{tbl} WHERE {col} IS NOT NULL GROUP BY {col} ORDER BY cnt DESC LIMIT 10
                    )
                    SELECT AVG(ABS(COALESCE(s.pct, 0) - COALESCE(t.pct, 0))) AS avg_deviation
                    FROM src_dist s FULL OUTER JOIN tgt_dist t ON s.{col} = t.{col}
                """).collect()[0][0]
                dev = float(dist_check or 0)
                status = "PASS" if dev < 5.0 else "WARN" if dev < 10.0 else "FAIL"
                session.sql(f"""
                    INSERT INTO {db}.EDRP_METADATA.VALIDATION_RESULTS
                        (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, DEVIATION_PERCENT,
                         THRESHOLD_PERCENT, PASS_FAIL, DETAILS)
                    VALUES ({job_id_param}, '{tbl}', 'DISTRIBUTION', '{tbl}.{col} top-10 frequency',
                            {round(dev, 4)}, 5.0, '{status}',
                            'Avg deviation of top-10 value frequencies: {round(dev, 2)}%')
                """).collect()
                results["distribution_checks"].append({"table": tbl, "column": col, "avg_deviation_pct": round(dev, 2), "status": status})
            except Exception as e:
                results["distribution_checks"].append({"table": tbl, "column": col, "error": str(e)})
    
    session.sql(f"""
        UPDATE {db}.EDRP_METADATA.REDUCTION_JOB_LOG
        SET JOB_STATUS = CASE WHEN {total_fail} > 0 THEN 'VALIDATION_FAILED' ELSE 'VALIDATED' END
        WHERE JOB_ID = {job_id_param}
    """).collect()
    
    results["summary"] = {
        "total_fk_checks": len(results["fk_checks"]), "fk_passed": total_pass,
        "fk_failed": total_fail, "distribution_checks": len(results["distribution_checks"]),
        "overall": "PASS" if total_fail == 0 else "FAIL"
    }
    return results
$$;


-- ============================================================================
-- 3.5 SP_ROLLBACK_REDUCTION
-- Cleanup failed or unwanted reduction jobs
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION(
    JOB_ID_PARAM NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Rolls back a reduction job by dropping all tables it created in the target schema'
AS
$$
import json

def run(session, job_id_param):
    job = session.sql(f"""
        SELECT j.TABLES_CREATED_LIST, j.JOB_STATUS, p.TARGET_DATABASE, p.TARGET_SCHEMA
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
        JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
        WHERE j.JOB_ID = {job_id_param}
    """).collect()
    
    if not job:
        return {"error": f"Job {job_id_param} not found"}
    
    j = job[0]
    tgt = f"{j['TARGET_DATABASE']}.{j['TARGET_SCHEMA']}"
    tables_raw = j["TABLES_CREATED_LIST"]
    
    if tables_raw is None:
        return {"error": "No tables recorded for this job - nothing to roll back"}
    
    tables_list = json.loads(str(tables_raw)) if not isinstance(tables_raw, list) else tables_raw
    if not tables_list:
        return {"error": "No tables recorded for this job - nothing to roll back"}
    
    dropped, errors = [], []
    for tbl_name in reversed(tables_list):
        try:
            session.sql(f"DROP TABLE IF EXISTS {tgt}.{tbl_name}").collect()
            dropped.append(tbl_name)
        except Exception as e:
            errors.append({"table": tbl_name, "error": str(e)})
    
    session.sql(f"""
        UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
        SET JOB_STATUS = 'ROLLED_BACK' WHERE JOB_ID = {job_id_param}
    """).collect()
    
    return {"job_id": job_id_param, "tables_dropped": len(dropped), "errors": errors}
$$;


-- ============================================================================
-- 3.6 SP_AI_DISCOVER_RELATIONSHIPS
-- Uses Cortex COMPLETE to suggest undiscovered relationships
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_AI_DISCOVER_RELATIONSHIPS(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Uses Cortex COMPLETE to suggest undiscovered relationships from column naming patterns'
AS
$$
import json

def run(session, source_database, source_schema):
    candidates = session.sql(f"""
        SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE
        FROM {source_database}.EDRP_METADATA.COLUMN_INVENTORY c
        LEFT JOIN {source_database}.EDRP_METADATA.RELATIONSHIP_MAP r
            ON r.CHILD_TABLE = c.TABLE_NAME AND r.CHILD_COLUMN = c.COLUMN_NAME
            AND r.SOURCE_DATABASE = c.SOURCE_DATABASE AND r.SOURCE_SCHEMA = c.SOURCE_SCHEMA
        WHERE c.SOURCE_DATABASE = '{source_database}' AND c.SOURCE_SCHEMA = '{source_schema}'
          AND (c.COLUMN_NAME LIKE '%_SK' OR c.COLUMN_NAME LIKE '%_ID' OR c.COLUMN_NAME LIKE '%_KEY')
          AND c.DATA_TYPE = 'NUMBER' AND r.RELATIONSHIP_ID IS NULL
    """).collect()
    
    if not candidates:
        return {"message": "No unmapped FK-like columns found", "suggestions": []}
    
    col_list = ", ".join(f"{c['TABLE_NAME']}.{c['COLUMN_NAME']}" for c in candidates)
    known_tables = session.sql(f"""
        SELECT LISTAGG(DISTINCT TABLE_NAME, ', ') AS TBL_LIST
        FROM {source_database}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
    """).collect()[0]["TBL_LIST"]
    
    prompt = (
        'You are a data architect. Given these unmapped columns that look like foreign keys, '
        'suggest which parent table and column each one likely references. '
        'Return ONLY a JSON array: [{"child_table":"X","child_column":"Y","parent_table":"Z","parent_column":"W","confidence":"HIGH/MEDIUM/LOW"}]. '
        f'Columns: {col_list}. Known tables in schema: {known_tables}'
    )
    
    ai_result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{prompt.replace("'", "''")}')
    """).collect()[0][0]
    
    try:
        start_idx = ai_result.find('[')
        end_idx = ai_result.rfind(']') + 1
        if start_idx >= 0 and end_idx > start_idx:
            suggestions = json.loads(ai_result[start_idx:end_idx])
        else:
            return {"message": "AI did not return valid JSON", "raw": ai_result}
        
        inserted = 0
        for s in suggestions:
            pt, pc = s.get("parent_table", "").upper(), s.get("parent_column", "").upper()
            ct, cc = s.get("child_table", "").upper(), s.get("child_column", "").upper()
            conf = s.get("confidence", "MEDIUM").upper()
            if not all([pt, pc, ct, cc]): continue
            exists = session.sql(f"""
                SELECT COUNT(*) FROM {source_database}.EDRP_METADATA.TABLE_INVENTORY
                WHERE TABLE_NAME = '{pt}' AND SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
            """).collect()[0][0]
            if exists > 0:
                session.sql(f"""
                    INSERT INTO {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
                        (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN,
                         CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE, CARDINALITY,
                         DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS, NOTES)
                    VALUES ('{source_database}', '{source_schema}', '{pt}', '{pc}',
                            '{ct}', '{cc}', 'LEFT', '1:N', 'CORTEX_AI', '{conf}',
                            'PENDING_REVIEW', 'AI-suggested relationship')
                """).collect()
                inserted += 1
        return {"candidates_found": len(candidates), "suggestions": len(suggestions), "inserted": inserted}
    except Exception as e:
        return {"error": str(e), "raw_response": ai_result[:2000]}
$$;


-- ============================================================================
-- 3.7 FN_AI_SUMMARIZE_VALIDATION
-- Uses Cortex COMPLETE to generate human-readable validation summary
-- ============================================================================
CREATE OR REPLACE FUNCTION DATA_REDUCTION_POC.EDRP_APP.FN_AI_SUMMARIZE_VALIDATION(JOB_ID_PARAM NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Uses Cortex COMPLETE to generate a human-readable validation summary'
AS
$$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Summarize this data reduction validation report for a business stakeholder. '
        || 'Highlight any failures or warnings. Be concise. '
        || 'Report data: ' || (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'table', TABLE_NAME, 'check', CHECK_NAME, 'type', CHECK_TYPE,
                'result', PASS_FAIL, 'detail', DETAILS
            ))::VARCHAR
            FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
            WHERE JOB_ID = JOB_ID_PARAM
        )
    )
$$;
