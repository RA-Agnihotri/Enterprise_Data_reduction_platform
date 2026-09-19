import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(page_title="EDRP Dashboard", layout="wide")
st.title("Enterprise Data Reduction Platform")

# Sidebar navigation
page = st.sidebar.radio("Navigation", [
    "Overview",
    "Relationship Map",
    "Reduction Profiles",
    "Execute Reduction",
    "Validation Results",
    "AI Insights"
])

# ─── OVERVIEW PAGE ───
if page == "Overview":
    st.header("Reduction Overview")
    
    col1, col2, col3 = st.columns(3)
    
    tables = session.sql("""
        SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY WHERE IS_ACTIVE = TRUE
    """).collect()[0]["CNT"]
    
    rels = session.sql("""
        SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP WHERE RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()[0]["CNT"]
    
    jobs = session.sql("""
        SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG WHERE JOB_STATUS IN ('COMPLETED', 'VALIDATED')
    """).collect()[0]["CNT"]
    
    col1.metric("Tables in Scope", tables)
    col2.metric("Active Relationships", rels)
    col3.metric("Completed Jobs", jobs)
    
    # Latest job results
    st.subheader("Latest Reduction Results")
    latest = session.sql("""
        SELECT TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT, STRATEGY_USED, STATUS
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
        WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
        ORDER BY SOURCE_ROW_COUNT DESC
    """).to_pandas()
    st.dataframe(latest, use_container_width=True)

# ─── RELATIONSHIP MAP PAGE ───
elif page == "Relationship Map":
    st.header("Relationship Map")
    
    # Filter by discovery source
    source_filter = st.multiselect("Filter by Discovery Source", 
        ["SCALA_PARSER", "INFO_SCHEMA", "ACCESS_HISTORY", "CORTEX_AI", "MANUAL"],
        default=["INFO_SCHEMA", "MANUAL"])
    
    if source_filter:
        filter_str = ",".join([f"'{s}'" for s in source_filter])
        rels_df = session.sql(f"""
            SELECT PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, 
                   JOIN_TYPE, DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
            WHERE DISCOVERED_BY IN ({filter_str})
            ORDER BY CHILD_TABLE, CHILD_COLUMN
        """).to_pandas()
        st.dataframe(rels_df, use_container_width=True)
    
    # Pending review section
    st.subheader("Pending Review")
    pending = session.sql("""
        SELECT RELATIONSHIP_ID, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, 
               DISCOVERED_BY, CONFIDENCE
        FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
        WHERE RELATIONSHIP_STATUS = 'PENDING_REVIEW'
    """).to_pandas()
    
    if len(pending) > 0:
        st.dataframe(pending, use_container_width=True)
        if st.button("Approve All Pending"):
            session.sql("""
                UPDATE DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                SET RELATIONSHIP_STATUS = 'ACTIVE', 
                    REVIEWED_BY = CURRENT_USER(), 
                    REVIEWED_AT = CURRENT_TIMESTAMP()
                WHERE RELATIONSHIP_STATUS = 'PENDING_REVIEW'
            """).collect()
            st.success("All pending relationships approved!")
            st.rerun()
    else:
        st.info("No relationships pending review.")

# ─── REDUCTION PROFILES PAGE ───
elif page == "Reduction Profiles":
    st.header("Reduction Profiles")
    
    profiles = session.sql("""
        SELECT PROFILE_NAME, SOURCE_SCHEMA, TARGET_SCHEMA, DEFAULT_SAMPLE_PERCENT, 
               DIMENSION_STRATEGY, FACT_STRATEGY, STATUS
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
        ORDER BY CREATED_AT DESC
    """).to_pandas()
    st.dataframe(profiles, use_container_width=True)
    
    # Table-level strategy editor
    st.subheader("Table-Level Strategies")
    table_strats = session.sql("""
        SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROLE, ROW_COUNT, 
               REDUCTION_STRATEGY, SAMPLE_PERCENT, SAMPLING_STRATEGY, STRATIFY_COLUMNS
        FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
        WHERE IS_ACTIVE = TRUE
        ORDER BY REDUCTION_ORDER
    """).to_pandas()
    st.dataframe(table_strats, use_container_width=True)

# ─── EXECUTE REDUCTION PAGE ───
elif page == "Execute Reduction":
    st.header("Execute Reduction")
    
    profiles = session.sql("""
        SELECT PROFILE_NAME FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE WHERE STATUS = 'ACTIVE'
    """).to_pandas()
    
    if len(profiles) == 0:
        st.warning("No active profiles found. Create one in REDUCTION_PROFILE with STATUS = 'ACTIVE'.")
    else:
        selected_profile = st.selectbox("Select Profile", profiles["PROFILE_NAME"].tolist())
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Run Reduction", type="primary"):
                with st.spinner("Executing reduction... this may take several minutes for large datasets."):
                    result = session.sql(f"CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('{selected_profile}')").collect()
                    st.success("Reduction complete!")
                    st.json(result[0][0])
        
        with col2:
            if st.button("Run Validation"):
                job_id = session.sql("SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG").collect()[0][0]
                with st.spinner("Validating..."):
                    result = session.sql(f"CALL DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION({job_id})").collect()
                    st.success("Validation complete!")
                    st.json(result[0][0])
        
        with col3:
            # Resume failed job
            failed_jobs = session.sql("""
                SELECT JOB_ID FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
                WHERE JOB_STATUS = 'FAILED' AND TABLES_CREATED_LIST IS NOT NULL
                  AND ARRAY_SIZE(TABLES_CREATED_LIST) > 0
                ORDER BY JOB_ID DESC LIMIT 5
            """).to_pandas()
            if len(failed_jobs) > 0:
                resume_id = st.selectbox("Resume Failed Job", failed_jobs["JOB_ID"].tolist())
                if st.button("Resume Job"):
                    with st.spinner(f"Resuming job {resume_id}..."):
                        result = session.sql(f"CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('{selected_profile}', {resume_id})").collect()
                        st.success("Resume complete!")
                        st.json(result[0][0])
    
    # ─── LIVE PROGRESS PANEL ───
    st.subheader("Live Progress")
    
    # Get the latest job
    latest_job = session.sql("""
        SELECT j.JOB_ID, j.JOB_STATUS, j.TOTAL_TABLES, j.TABLES_PROCESSED,
               j.CURRENT_TABLE, j.STARTED_AT, j.COMPLETED_AT,
               p.PROFILE_NAME, p.SOURCE_SCHEMA, p.TARGET_SCHEMA
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
        JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
        ORDER BY j.JOB_ID DESC LIMIT 1
    """).collect()
    
    if latest_job:
        lj = latest_job[0]
        job_id_display = lj["JOB_ID"]
        status = lj["JOB_STATUS"]
        total = lj["TOTAL_TABLES"] or 0
        processed = lj["TABLES_PROCESSED"] or 0
        current_tbl = lj["CURRENT_TABLE"]
        profile_name_display = lj["PROFILE_NAME"]
        
        # Status badges
        status_colors = {
            "RUNNING": "orange", "COMPLETED": "green", "VALIDATED": "green",
            "FAILED": "red", "VALIDATION_FAILED": "red", "ROLLED_BACK": "gray"
        }
        st_color = status_colors.get(status, "gray")
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Job ID", job_id_display)
        m2.metric("Status", status)
        m3.metric("Progress", f"{processed}/{total}" if total > 0 else "—")
        m4.metric("Profile", profile_name_display)
        
        # Progress bar
        if total > 0:
            progress_pct = min(processed / total, 1.0)
            st.progress(progress_pct, text=f"{'Currently processing: ' + current_tbl if current_tbl else 'Complete' if status == 'COMPLETED' else status}")
        
        # Table-by-table breakdown
        st.subheader("Table Status")
        
        # Get all active tables in reduction order
        all_tables = session.sql(f"""
            SELECT TABLE_NAME, REDUCTION_STRATEGY, REDUCTION_ORDER
            FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
            WHERE SOURCE_DATABASE = '{lj["SOURCE_SCHEMA"]}' OR SOURCE_SCHEMA = '{lj["SOURCE_SCHEMA"]}'
              AND IS_ACTIVE = TRUE
            ORDER BY REDUCTION_ORDER
        """).to_pandas()
        
        # Get completed tables for this job from table log
        completed_tables = session.sql(f"""
            SELECT TABLE_NAME, STRATEGY_USED, TARGET_ROW_COUNT, REDUCTION_PERCENT,
                   ROUND(EXECUTION_TIME_SEC, 1) AS TIME_SEC, STATUS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
            WHERE JOB_ID = {job_id_display}
            ORDER BY LOG_ID
        """).to_pandas()
        
        if len(completed_tables) > 0:
            # Build status for each table
            completed_set = set(completed_tables["TABLE_NAME"].tolist())
            
            display_rows = []
            for _, row in completed_tables.iterrows():
                icon = "COMPLETED" if row["STATUS"] == "COMPLETED" else "FAILED" if row["STATUS"] == "FAILED" else "SKIPPED"
                display_rows.append({
                    "Status": icon,
                    "Table": row["TABLE_NAME"],
                    "Strategy": row["STRATEGY_USED"],
                    "Reduced Rows": f"{int(row['TARGET_ROW_COUNT']):,}" if row["TARGET_ROW_COUNT"] else "—",
                    "Reduction %": f"{row['REDUCTION_PERCENT']}%" if row["REDUCTION_PERCENT"] else "—",
                    "Time (s)": row["TIME_SEC"] if row["TIME_SEC"] else "—"
                })
            
            # Add currently running table
            if current_tbl and current_tbl not in completed_set:
                display_rows.append({
                    "Status": "RUNNING",
                    "Table": current_tbl,
                    "Strategy": "—",
                    "Reduced Rows": "—",
                    "Reduction %": "—",
                    "Time (s)": "..."
                })
            
            # Add pending tables
            if len(all_tables) > 0:
                for _, row in all_tables.iterrows():
                    tname = row["TABLE_NAME"]
                    if tname not in completed_set and tname != current_tbl:
                        display_rows.append({
                            "Status": "PENDING",
                            "Table": tname,
                            "Strategy": row["REDUCTION_STRATEGY"],
                            "Reduced Rows": "—",
                            "Reduction %": "—",
                            "Time (s)": "—"
                        })
            
            import pandas as pd
            st.dataframe(pd.DataFrame(display_rows), use_container_width=True)
        else:
            st.info("No table-level progress yet for this job.")
    else:
        st.info("No jobs found. Run a reduction first.")
    
    # ─── JOB HISTORY ───
    st.subheader("Recent Job History")
    history = session.sql("""
        SELECT JOB_ID, JOB_STATUS, TOTAL_TABLES, TABLES_PROCESSED, 
               CURRENT_TABLE, STARTED_AT, COMPLETED_AT
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
        ORDER BY JOB_ID DESC LIMIT 10
    """).to_pandas()
    st.dataframe(history, use_container_width=True)

# ─── VALIDATION RESULTS PAGE ───
elif page == "Validation Results":
    st.header("Validation Results")
    
    job_ids = session.sql("""
        SELECT JOB_ID, JOB_STATUS, STARTED_AT, TABLES_PROCESSED
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
        ORDER BY JOB_ID DESC LIMIT 10
    """).to_pandas()
    
    if len(job_ids) == 0:
        st.info("No jobs found. Run a reduction first.")
    else:
        selected_job = st.selectbox("Select Job", job_ids["JOB_ID"].tolist())
        
        # Summary metrics
        summary = session.sql(f"""
            SELECT 
                SUM(CASE WHEN PASS_FAIL = 'PASS' THEN 1 ELSE 0 END) AS PASSED,
                SUM(CASE WHEN PASS_FAIL = 'FAIL' THEN 1 ELSE 0 END) AS FAILED,
                SUM(CASE WHEN PASS_FAIL = 'WARN' THEN 1 ELSE 0 END) AS WARNINGS,
                COUNT(*) AS TOTAL
            FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
            WHERE JOB_ID = {selected_job}
        """).collect()[0]
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Checks", summary["TOTAL"])
        col2.metric("Passed", summary["PASSED"])
        col3.metric("Failed", summary["FAILED"])
        col4.metric("Warnings", summary["WARNINGS"])
        
        # Detailed results in tabs
        tab1, tab2, tab3 = st.tabs(["FK Integrity", "Distribution", "Row Counts"])
        
        with tab1:
            fk_results = session.sql(f"""
                SELECT TABLE_NAME, CHECK_NAME, PASS_FAIL, DETAILS
                FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
                WHERE JOB_ID = {selected_job} AND CHECK_TYPE = 'FK_INTEGRITY'
                ORDER BY PASS_FAIL DESC, TABLE_NAME
            """).to_pandas()
            st.dataframe(fk_results, use_container_width=True)
        
        with tab2:
            dist_results = session.sql(f"""
                SELECT TABLE_NAME, CHECK_NAME, DEVIATION_PERCENT, THRESHOLD_PERCENT, PASS_FAIL
                FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
                WHERE JOB_ID = {selected_job} AND CHECK_TYPE = 'DISTRIBUTION'
                ORDER BY DEVIATION_PERCENT DESC
            """).to_pandas()
            st.dataframe(dist_results, use_container_width=True)
        
        with tab3:
            row_results = session.sql(f"""
                SELECT TABLE_NAME, SOURCE_VALUE AS SOURCE_ROWS, TARGET_VALUE AS REDUCED_ROWS, 
                       DEVIATION_PERCENT AS REDUCTION_PCT
                FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
                WHERE JOB_ID = {selected_job} AND CHECK_TYPE = 'ROW_COUNT'
                ORDER BY TABLE_NAME
            """).to_pandas()
            st.dataframe(row_results, use_container_width=True)
        
        # AI Summary
        st.subheader("AI-Generated Summary")
        if st.button("Generate AI Summary"):
            with st.spinner("Generating summary with Cortex AI..."):
                ai_summary = session.sql(f"""
                    SELECT DATA_REDUCTION_POC.EDRP_APP.FN_AI_SUMMARIZE_VALIDATION({selected_job})
                """).collect()[0][0]
                st.markdown(ai_summary)

# ─── AI INSIGHTS PAGE ───
elif page == "AI Insights":
    st.header("AI-Powered Insights")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "Talk to Reduced Data", "Ask Metadata", "Relationship Discovery", "Column Sensitivity"
    ])
    
    # ─── TAB 1: TALK TO REDUCED DATA (Cortex Agent) ───
    with tab1:
        st.subheader("Talk to Reduced Data")
        st.caption("Use the EDRP Data Agent to ask natural language questions about the reduced dataset.")
        
        st.info("The **EDRP_DATA_AGENT** is deployed and ready. You can chat with it in Snowsight:")
        st.markdown("""
**How to use the Agent:**
1. Go to **Snowsight** > **AI & ML** > **Cortex Agents** (or **Snowflake Intelligence**)
2. Select **EDRP_DATA_AGENT** from `DATA_REDUCTION_POC.EDRP_APP`
3. Start asking questions in natural language!
        """)
        
        st.subheader("Quick Query")
        st.caption("Or run a quick question here using Cortex AI:")
        
        question = st.text_input("Ask a question about the reduced data:", key="data_question",
            placeholder="e.g., What are the top 10 stores by total return amount?")
        
        if st.button("Ask", key="ask_data") and question:
            with st.spinner("Analyzing..."):
                try:
                    data_context = (
                        "You are a SQL expert. Generate Snowflake SQL to answer the user question. "
                        "Use these tables in DATA_REDUCTION_POC.STAGE_DATA_REDUCED: "
                        "STORE_RETURNS (SR_ITEM_SK, SR_CUSTOMER_SK, SR_STORE_SK, SR_RETURN_QUANTITY, SR_RETURN_AMT, SR_RETURN_TAX, SR_FEE, SR_NET_LOSS), "
                        "CUSTOMER (C_CUSTOMER_SK, C_FIRST_NAME, C_LAST_NAME, C_BIRTH_COUNTRY), "
                        "STORE (S_STORE_SK, S_STORE_NAME, S_CITY, S_STATE), "
                        "ITEM (I_ITEM_SK, I_CURRENT_PRICE, I_BRAND, I_CLASS, I_CATEGORY, I_PRODUCT_NAME). "
                        "Joins: SR_CUSTOMER_SK=C_CUSTOMER_SK, SR_STORE_SK=S_STORE_SK, SR_ITEM_SK=I_ITEM_SK. "
                        "All amounts in USD. Return ONLY the SQL, nothing else."
                    )
                    prompt = f"{data_context}\n\nQuestion: {question}"
                    safe_prompt = prompt.replace("'", "''")
                    
                    ai_sql = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{safe_prompt}')
                    """).collect()[0][0]
                    
                    sql_text = ai_sql.strip()
                    if "```sql" in sql_text:
                        sql_text = sql_text.split("```sql")[1].split("```")[0].strip()
                    elif "```" in sql_text:
                        sql_text = sql_text.split("```")[1].split("```")[0].strip()
                    
                    lines = sql_text.split('\n')
                    sql_lines = []
                    found_sql = False
                    for line in lines:
                        if line.strip().upper().startswith(('SELECT', 'WITH')):
                            found_sql = True
                        if found_sql:
                            sql_lines.append(line)
                    if sql_lines:
                        sql_text = '\n'.join(sql_lines).rstrip(';')
                    
                    with st.expander("Generated SQL", expanded=False):
                        st.code(sql_text, language="sql")
                    
                    df = session.sql(sql_text).to_pandas()
                    st.dataframe(df, use_container_width=True)
                    
                    if len(df) > 0 and len(df.columns) >= 2:
                        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                        non_numeric = [c for c in df.columns if c not in numeric_cols]
                        if numeric_cols and non_numeric:
                            try:
                                st.bar_chart(df.set_index(non_numeric[0])[numeric_cols[0]])
                            except Exception:
                                pass
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # ─── TAB 2: ASK METADATA ───
    with tab2:
        st.subheader("Ask About EDRP Metadata")
        st.caption("Ask natural language questions about the reduction metadata — tables, relationships, jobs, validation results.")
        
        METADATA_CONTEXT = (
            "You have access to these EDRP metadata tables in DATA_REDUCTION_POC.EDRP_METADATA: "
            "TABLE_INVENTORY (TABLE_NAME, TABLE_TYPE, TABLE_ROLE, ROW_COUNT, REDUCTION_STRATEGY, SAMPLE_PERCENT, SAMPLING_STRATEGY, STRATIFY_COLUMNS, IS_ACTIVE, REDUCTION_ORDER), "
            "RELATIONSHIP_MAP (PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE, CARDINALITY, CONFIDENCE, RELATIONSHIP_STATUS, DISCOVERED_BY), "
            "REDUCTION_PROFILE (PROFILE_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA, DEFAULT_SAMPLE_PERCENT, STATUS), "
            "REDUCTION_JOB_LOG (JOB_ID, PROFILE_ID, JOB_STATUS, TOTAL_TABLES, TABLES_PROCESSED, STARTED_AT, COMPLETED_AT, CURRENT_TABLE), "
            "REDUCTION_TABLE_LOG (JOB_ID, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT, STRATEGY_USED, STATUS, EXECUTION_TIME_SEC), "
            "VALIDATION_RESULTS (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, PASS_FAIL, DEVIATION_PERCENT, DETAILS), "
            "COLUMN_INVENTORY (TABLE_NAME, COLUMN_NAME, DATA_TYPE, SENSITIVITY_CLASS). "
            "Generate a single Snowflake SQL query to answer the question. Return ONLY the SQL, no explanation."
        )
        
        with st.expander("Example questions"):
            st.markdown("""
- How many tables are configured for reduction?
- Which tables use FK_CASCADE strategy?
- Show me all relationships where CUSTOMER is the parent
- What was the result of the last validation run?
- Which tables had the highest reduction percentage?
            """)
        
        meta_question = st.text_input("Ask about EDRP metadata:", key="meta_question",
            placeholder="e.g., Which tables use stratified sampling?")
        
        if st.button("Ask", key="ask_meta") and meta_question:
            with st.spinner("Generating query..."):
                try:
                    prompt = f"{METADATA_CONTEXT}\n\nQuestion: {meta_question}"
                    safe_prompt = prompt.replace("'", "''")
                    
                    ai_sql = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{safe_prompt}')
                    """).collect()[0][0]
                    
                    sql_text = ai_sql.strip()
                    if "```sql" in sql_text:
                        sql_text = sql_text.split("```sql")[1].split("```")[0].strip()
                    elif "```" in sql_text:
                        sql_text = sql_text.split("```")[1].split("```")[0].strip()
                    
                    lines = sql_text.split('\n')
                    sql_lines = []
                    found_sql = False
                    for line in lines:
                        if line.strip().upper().startswith(('SELECT', 'WITH', 'SHOW')):
                            found_sql = True
                        if found_sql:
                            sql_lines.append(line)
                    if sql_lines:
                        sql_text = '\n'.join(sql_lines).rstrip(';')
                    
                    with st.expander("Generated SQL", expanded=False):
                        st.code(sql_text, language="sql")
                    
                    df = session.sql(sql_text).to_pandas()
                    st.dataframe(df, use_container_width=True)
                    
                    if len(df) > 0:
                        data_preview = df.head(10).to_string()
                        answer_prompt = f"Based on this data, give a brief 1-2 sentence answer to: {meta_question}\n\nData:\n{data_preview}"
                        safe_answer = answer_prompt.replace("'", "''")
                        answer = session.sql(f"""
                            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{safe_answer}')
                        """).collect()[0][0]
                        st.markdown(f"**Summary:** {answer}")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # ─── TAB 3: RELATIONSHIP DISCOVERY ───
    with tab3:
        st.subheader("Discover Missing Relationships")
        st.write("Uses Cortex AI to analyze column naming patterns and suggest FK relationships that may not be in the metadata.")
        if st.button("Run AI Relationship Discovery"):
            with st.spinner("Analyzing column patterns with Cortex AI..."):
                result = session.sql("""
                    CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_DISCOVER_RELATIONSHIPS('DATA_REDUCTION_POC', 'STAGE_DATA')
                """).collect()
                st.success("Discovery complete! Check the Relationship Map page for new PENDING_REVIEW entries.")
                st.json(result[0][0])
    
    # ─── TAB 4: COLUMN SENSITIVITY ───
    with tab4:
        st.subheader("Column Sensitivity Classification")
        st.write("Uses Cortex AI to classify columns as PII, PHI, PCI, CONFIDENTIAL, or PUBLIC.")
        if st.button("Run AI Classification"):
            with st.spinner("Classifying columns with Cortex AI..."):
                result = session.sql("""
                    CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_CLASSIFY_SENSITIVITY('DATA_REDUCTION_POC', 'STAGE_DATA')
                """).collect()
                st.success("Classification complete!")
                st.json(result[0][0])
        
        sensitivity = session.sql("""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, SENSITIVITY_CLASS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY
            WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC' AND SOURCE_SCHEMA = 'STAGE_DATA'
              AND SENSITIVITY_CLASS IS NOT NULL
            ORDER BY SENSITIVITY_CLASS, TABLE_NAME, COLUMN_NAME
        """).to_pandas()
        
        if len(sensitivity) > 0:
            st.dataframe(sensitivity, use_container_width=True)
        else:
            st.info("No columns classified yet. Run AI Classification above.")
