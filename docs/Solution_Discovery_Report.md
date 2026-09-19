# Enterprise Data Reduction Platform
## Solution Discovery Report

**Document Type:** Discovery & Solution Planning  
**Status:** DRAFT - Pending Stakeholder Approval  
**Date:** September 18, 2026  

---

## 1. Executive Understanding

### Problem Summary

Organizations operating at enterprise scale accumulate massive production datasets in Snowflake. Development teams, QA engineers, data scientists, and analysts frequently need representative subsets of this data for testing, development, training, analytics validation, and compliance-safe environments. Today, most data reduction efforts are manual, fragile, and produce datasets that either break referential integrity, miss critical edge cases, or fail to represent the statistical reality of production.

The Enterprise Data Reduction Platform ("EDRP") aims to solve this by intelligently generating reduced-volume datasets that are structurally sound, statistically representative, and business-scenario complete — built entirely on Snowflake-native capabilities.

**Key Context:** The client provides table names only. However, detailed relationship metadata — source/target databases, tables, columns, join types, and join columns — can be extracted by parsing existing Scala scripts in Bitbucket that consume the reduced data. This is a critical input that eliminates the need for manual relationship mapping and provides a ground-truth view of how data is actually joined and used downstream.

### Business Objectives

- **Reduce environment costs** by enabling smaller, representative datasets for non-production environments
- **Accelerate development cycles** by providing developers and testers with realistic but manageable data volumes
- **Preserve data fidelity** so that queries, reports, dashboards, ML models, and business logic behave identically on reduced datasets as they do on production
- **Support regulatory compliance** by enabling data subsetting that respects masking, lineage, and governance requirements
- **Leverage existing Scala scripts** as the primary source of relationship intelligence, minimizing manual discovery effort
- **Protect rare scenarios** that are critical for edge-case testing, fraud detection model validation, and actuarial analysis

### Expected Outcomes

- A Snowflake-native platform that accepts table names and produces reduced datasets at configurable target volumes (e.g., 1%, 5%, 10% of production)
- Relationship metadata automatically extracted from Scala scripts in Bitbucket (join types, join columns, source/target mappings)
- Datasets that pass referential integrity validation with zero orphaned foreign keys
- Statistical distribution profiles that match production within configurable tolerance thresholds
- Rare events and edge cases preserved at rates equal to or higher than their natural occurrence
- Temporal relationships (event sequences, date ordering, window-based logic) maintained
- Full audit trail and lineage from source record to reduced dataset inclusion/exclusion
- All processing runs within Snowflake (Snowpark, Cortex, Stored Procedures, Snowflake Tasks)

### Key Challenges

1. **Relationship graph complexity** — Production schemas can contain hundreds of tables with multi-level parent-child hierarchies, self-referential relationships, polymorphic associations, and implicit (non-declared) foreign keys
2. **Scala script parsing accuracy** — Extracting join metadata from Scala/Spark code requires handling multiple coding styles, dynamic SQL, and variable-driven table references
3. **Statistical preservation at low sample rates** — Maintaining distribution fidelity at 1% sampling is fundamentally harder than at 10%; some distributions collapse below certain thresholds
4. **Rare event detection without domain knowledge** — Identifying what constitutes a "rare but critical" record requires either business rules or intelligent inference
5. **Temporal integrity** — Time-series data, event chains, and slowly changing dimensions must retain their sequential logic after reduction
6. **Performance at scale** — Analyzing and reducing multi-terabyte Snowflake tables within acceptable compute windows
7. **Circular and complex dependencies** — Many-to-many relationships, bridge tables, and circular foreign keys require graph-aware traversal

### Assumptions

- All source and target data resides in Snowflake; no other database platforms are in scope
- The client provides table names; relationship metadata (joins, columns, cardinality) is extracted from Scala scripts in Bitbucket
- Bitbucket repository access is available for Scala script parsing
- Snowflake INFORMATION_SCHEMA and ACCOUNT_USAGE views are accessible for DDL and constraint introspection
- Not all foreign key relationships are formally declared in Snowflake DDL; Scala scripts and INFORMATION_SCHEMA together provide the complete relationship picture
- VARIANT, OBJECT, and ARRAY columns may exist and require specialized handling
- Data masking/anonymization is a complementary concern but is NOT the primary scope of this platform (though the architecture should not preclude it)
- The platform will operate in a read-only capacity against production databases; reduced datasets are written to separate Snowflake schemas or databases
- Users will interact with the platform through a Streamlit in Snowflake UI

---

## 2. Questions and Clarifications

### Data Questions

| # | Question | Why It Matters |
|---|----------|---------------|
| D1 | What is the typical schema complexity (number of tables, depth of FK chains) in the target Snowflake databases? | Determines graph traversal strategy and memory requirements |
| D2 | What percentage of foreign key relationships are formally declared in Snowflake DDL vs. only visible in Scala scripts? | Determines reliance on Scala parsing vs. INFORMATION_SCHEMA |
| D3 | Are there composite keys, natural keys, or surrogate keys in use? What is the mix? | Affects join preservation and key-matching logic |
| D4 | Do tables use soft deletes, temporal patterns, or slowly changing dimensions (SCD Type 1/2/3)? | Temporal reduction logic must account for these patterns |
| D5 | What is the largest single Snowflake table expected (row count and storage size)? | Determines sampling strategy and warehouse sizing |
| D6 | Are there VARIANT, OBJECT, ARRAY, or other semi-structured columns? | Requires specialized handling during reduction — nested join keys inside VARIANT are harder to track |
| D7 | Do any tables contain hierarchical/recursive relationships (e.g., org charts, bill of materials)? | Requires graph-aware tree traversal during subsetting |
| D8 | What are the expected target reduction ratios (1%, 5%, 10%, custom)? | Affects statistical preservation feasibility |
| D9 | How many Scala scripts exist in Bitbucket, and do they follow consistent patterns (Spark DataFrame API, Spark SQL, or mixed)? | Drives the complexity of the Scala parser |
| D10 | Are there Scala scripts that use dynamic table/column names (built at runtime from variables or config)? | May require manual supplementation where parsing cannot resolve references |

### Business Questions

| # | Question | Why It Matters |
|---|----------|---------------|
| B1 | Who are the primary consumers of reduced datasets? (Dev, QA, Data Science, Analytics, Compliance) | Different consumers have different fidelity requirements |
| B2 | Are there specific business scenarios that MUST be preserved in every reduced dataset? | Drives mandatory inclusion rules |
| B3 | What defines a "rare event" in your business context? Is there a frequency threshold? | Needed to calibrate rare-event protection algorithms |
| B4 | Are reduced datasets needed on a scheduled basis (nightly, weekly) or on-demand? | Drives architecture: batch pipeline vs. interactive platform |
| B5 | Must the platform support incremental reduction (delta updates) or is full regeneration acceptable? | Significantly impacts complexity and performance |
| B6 | Are there SLAs on how quickly a reduced dataset must be generated? | Constrains technology and parallelism choices |
| B7 | Do different teams need different reduction profiles from the same source? | Requires profile/template management capability |

### Technical Questions

| # | Question | Why It Matters |
|---|----------|---------------|
| T1 | What Snowflake edition is in use (Standard, Enterprise, Business Critical)? | Determines availability of features like dynamic data masking, tag-based policies, Cortex functions |
| T2 | What warehouse sizes are available for reduction workloads? | Constrains parallelism and wall-clock time for large tables |
| T3 | Are there existing metadata catalogs (Snowflake Horizon, Alation, Collibra) or is INFORMATION_SCHEMA the primary source? | Could accelerate relationship and business rule discovery |
| T4 | What Bitbucket access model is in use? (API tokens, SSH keys, OAuth) | Affects automation of Scala script retrieval |
| T5 | Is Snowflake Tasks the preferred orchestration mechanism, or is there an external orchestrator (Airflow, etc.)? | Determines pipeline orchestration approach |
| T6 | Are Snowpark stored procedures and UDFs permitted in this environment? | Core execution model depends on Snowpark availability |
| T7 | Should the platform support data masking in conjunction with reduction? | Scope expansion that affects architecture |
| T8 | Is Streamlit in Snowflake available and permitted for building the UI? | Determines UI deployment model |

### Governance Questions

| # | Question | Why It Matters |
|---|----------|---------------|
| G1 | Are there regulatory constraints on which data can leave production (PII, PHI, PCI)? | May require masking before or during reduction |
| G2 | Must reduced datasets carry lineage metadata back to source records? | Affects metadata model and audit trail design |
| G3 | Are there data retention policies that affect what can be included in reduced datasets? | Reduction logic must respect retention boundaries |
| G4 | Who approves the reduction profile for a given source system? | Requires approval workflow in the platform |
| G5 | Is there a requirement for reproducibility — same input parameters producing identical output? | Affects random seed management and determinism |
| G6 | Must the platform log every inclusion/exclusion decision for audit purposes? | Storage and performance implications for decision logging |

---

## 3. Current State Assessment

To properly design the solution, the following information must be gathered:

### Snowflake Environment Inventory

- List of databases and schemas containing tables in scope for reduction
- Snowflake edition and region
- Available warehouse sizes and compute budget
- Snowpark and Cortex feature availability
- Existing roles and access controls relevant to production data

### Bitbucket / Scala Script Inventory

- Number of Bitbucket repositories containing Scala scripts that consume reduced data
- Coding patterns in use: Spark DataFrame API (`.join()`, `.filter()`, `.select()`), Spark SQL (`spark.sql("...")`), or mixed
- Whether table and column names are hardcoded or driven by configuration files
- Presence of dynamic SQL generation or runtime-resolved table references
- Approximate number of distinct table-to-table join relationships extractable from scripts

### Metadata Landscape (from INFORMATION_SCHEMA + Scala Scripts)

- Number of schemas and tables per database in scope
- Declared primary keys, foreign keys, unique constraints (from `TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`)
- Additional join relationships discovered from Scala script parsing (not declared in DDL)
- Column inventory including data types, nullable flags, default values
- VARIANT/OBJECT/ARRAY column usage
- Clustering keys and search optimization in use

### Relationship Complexity

- Depth of the deepest FK/join chain (root-to-leaf) as seen in Scala scripts
- Presence of circular references or self-joins
- Many-to-many bridge tables
- Polymorphic associations (e.g., `entity_type` + `entity_id` patterns)
- Cross-schema or cross-database references within Snowflake

### Data Quality Baseline

- Current null rates by column
- Orphaned foreign key rates (existing integrity violations)
- Duplicate rates on business keys
- Data freshness and staleness patterns

### Volume Profile

- Row counts by table (from `TABLE_STORAGE_METRICS` or `TABLES` view)
- Storage size by table (active bytes, time travel bytes)
- Growth rates (daily, monthly)
- Skew analysis (are a few tables disproportionately large?)

### Performance Requirements

- Acceptable wall-clock time for a full reduction run
- Available compute resources (warehouse sizes, multi-cluster warehouse availability)
- Concurrency requirements (parallel reduction jobs for different schemas/databases)

---

## 4. Solution Options

### Option A: Rule-Based Reduction

**Description:** A deterministic, configuration-driven engine where human operators define reduction rules per table — sample percentages, mandatory inclusion filters, FK traversal depth, and ordering constraints. Rules are expressed in a declarative configuration format (YAML/JSON).

| Dimension | Assessment |
|-----------|-----------|
| **Benefits** | Fully predictable and reproducible. Easy to audit and explain. No AI/ML dependencies. Low operational cost. Rules can be version-controlled. |
| **Drawbacks** | Requires deep manual effort to define rules for every table. Does not adapt to schema changes. Cannot discover implicit relationships. Misses rare events unless explicitly configured. Does not scale to hundreds of tables without significant human investment. |
| **Complexity** | Medium |
| **Implementation Effort** | Medium — core engine is straightforward, but rule authoring per source system is labor-intensive |
| **Risk** | Medium — risk of rules becoming stale as schemas evolve; risk of human error in rule definition |

### Option B: Metadata-Driven Reduction

**Description:** The platform automatically introspects Snowflake INFORMATION_SCHEMA metadata (DDL, constraints, statistics) and parses Scala scripts from Bitbucket to build a dependency graph and derive reduction rules. Human input is limited to target ratios and override rules.

| Dimension | Assessment |
|-----------|-----------|
| **Benefits** | Dramatically reduces manual configuration. Adapts to schema changes via re-introspection. Leverages both declared constraints and Scala-derived joins for comprehensive FK preservation. Can compute statistical profiles from Snowflake catalog statistics. Single-platform focus simplifies implementation significantly. |
| **Drawbacks** | Scala parser must handle multiple coding patterns. Cannot discover relationships that exist neither in DDL nor in Scala scripts. Limited ability to identify business-critical scenarios. Statistical preservation depends on catalog statistics quality. |
| **Complexity** | Medium |
| **Implementation Effort** | Medium — Snowflake introspection is well-documented; Scala parsing is the main variable |
| **Risk** | Medium — Scala parsing accuracy is the primary risk; mitigated by human review of parsed relationships |

### Option C: AI-Assisted Reduction

**Description:** Extends the metadata-driven approach with Snowflake Cortex AI capabilities for implicit relationship discovery, rare event detection, business rule inference, and statistical distribution matching. Uses Cortex LLM functions (COMPLETE, CLASSIFY, EXTRACT) to augment human-defined rules rather than replacing them.

| Dimension | Assessment |
|-----------|-----------|
| **Benefits** | Discovers implicit relationships through column name analysis, data profiling, and Snowflake ACCESS_HISTORY join pattern mining. Detects rare events and edge cases statistically. Can infer business rules from data patterns using Cortex COMPLETE. Produces higher-fidelity reduced datasets with less manual effort. All AI runs inside Snowflake — no external API calls needed. |
| **Drawbacks** | AI recommendations require human validation. Cortex function costs scale with data volume. More complex to debug when results are unexpected. |
| **Complexity** | Medium-High |
| **Implementation Effort** | Medium-High — Cortex functions are straightforward to call but require prompt engineering and validation workflows |
| **Risk** | Medium — AI recommendations may be incorrect; mitigated by human-in-the-loop validation |

### Option D: Agentic Reduction Platform

**Description:** A fully agentic architecture using Snowflake Cortex Agents where specialized agents collaborate to analyze schemas, discover relationships, classify data, detect rare events, generate reduction plans, execute reduction, and validate results. Agents operate autonomously within defined guardrails and escalate to humans when confidence is low.

| Dimension | Assessment |
|-----------|-----------|
| **Benefits** | Highest degree of automation. Agents can reason about complex scenarios (circular dependencies, cross-schema relationships). Self-improving through feedback loops. Supports natural language interaction for reduction profile specification. Can explain decisions in human-readable form. |
| **Drawbacks** | Highest implementation complexity. Cortex Agents are a newer capability with evolving best practices. Debugging agent behavior is harder than debugging deterministic code. Cortex credit consumption can be significant. Requires careful guardrail design to prevent data loss or integrity violations. |
| **Complexity** | Very High |
| **Implementation Effort** | Very High — requires agent framework, orchestration, guardrails, feedback loops, and extensive testing |
| **Risk** | High — emerging technology; requires strong validation layer to catch agent errors |

### Option E: Snowflake-Native Pragmatic Reduction (NEW — RECOMMENDED)

**Description:** A practical, easy-to-implement approach that combines Snowflake INFORMATION_SCHEMA introspection with Scala script parsing (from Bitbucket) to build a complete relationship map, then executes graph-aware reduction using Snowpark Python stored procedures and dynamic SQL. Cortex AI is used selectively for rare event detection and validation — not as the core engine. The UI runs as Streamlit in Snowflake. Orchestration uses Snowflake Tasks.

| Dimension | Assessment |
|-----------|-----------|
| **Benefits** | **Entirely Snowflake-native** — no external services, APIs, or infrastructure to manage. Scala scripts provide the relationship ground truth that eliminates the hardest discovery problem. INFORMATION_SCHEMA fills gaps with declared constraints. Snowpark stored procedures handle graph traversal and dynamic SQL generation. Cortex AI adds value where deterministic logic falls short (rare events, validation interpretation). Streamlit in Snowflake provides a zero-infrastructure UI. Snowflake Tasks handle scheduling. Easy to implement incrementally — each component is a stored procedure or Streamlit page. |
| **Drawbacks** | Limited to Snowflake (by design — this is a feature, not a bug). Scala parser is a one-time investment that requires maintenance if script patterns change. Cortex AI is optional, not central — the platform works without it but with less intelligence. |
| **Complexity** | Medium |
| **Implementation Effort** | Medium — the hardest part (relationship discovery) is largely solved by Scala parsing; everything else uses well-established Snowflake patterns |
| **Risk** | Low-Medium — deterministic core with AI augmentation; all data stays in Snowflake; no external dependencies |

### Recommendation

**Option E (Snowflake-Native Pragmatic Reduction)** is the recommended approach.

Rationale:
- The single most important advantage: **relationships are already documented in Scala scripts**. This eliminates the hardest problem in data reduction (relationship discovery) and makes AI-based inference a nice-to-have rather than a must-have
- 100% Snowflake-native means zero external infrastructure, zero API keys, zero network configuration, and full leverage of Snowflake's compute, storage, and governance
- Snowpark stored procedures + dynamic SQL is a proven, debuggable, auditable execution model — no black-box agent behavior
- Cortex AI is used surgically (rare event detection, validation interpretation) where it adds clear value, not as the core engine
- Streamlit in Snowflake provides a production-ready UI with zero deployment complexity
- Snowflake Tasks provide native scheduling with built-in monitoring
- The approach is **easy to implement incrementally** — start with metadata extraction and core reduction, layer AI and UI in subsequent phases
- Data integrity is preserved by design: graph-aware top-down traversal guarantees FK chain completeness before any record is written

---

## 5. AI / Agent Opportunities

### 5.1 Implicit Relationship Discovery (Supplementary to Scala Parsing)

**What:** Use Snowflake Cortex COMPLETE to analyze column names, data types, and value distributions to discover join relationships that are NOT present in either Snowflake DDL or the Scala scripts. Additionally, mine Snowflake ACCESS_HISTORY and QUERY_HISTORY for actual join patterns executed by users and applications.

**Why AI is beneficial:** Scala scripts capture the known ETL relationships, and INFORMATION_SCHEMA captures declared constraints. But there may be ad-hoc joins used by analysts or applications not reflected in either source. Cortex COMPLETE can reason about column naming conventions and cross-reference with ACCESS_HISTORY join patterns to surface these hidden relationships for human review.

### 5.2 Business Rule Extraction

**What:** Analyze Snowflake stored procedures, views, Scala script logic, and data patterns to extract implicit business rules that affect which records are valid together.

**Why AI is beneficial:** Business rules are rarely fully documented. They live in Scala scripts, Snowflake stored procedures, view definitions, and data patterns. Cortex COMPLETE can parse this logic, interpret constraint expressions, and identify co-occurrence patterns in data that suggest business rules (e.g., "orders with status CANCELLED never have a shipment record").

### 5.3 Data Classification and Sensitivity Detection

**What:** Use Snowflake Cortex CLASSIFY and Snowflake's native data classification features to classify columns by data sensitivity (PII, PHI, PCI, confidential) and by semantic type (identifier, measure, dimension, timestamp, flag).

**Why AI is beneficial:** Snowflake has built-in classification capabilities, and Cortex CLASSIFY can supplement them. Automated classification reduces the manual effort of tagging thousands of columns. Sensitivity classification ensures reduction does not inadvertently expose restricted data in non-production environments.

### 5.4 Rare Event and Edge Case Detection

**What:** Identify records that represent statistically rare but analytically important scenarios — outliers, boundary conditions, extreme values, unusual combinations.

**Why AI is beneficial:** Statistical methods alone can identify outliers, but determining which outliers are meaningful requires contextual reasoning. Cortex COMPLETE can interpret column semantics to distinguish between a data error (meaningless outlier) and a genuine rare business event (meaningful outlier that must be preserved).

### 5.5 Scenario Mining and Coverage Analysis

**What:** Analyze production data to identify distinct business scenarios (combinations of status codes, transaction types, customer segments) and ensure the reduced dataset covers all observed scenarios.

**Why AI is beneficial:** The combinatorial space of possible scenarios across multiple categorical columns is enormous. AI can identify which combinations actually occur, which are critical, and which can be represented by proxy — reducing the coverage problem from combinatorial explosion to a manageable set.

### 5.6 Reduction Plan Generation and Explanation

**What:** Given a source schema and reduction targets, generate a human-readable reduction plan that explains the strategy for each table, the expected output size, and the rationale for inclusion/exclusion criteria.

**Why AI is beneficial:** Natural language explanation of reduction decisions builds trust with stakeholders and supports governance review. Cortex COMPLETE excels at translating technical strategies into business-readable narratives that can be displayed in the Streamlit UI.

### 5.7 Validation and Anomaly Reporting

**What:** After reduction, compare statistical profiles, relationship integrity, and scenario coverage between source and reduced datasets, and generate a validation report highlighting any anomalies.

**Why AI is beneficial:** Validation involves interpreting hundreds of metrics across hundreds of tables. Cortex COMPLETE can prioritize which deviations are concerning vs. expected, generating a summary report in the Streamlit UI that reduces the human review burden from hours to minutes.

### 5.8 Schema Change Impact Analysis

**What:** When Snowflake schemas evolve (new tables, dropped columns, altered constraints) or Scala scripts change in Bitbucket, assess the impact on existing reduction profiles and recommend adjustments.

**Why AI is beneficial:** Schema drift is continuous in enterprise environments. Cortex COMPLETE can reason about whether a new column affects reduction logic, whether a dropped constraint changes relationship assumptions, and whether new tables need to be incorporated into existing reduction graphs. This can be triggered by comparing the current metadata snapshot against the stored profile.

---

## 6. Technology Assessment (Snowflake-Native Stack)

Since the platform is 100% Snowflake-native, the technology assessment focuses on Snowflake capabilities and the minimal external tooling needed for Scala script parsing.

### Snowflake Cortex LLM Functions

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| CORTEX.COMPLETE | Strong | Implicit relationship discovery, business rule inference, validation report generation, reduction plan explanation |
| CORTEX.CLASSIFY | Strong | Column sensitivity classification, semantic type detection |
| CORTEX.EXTRACT | Moderate | Extracting structured metadata from unstructured documentation |

**Assessment:** Primary AI layer. All LLM inference runs inside Snowflake — no external API calls, no data egress, no API key management. Cost is Cortex credits, which are predictable and governed by Snowflake billing.

### Snowpark Python

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| Stored Procedures (Python) | Strong | Core reduction engine, graph traversal, dynamic SQL generation, metadata extraction |
| UDFs / UDTFs | Moderate | Statistical profiling functions, distribution comparison helpers |
| DataFrame API | Strong | Data profiling, sampling, and transformation within stored procedures |

**Assessment:** The execution backbone of the platform. Snowpark Python stored procedures allow complex logic (graph traversal, topological sort, cycle detection) while executing inside Snowflake's compute infrastructure. Dynamic SQL generation for reduction queries runs here.

### Snowflake INFORMATION_SCHEMA & ACCOUNT_USAGE

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| TABLE_CONSTRAINTS, KEY_COLUMN_USAGE | Strong | Declared PK/FK extraction |
| COLUMNS view | Strong | Column inventory, data types, nullable flags |
| TABLE_STORAGE_METRICS | Strong | Row counts, storage sizes for volume profiling |
| ACCESS_HISTORY (ACCOUNT_USAGE) | Strong | Join pattern mining — discover which tables are actually joined and on which columns |
| QUERY_HISTORY (ACCOUNT_USAGE) | Moderate | Supplementary join pattern discovery from SQL query text |

**Assessment:** The metadata foundation. INFORMATION_SCHEMA provides declared relationships; ACCESS_HISTORY provides observed join behavior. Together with Scala script parsing, these three sources create a comprehensive relationship map.

### Streamlit in Snowflake

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| Interactive UI | Strong | Reduction profile configuration, job execution, validation review |
| Data visualization | Strong | Distribution comparison charts, dependency graph visualization |
| Session state | Strong | Multi-step workflow (configure → review → execute → validate) |
| Role-based access | Strong | Inherits Snowflake RBAC — no separate auth system needed |

**Assessment:** The UI layer. Zero deployment infrastructure. Inherits Snowflake security. Supports all required workflows: profile management, job execution, validation review, and audit trail browsing.

### Snowflake Tasks

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| Scheduled execution | Strong | Scheduled reduction runs (nightly, weekly) |
| Task graphs (DAGs) | Strong | Multi-step reduction pipeline: profile → reduce → validate → certify |
| Monitoring and alerting | Strong | Built-in task history, failure detection |

**Assessment:** Native orchestration. Task graphs model the reduction pipeline naturally: metadata refresh → reduction execution → validation → notification. No external orchestrator needed.

### Snowflake Dynamic Tables

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| Declarative transformation | Moderate | Could model reduced tables as dynamic tables that auto-refresh when source data changes |
| Incremental refresh | Moderate | Supports incremental reduction scenarios |

**Assessment:** Interesting for incremental reduction use cases but not the primary execution model. Worth evaluating for scenarios where reduced datasets must stay in sync with production.

### Python (External — Scala Parser Only)

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| Bitbucket API client | Required | Clone/fetch Scala scripts from Bitbucket repositories |
| Scala/Spark AST parsing | Required | Extract join metadata (tables, columns, join types) from Scala code |
| NetworkX (graph library) | Strong | Build and analyze the dependency graph — topological sort, cycle detection |

**Assessment:** The only component that runs outside Snowflake. A lightweight Python script that parses Scala files from Bitbucket and loads the extracted relationship metadata into a Snowflake configuration table. Can run as a Snowpark stored procedure with external access integration, or as an external script that writes results to Snowflake via the Snowflake connector.

### Snowflake Data Metric Functions (DMFs)

| Capability | Fit for EDRP | Role in Platform |
|-----------|-------------|-----------------|
| Custom data quality checks | Strong | Post-reduction validation — attach DMFs to reduced tables to continuously monitor integrity |
| Built-in metrics (null rate, uniqueness, freshness) | Strong | Automated quality baseline for reduced datasets |
| Alerting on violations | Strong | Notify when reduced dataset quality degrades |

**Assessment:** Strong fit for the validation layer. DMFs can be attached to reduced tables to enforce ongoing quality — not just at reduction time but continuously. Replaces the need for Great Expectations or Soda.

### Technologies Removed from Consideration

| Technology | Reason for Removal |
|-----------|-------------------|
| OpenAI / Claude (external APIs) | Not needed — Snowflake Cortex provides equivalent LLM capabilities natively without data egress |
| LangGraph / CrewAI | Not needed — the recommended approach uses deterministic Snowpark logic, not agentic orchestration |
| Great Expectations / Soda | Replaced by Snowflake Data Metric Functions (DMFs) — native, no external infrastructure |
| dbt | Optional complement but not required — Snowpark stored procedures and dynamic SQL handle the transformation layer |
| SQLAlchemy / multi-platform connectors | Not needed — Snowflake-only platform |

---

## 7. Risks and Challenges

### Integrity Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Orphaned foreign keys in reduced dataset | Queries and joins fail; application errors | High (if not addressed architecturally) | Graph-aware top-down reduction that traverses FK chains from parent to child; mandatory post-reduction integrity validation |
| Circular dependency deadlocks during reduction | Reduction process hangs or produces inconsistent subsets | Medium | Cycle detection in dependency graph; break cycles with configurable pivot tables; two-pass reduction for circular references |
| Implicit relationships missed | Reduced dataset breaks Scala script logic that depends on undeclared FKs | Medium (lower due to Scala parsing) | Three-source relationship discovery: Scala scripts + INFORMATION_SCHEMA + ACCESS_HISTORY; human review of combined graph |

### Sampling Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Statistical distribution collapse at low sample rates | ML models trained on reduced data behave differently than production | High at <5% sampling | Stratified sampling; distribution-aware sampling with KL-divergence monitoring; minimum sample size thresholds per stratum |
| Rare events eliminated by random sampling | Edge case testing impossible; fraud models undertrained | High | Explicit rare event detection and mandatory inclusion; oversampling of tail distributions; scenario coverage validation |
| Temporal sequence breaks | Time-series analysis, event replay, and SCD logic fail | Medium | Temporal-aware sampling that preserves event chains; window-based inclusion that keeps related events together |

### Performance Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Full table scans on multi-TB Snowflake tables for profiling | Excessive warehouse credit consumption and wall-clock time | High | Sample-based profiling; leverage TABLE_STORAGE_METRICS for row counts; use TABLESAMPLE for statistical profiling; run on dedicated warehouse with auto-suspend |
| Cortex function cost scales with schema size | Operational cost exceeds budget | Medium | Batch Cortex calls; cache results in metadata tables; use Cortex selectively (rare events, validation) not on every column |
| Scala parser fails on complex or dynamic code patterns | Incomplete relationship graph | Medium | Human review of unparseable scripts; fallback to ACCESS_HISTORY for relationship validation; manual override capability in UI |

### Cost Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Cortex credit consumption for large schema analysis | Budget overrun | Medium | Use Cortex selectively; cache AI results in metadata tables; profile once, reduce many |
| Warehouse credit costs for profiling and reduction | Warehouse costs exceed expectations | Medium | Use TABLESAMPLE for profiling; dedicated warehouse with auto-suspend; off-peak scheduling via Snowflake Tasks |
| Storage costs for reduced datasets and metadata | Incremental storage growth | Low | Snowflake lifecycle policies; reduced dataset TTLs; Transient tables for ephemeral reduced datasets |

### Governance Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| PII/PHI leaks into non-production via reduced datasets | Regulatory violation; breach notification | High (if not addressed) | Leverage Snowflake native classification and tag-based masking policies; mandatory sensitivity scan using Cortex CLASSIFY before reduction; dynamic data masking on reduced schema |
| Reduced dataset lineage gaps | Audit failure; inability to trace data provenance | Medium | Mandatory lineage metadata capture in EDRP metadata tables; reduction decision logging with source record IDs |
| Unauthorized access to reduction profiles | Exposure of production schema details and business rules | Low | Snowflake RBAC on EDRP schema; database roles for profile management vs. execution vs. review |

### Compliance Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Cross-border data movement during reduction | GDPR, data sovereignty violations | Low (all data stays in Snowflake, same account/region) | Ensure reduced datasets are created in the same Snowflake region as source; use Snowflake replication only if cross-region is required |
| Retention policy violations in reduced datasets | Reduced dataset contains records that should have been purged | Low | Retention policy integration; date-based exclusion filters; respect Snowflake Time Travel and retention settings |

---

## 8. Recommended Approach

### High-Level Solution

Build the Enterprise Data Reduction Platform as a **Snowflake-native, metadata-driven, AI-augmented reduction engine** with the following architectural pillars:

**Pillar 1 — Snowflake Metadata Layer + Scala Script Parser**  
A metadata extraction layer that combines three sources into a unified relationship map: (1) Snowflake INFORMATION_SCHEMA for declared PKs, FKs, column definitions, and constraints, (2) Scala script parsing from Bitbucket for actual join relationships used by downstream ETL, and (3) Snowflake ACCESS_HISTORY for observed join patterns from queries. The output is a set of Snowflake metadata tables that describe every table, column, and relationship in scope.

**Pillar 2 — Relationship Dependency Graph**  
A graph representation stored in Snowflake tables, built from Pillar 1's metadata. Tables are nodes; joins/FKs are directed edges. The graph supports topological sorting (to determine reduction order), cycle detection (to handle circular dependencies), and depth analysis (to identify root entities vs. leaf entities). Implemented in Snowpark Python using NetworkX within a stored procedure.

**Pillar 3 — Deterministic Reduction Engine**  
The core engine, implemented as Snowpark Python stored procedures that generate and execute dynamic SQL. The engine: (a) starts from root entities and samples using stratified sampling, (b) traverses the dependency graph top-down to select child records that match sampled parent keys, (c) applies rare-event protection rules to include mandatory records, (d) preserves temporal chains by selecting complete event sequences, (e) writes reduced data to a target schema/database using CREATE TABLE AS SELECT (CTAS) with the computed record sets.

**Pillar 4 — Cortex AI Augmentation (Selective)**  
Cortex LLM functions used surgically for: (a) rare event detection — Cortex COMPLETE analyzes column value distributions and identifies which outliers represent meaningful business events, (b) validation interpretation — Cortex COMPLETE summarizes validation results into human-readable reports, (c) supplementary relationship discovery — Cortex COMPLETE analyzes column names and types to suggest relationships not found in Scala scripts or DDL. AI is an enhancer, not the engine.

**Pillar 5 — Validation and Certification (DMFs + Snowpark)**  
Post-reduction validation implemented as: (a) Snowpark stored procedures that compare source vs. reduced dataset statistics (distribution KL divergence, cardinality ratios, null rates, scenario coverage), (b) referential integrity checks that verify zero orphaned FKs across all relationships, (c) Snowflake Data Metric Functions (DMFs) attached to reduced tables for ongoing quality monitoring. A reduced dataset is only certified for use after passing all validation thresholds.

**Pillar 6 — Streamlit in Snowflake UI + Snowflake Tasks Orchestration**  
A Streamlit in Snowflake application for: (a) reduction profile configuration (select databases, schemas, tables; set target ratios; review discovered relationships; approve/override), (b) job execution and monitoring, (c) validation report review with distribution comparison charts, (d) audit trail browsing. Snowflake Tasks handle scheduled/recurring reduction runs with built-in DAG support and monitoring.

### Operating Model

- **Profile Once, Reduce Many:** Metadata is extracted and the relationship graph is built once (with incremental refresh). Multiple reduction jobs can run against the same profile with different target ratios
- **Human-in-the-Loop for Relationships:** The parsed Scala relationships and INFORMATION_SCHEMA constraints are displayed in the Streamlit UI for human review and approval before the first reduction run
- **Validate Always:** Every reduced dataset undergoes automated validation. No dataset is certified for use without passing integrity and distribution thresholds
- **100% Snowflake-Native:** All data stays in Snowflake. All compute runs on Snowflake warehouses. All AI runs via Cortex. All UI runs via Streamlit in Snowflake. The only external touchpoint is Bitbucket API access for Scala script retrieval

---

## 9. Proposed Next Phases

### Phase 1: Discovery (Current Phase)

**Objective:** Fully understand the problem domain, Snowflake environment, Bitbucket Scala script landscape, and stakeholder requirements before committing to architecture.

**Deliverables:**
- Solution Discovery Report (this document)
- Snowflake environment inventory (databases, schemas, table counts, editions, features)
- Bitbucket Scala script inventory (repo count, coding patterns, parsability assessment)
- Questions resolved and assumptions validated
- Prioritized database/schema list for initial scope

**Approval Checkpoint:** Stakeholder review and sign-off on discovery findings and recommended approach before architecture design begins.

---

### Phase 2: Architecture Design

**Objective:** Define the Snowflake-native technical architecture, metadata model, reduction algorithms, and component specifications.

**Deliverables:**
- Snowflake-native architecture document (all six pillars)
- EDRP metadata schema design (tables for relationships, profiles, job runs, validation results)
- Scala parser specification (supported patterns, output format, edge case handling)
- Reduction algorithm specification (graph traversal strategy, sampling methods, rare event protection)
- Cortex AI integration points (which functions, where invoked, prompt templates)
- Snowflake RBAC design for EDRP (roles, privileges, database structure)
- Streamlit UI wireframes and page flow

**Approval Checkpoint:** Architecture review. All component interfaces agreed. Snowflake resource requirements approved (warehouses, databases, roles).

---

### Phase 3: Detailed Design

**Objective:** Produce implementation-ready specifications for each Snowpark stored procedure, Streamlit page, Snowflake Task, and metadata table.

**Deliverables:**
- Stored procedure specifications (inputs, outputs, logic, error handling) for: metadata extractor, Scala parser, graph builder, reduction engine, validator
- Snowflake DDL for all EDRP metadata tables
- Streamlit page designs (profile config, relationship review, job execution, validation dashboard, audit trail)
- Snowflake Task DAG design (metadata refresh → reduction → validation → notification)
- DMF definitions for post-reduction quality monitoring
- Test strategy: unit tests for stored procedures, integration tests for end-to-end reduction, validation acceptance criteria

**Approval Checkpoint:** Technical lead review. Design walkthrough. Test strategy approved.

---

### Phase 4: Implementation

**Objective:** Build the platform incrementally within Snowflake.

**Deliverables:**
- Increment 1: EDRP metadata schema + Snowflake INFORMATION_SCHEMA extractor (stored procedure)
- Increment 2: Scala script parser (Python) + Bitbucket integration + relationship metadata loader
- Increment 3: Dependency graph builder (Snowpark + NetworkX) — topological sort, cycle detection
- Increment 4: Core reduction engine (Snowpark stored procedure) — graph-aware CTAS with stratified sampling
- Increment 5: Rare event detection + temporal chain preservation
- Increment 6: Validation framework (Snowpark stored procedures + DMFs)
- Increment 7: Streamlit in Snowflake UI (profile management, job execution, validation review)
- Increment 8: Snowflake Tasks orchestration (scheduled reduction DAG)
- Increment 9: Cortex AI augmentation (optional — rare event intelligence, validation summaries)

**Approval Checkpoint:** Demo at the end of each increment. Stakeholder acceptance. Reduction produces valid output after Increment 4.

---

### Phase 5: Testing and Validation

**Objective:** Validate the platform against real Snowflake databases with production-representative schemas and volumes. Certify that reduced datasets meet all fidelity requirements.

**Deliverables:**
- Integration test results across target Snowflake databases
- Performance test results (reduction time vs. table volume, profiling time vs. schema size, warehouse credit consumption)
- Fidelity validation reports (statistical distribution comparison, FK integrity checks, scenario coverage)
- Edge case testing results (circular dependencies, large hierarchies, sparse tables, empty tables, VARIANT columns)
- Scala parser accuracy report (parsed relationships vs. manual review)
- User acceptance testing sign-off from dataset consumers (Dev, QA, Data Science)

**Approval Checkpoint:** QA sign-off on test results. UAT sign-off from business stakeholders. Performance benchmarks met within warehouse budget.

---

### Phase 6: Production Rollout

**Objective:** Deploy the platform in the production Snowflake account, onboard initial databases, train users, and establish operational support.

**Deliverables:**
- Production deployment (EDRP database, schemas, stored procedures, Streamlit app, Tasks)
- Initial database onboarding (first 2-3 databases reduced and validated)
- User training sessions and Streamlit-embedded help documentation
- Operational runbook (how to add new databases, how to update Scala parser when scripts change, how to troubleshoot failed reductions)
- Snowflake Task monitoring and alerting configuration
- Feedback collection and improvement backlog

**Approval Checkpoint:** Production readiness review. Go-live approval. Post-deployment health check at 30 days.

---

## Appendix: Glossary

| Term | Definition |
|------|-----------|
| **EDRP** | Enterprise Data Reduction Platform — the system being designed |
| **Reduction** | The process of creating a smaller dataset from a larger one while preserving specified fidelity characteristics |
| **Reduction Profile** | A configuration stored in Snowflake that defines target ratios, mandatory inclusion rules, and quality thresholds for a specific database/schema |
| **Dependency Graph** | A directed graph stored in Snowflake tables representing tables as nodes and join/FK relationships as directed edges |
| **Scala Script Parsing** | Automated extraction of join metadata (table names, join columns, join types) from Scala/Spark code in Bitbucket |
| **Fidelity** | The degree to which a reduced dataset accurately represents the statistical and structural properties of the source |
| **Rare Event** | A record or combination of values that occurs with low frequency but has high analytical or business significance |
| **Stratified Sampling** | A sampling technique that divides a population into subgroups (strata) and samples from each to preserve distribution |
| **KL Divergence** | Kullback-Leibler divergence; a statistical measure of how one probability distribution differs from another |
| **Orphaned FK** | A foreign key value in a child table that has no matching primary key in the parent table |
| **CTAS** | CREATE TABLE AS SELECT — the Snowflake pattern used to materialize reduced datasets |
| **DMF** | Data Metric Function — Snowflake-native quality monitoring attached to tables |
| **Cortex COMPLETE** | Snowflake's built-in LLM function for text generation, analysis, and reasoning |

---

**END OF DISCOVERY REPORT**

**Platform:** Snowflake-Native (100%)  
**Status:** Awaiting stakeholder review and approval before proceeding to Phase 2 (Architecture Design).
