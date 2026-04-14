# Parliamentary Discourse Knowledge Graph

Parliamentary Discourse Knowledge Graph (PDKG) is an NLP-driven data engineering framework designed to model, enrich, and analyze parliamentary debate transcripts from the Spanish Congress of Deputies (Congreso de los Diputados de España) through a structured knowledge graph approach.

The system ingests official session diaries (Diarios de Sesiones), processes them through a Bronze/Silver/Gold medallion architecture, enriches the discourse using Large Language Models (LLMs), and materializes structured relationships in a Neo4j knowledge graph.

The primary objective is to enable reproducible, scalable, and longitudinal computational analysis of political discourse complexity, thematic evolution, narrative framing, and party positioning in Spain.

## Project Scope

This project transforms unstructured parliamentary transcripts into structured, queryable analytical assets.

Core capabilities include:

- **Speaker Resolution Engine**: Robust identification of MPs and Government members using a person-centric deduplication approach.
- Discourse normalization and metadata extraction (aggressive removal of titles and treatments).
- **Human-in-the-loop Validation**: Integrated review workflow for unresolved or ambiguous speakers.
- Topic modeling and thematic clustering.
- Sentiment and discourse framing analysis.
- Linguistic and structural complexity measurement.
- Knowledge graph construction.
- Longitudinal political discourse analysis.

The current implementation features a highly specialized Speaker Resolution layer that handles OCR errors, name variants (swaps, subsets), and separates personal identity from institutional roles.

## Data Architecture (Bronze / Silver / Gold)

The project follows a medallion architecture to ensure traceability, idempotency, and analytical integrity.

### Bronze Layer (`data/bronze`)

- Raw HTML ingestion of official parliamentary session records
- Immutable, append-only storage
- Strict versioning
- Documents are never deleted
- Reprocessing occurs only when checksum differences are detected

### Silver Layer (`data/silver`)

- Fully normalized and structured datasets in Parquet format
- One table per entity (sessions, speakers, interventions, etc.)
- Partitioned by legislature (e.g. `legislature=15/`)
- Fully idempotent and regenerable from Bronze
- Only the latest active version of each document is retained
- No embeddings or aggregated analytical artifacts are stored at this layer

### Gold Layer

- LLM-driven enrichment and semantic augmentation
- Analytical queries executed locally using DuckDB
- Relationship modeling and graph materialization in Neo4j
- Logical layer (no exclusive folder representation)

This layer enables:

- Speaker-to-topic relationships
- Inter-party narrative comparison
- Argument network mapping
- Temporal graph evolution analysis

## Document State & Idempotency

DuckDB acts as the embedded state control engine for document processing.

The system:

- Tracks processed documents
- Compares checksums to detect modifications
- Automatically reprocesses altered transcripts
- Applies a full replacement strategy in Silver and Gold layers when required

This guarantees consistency and prevents duplication or divergence across analytical stages.

## Pre-execution Backup

To ensure data safety before any transformation, the system includes a **Backup Layer**. 

Before each execution, the `BackupManager`:
- Creates a timestamped directory in `backups/YYYY-MM-DD_HHMMSS/`.
- Copies all `.parquet` files from Bronze and Silver layers.
- Copies `government_manual_mapping.csv` reference files.
- Generates a `manifest.json` with metadata (file paths and sizes) of the backed-up assets.

This step is non-blocking: if a backup fails or data is missing, the pipeline continues after logging a warning.

## Comparison Layer (Validation Tool)

To ensure analytical consistency during development and refactoring, the project includes a **Comparison Layer**.

The `ParquetComparator` allows comparing a specific backup against the current state of the project:
- **Schema Validation**: Detects if columns have been added, removed, or renamed.
- **Row Count**: Reports mismatches in the number of records.
- **Content Integrity**: Uses normalized row hashing to identify exactly which rows are identical, missing, or new.
- **Reporting**: Generates a `summary.json` report in `comparison_reports/{timestamp}/`.

This tool is used as a standalone validator to guarantee that changes in the pipeline logic do not unintentionally alter the resulting datasets.

## Experimental Streaming Pipeline

The project includes an experimental in-memory extraction pipeline that runs concurrently with session scraping to reduce total execution time.

### Flag Hierarchy
- `--disable-streaming`: **Authoritative**. Forces pure batch mode, disabling all streaming logic/validation.
- `--experimental-streaming`: Optional explicit shadow-mode flag (**now active by default** unless `--disable-streaming` is used).
- `--use-streaming-candidate`: Opts into using the streaming results for downstream enrichment (independent of `--experimental-streaming`).
- `--streaming-confidence-threshold`: Sets a custom confidence gate for candidate promotion (requires `--use-streaming-candidate`).

### Modes of Operation

| Mode | CommandLine Arguments | Official Outputs | Candidate Artifact | Parity Report | Downstream Source |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Default (Shadow)** | (none) | Updated (Batch) | Generated | Generated | Official Batch |
| **Pure Batch** | `--disable-streaming` | Updated (Batch) | Not generated | Not generated | Official Batch |
| **Strict Match** | `--use-streaming-candidate` | Updated (Batch) | Generated | Generated | Candidate (if 100% match) |
| **Threshold** | `--use-streaming-candidate --streaming-confidence-threshold 0.95` | Updated (Batch) | Generated | Generated | Candidate (if score >= 0.95) |
| **Explicit Promotion**| `--use-streaming-candidate --promote-streaming` | Updated (Batch) | Generated | Generated | Candidate (if 100% match) |

> [!NOTE]
> Even when a candidate is promoted, the **official batch output** (`interventions_raw.parquet`) is always updated and preserved as the system of record in `data/silver/`.

### Generated Artifacts
- **Parity Report**: `data/validation/legislature={term}/parity_report.json`
  - Contains global, document, and row-level parity statuses + confidence metrics.
- **Validation Run Summary**: `data/validation/legislature={term}/validation_run_summary.json`
  - A compact, operator-focused summary of the individual run and the resulting source selection decision.
- **Validation Run History**: `data/validation/validation_run_history.jsonl`
  - An append-only log of all streaming validation runs, used for longitudinal analysis of confidence and parity trends.
- **Streaming Candidate**: `data/validation/legislature={term}/interventions_streaming_candidate.parquet`
  - The dataset produced by the experimental pipeline.

## Streaming Promotion Readiness

### Validation Status
- sandbox full reprocess completed
- FULL_MATCH achieved
- confidence_score = 1.0
- no mismatched documents

### Recommended Policy
- recommended mode: strict_match
- threshold mode: optional / experimental
- batch remains system of record

### Promotion Criteria
- strict match parity
- row/document/global equality
- no fallback triggered in validated scenarios

### Fallback & Safety Model
- batch remains authoritative
- any mismatch falls back to batch
- no destructive changes introduced

### Decision Outcome
- Streaming candidate: VALIDATED
- Recommended policy: strict_match
- Default production path: batch (unchanged)
- Promotion status: pending controlled rollout

Streaming is validated but not yet promoted as the default system of record.

## Controlled Rollout (Phase 16)

### Shadow Mode
- streaming runs in parallel with batch (via `--experimental-streaming`)
- does NOT affect downstream outputs
- generates validation artifacts

### When to use it
- production monitoring
- confidence tracking
- regression detection

### Guarantees
- zero risk to official datasets
- full fallback always active
- no behavior change

## Controlled Promotion (Phase 17)

### Explicit Promotion
Streaming is promoted to the downstream source for a specific run only when:
1. `--promote-streaming` is explicitly enabled.
2. Strict match parity (`strict_match`) is successfully achieved.

### How to activate it
```bash
python src/main.py --experimental-streaming --use-streaming-candidate --promote-streaming
```

### Guarantees
- **Safe Fallback**: If strict match fails, the pipeline automatically falls back to the authoritative batch source.
- **No Data Corruption**: Promotion only affects source selection; official output paths and schemas remain unchanged.
- **Auditability**: Every promotion decision is logged and recorded in the validation artifacts.
- **Batch Authoritative**: Batch remains the official system of record and the default baseline for all processing.

## Default Shadow Mode (Phase 20A)

Starting with Phase 20A, the streaming validation pipeline runs in **shadow mode by default** in every execution. This ensures continuous monitoring of parity and confidence levels without affecting the official system of record.

### Key Features
- **Observability by Default**: Every run generates a parity report and validation artifacts.
- **Batch-Authoritative**: Downstream processing (Silver enrichment) still uses the batch-extracted source by default.
- **Strict Isolation**: A new authoritative switch `--disable-streaming` is provided for operators who need to bypass all experimental logic.

### Command Examples

**1. Standard run (Shadow Mode active by default):**
```bash
python src/main.py
```
- Generates `validation_run_summary.json` and `parity_report.json`.
- Uses Batch for downstream.

**2. Pure Batch run (Disables all streaming logic):**
```bash
python src/main.py --disable-streaming
```
- No validation artifacts generated.

**3. Strict promotion (Opt-in to use streaming source):**
```bash
python src/main.py --use-streaming-candidate --promote-streaming
```

**4. Threshold-gated promotion:**
```bash
python src/main.py --use-streaming-candidate --streaming-confidence-threshold 0.99
```

## Default Guarded Promotion (Phase 20B)

In Phase 20B, the pipeline moves from shadow monitoring to **Guarded Production**. The streaming candidate is now automatically selected for downstream processing if it meets strict parity requirements.

### Key Logic
- **Automatic Selection**: If `strict_match` passes (`FULL_MATCH`), the streaming candidate is promoted as the source for Silver enrichment.
- **Authoritative Fallback**: If strict match fails or is skipped, the system automatically falls back to the official batch source with a precise- `fallback_reason`: "strict_match_failed", "validation_skipped", "candidate_missing".

### Batch Sampling Strategy (Phase 22A)

To optimize execution while maintaining safety, the pipeline supports conditional batch execution:
- `--batch-strategy always`: (Default) Batch runs on every execution.
- `--batch-strategy sampled`: Runs batch every Nth execution, where N is set by `--batch-sample-every` (default: 5). The rule is `(total_runs + 1) % N == 0`.
- `--batch-strategy disabled_only_if_stable`: Skips batch only if `stable_streaming` is `True` in `promotion_monitoring_summary.json`.

**Safety Features**:
- **Authoritative Fallback**: If the monitoring summary is missing or malformed, the system defaults to `run_batch=True`.
- **Validation Gating**: If batch is skipped, `parity_status` is explicitly set to `SKIPPED` and the run is not counted as evaluable for stability metrics.
- **Kill Switch**: `--disable-streaming` remains authoritative and disables all streaming logic.

### Safety Guarantees
- **Reversibility**: Use `--batch-strategy always` to force batch execution and full parity validation at any time.
- **Observability**: When batch is skipped, `validation_run_summary.json` records `batch_executed: false` and the specific `batch_skip_reason`.
- **Gated Skipping**: Batch cannot be skipped if recent pipeline history shows any parity failures.
- **System of Record**: The official batch outputs (`interventions_raw.parquet`) are always written and preserved, regardless of whether the candidate was promoted.
- **Kill Switch**: The `--disable-streaming` flag remains authoritative and bypasses all streaming logic.

### Validation Modes (Phase 22B)

To ensure metric integrity and observability correctness, the system explicitly distinguishes between validation outcomes:

- **full_batch_validation**: The run was fully validated against the authoritative batch source. These runs are **evaluable** and count towards accuracy metrics (success rate, fallback rate, confidence).
- **skipped_batch**: Batch execution was intentionally skipped (e.g., via sampling), and the run does not meet stability criteria for inferred validation. Metrics are not evaluable.

## Streaming-Only Validation Classification (Phase 23A)

Introduced in Phase 23A, `streaming_only_validation` is an inferred validation category used when batch execution is skipped but the pipeline has demonstrated high historical stability.

- **Usage**: Automatically triggered when `batch_strategy` skips execution but `stable_streaming` is true and historical accuracy metrics (`strict_match`, `confidence`) are high.
- **Safety**: This mode is strictly classificatory. It **does not** trigger automatic promotion or alter source selection. It serves to track "likely valid" runs without requiring full batch backing.
- **Parity Status**: Such runs are marked as `INFERRED_VALID` instead of `MATCH` or `SKIPPED`.
- **Reporting**: These runs are reported separately from `full_batch_validation` and are not included in the core stability metrics used for promotion decisions.

This decoupling ensures that "skipped" runs do not artificially inflate or deflate the observed stability of the streaming pipeline.

## Inferred Promotion Policy (Phase 23B)

Introduced in Phase 23B, Inferred Promotion allows strictly controlled, opt-in promotion for runs classified as `streaming_only_validation`.

- **Opt-in Only**: Requires the `--allow-inferred-promotion` flag.
- **Authoritative Kill Switch**: If `--disable-streaming` is used, it overrides and disables inferred promotion.
- **Rigorous Eligibility**: Promotion only occurs if `stable_streaming` is true AND historical metrics meet strict thresholds:
  - `strict_match_success_rate` == 100%
  - `fallback_rate` == 0%
  - `avg_confidence_score` >= 0.95
- **Promotion Result**: If successful, the run is marked as `PROMOTED_INFERRED`.
- **Defensive Safety**: If the monitoring summary is missing, malformed, or inconsistent, the system forces a `FALLBACK` with reason `missing_or_invalid_stability_context`.
- **System of Record**: Full batch-backed validation remains the strongest evidence path. Inferred promotion is an operational optimization for stable streams.
- **Rules Integrated with Adaptive Batch**: These eligibility metrics also drive Rule B and Rule D of the Adaptive Batch Strategy.

## Adaptive Batch Degradation (Phase 24)

Introduced in Phase 24, Adaptive Batch transforms batch execution from a structural dependency into an adaptive safety mechanism. Instead of running on every execution, batch is triggered dynamically based on the observed stability and freshness of the streaming pipeline.

### Adaptive Decision Rules
The system uses the following deterministic rules to decide if `run_batch` is required:

- **Rule A (Stability)**: Batch **must run** if `stable_streaming` is `false`.
- **Rule B (Recent Fallback)**: Batch **must run** if there has been any recent `FALLBACK` in the stability window (specifically if `rolling_metrics_last_10.fallback_rate > 0`).
- **Rule C (Freshness)**: Batch **must run periodically** to refresh the baseline evidence. If the number of runs since the last `full_batch_validation` exceeds the `--batch-freshness-window` (default: 5), batch is triggered.
- **Rule D (Ready)**: Batch is **safely skipped** only if streaming is stable, no recent fallbacks are detected, and the baseline evidence is fresh.

### Safety & Observability
- **Deterministic**: Decisions are based strictly on historical logs and CLI parameters; no randomness is involved.
- **Transparent**: Every decision is logged and recorded in `validation_run_summary.json` via fields like `batch_required_by_rule` and `adaptive_batch_reason`.
- **Safe Fallback**: If the monitoring context (`promotion_monitoring_summary.json`) is missing or invalid, the system defaults to `run_batch=True`.
- **Reversible**: Operators can force full validation at any time using `--batch-strategy always`.

### Command Example
```bash
python src/main.py --batch-strategy adaptive --batch-freshness-window 10
```

### Backward Compatibility
Explicit flags (`--use-streaming-candidate`, `--promote-streaming`) are retained to support legacy workflows and allow operators to explicitly state their intent, even though candidate evaluation is now the guarded default.

## Batch Retirement Readiness (Phase 25)

Introduced in Phase 25, the Batch Retirement Readiness assessment is an observability layer that determines if the system can safely transition batch processing from a synchronous safety mechanism into a secondary role (such as periodic audit, asynchronous verification, or recovery-only mode).

### Readiness Criteria
The system assesses retirement readiness based on a deterministic rule applied to the recent history (last 20 runs):
- **Stability**: `stable_streaming` must be `true` (100% success rate in the window).
- **Recent Smoothness**: Zero `FALLBACK` events in the recent window.
- **Baseline Freshness**: The time since the last `full_batch_validation` must be within a safe threshold (currently < 30 runs). This ensures we still have relatively recent ground-truth evidence.
- **Exercised Adaptive Logic**: The system must have successfully skipped batch at least once in the recent window (`recent_adaptive_skip_count > 0`), proving that the adaptive logic is operational.

### Assessment Outputs
The `promotion_monitoring_summary.json` includes a `batch_retirement_readiness` section with:
- **`ready_for_batch_retirement`**: Boolean flag indicating if all criteria are met.
- **`recommended_next_mode`**:
    - `move_batch_to_periodic_audit`: All criteria met; batch can be decoupled from the main path.
    - `keep_adaptive_batch`: One or more criteria failed; batch should remain in its adaptive role.
- **`retirement_reason`**: Detailed explanation of why the current mode is recommended.

### Safety Guarantees
- **Assessment-Only**: This phase does **not** modify the runtime behavior of `src/main.py`. It provides decision-support metrics for future phases.
- **Periodic Evidence**: The freshness threshold ensures that ground-truth validation is never abandoned completely, even in highly stable environments.

## Periodic Audit Batch Mode (Phase 26)

Introduced in Phase 26, the Periodic Audit strategy officially transitions batch execution from a synchronous safety mechanism into a background audit role. This mode prioritizes the streaming pipeline for production while ensuring that ground-truth validation is performed periodically or immediately upon detection of issues.

### Audit Trigger Rules
The system evaluates the following deterministic rules to decide if a batch audit is required:

- **Rule A (Stability Check)**: Batch **must run** if `stable_streaming` is `false`.
- **Rule B (Degradation Check)**: Batch **must run** if any recent fallback has been detected in the stability window (`rolling_metrics_last_10.fallback_rate > 0`).
- **Rule C (Freshness Check)**: Batch **must run periodically** to refresh validation evidence. If the number of runs since the last `full_batch_validation` meets or exceeds `--batch-audit-every` (default: 10), an audit is triggered.
- **Rule D (Ready)**: Batch is **safely skipped** only if streaming is stable, no recent fallbacks are detected, and the audit window has not been reached.

### Safety & Recovery
- **Safe Fallback**: If the monitoring summary (`promotion_monitoring_summary.json`) is missing, malformed, or incomplete, the system forces a batch audit for safety.
- **Audit Decision Metadata**: Every decision is recorded in `validation_run_summary.json` via fields:
  - `batch_audit_due`: Boolean indicating if an audit was required.
  - `batch_audit_reason`: Specific rule that triggered the audit.
  - `periodic_audit_mode_active`: Boolean flag confirming the strategy was in use.

### Command Example
```bash
python src/main.py --batch-strategy periodic_audit --batch-audit-every 20
```

### Command Examples

**1. Standard Guarded Run (Default):**
```bash
python src/main.py
```
- Performs streaming validation.
- Selects streaming candidate if `FULL_MATCH`.
- Falls back to batch otherwise.

**2. Forced Batch Run (Kill Switch):**
```bash
python src/main.py --disable-streaming
```

**3. Explicit Guarded Run:**
```bash
python src/main.py --use-streaming-candidate --promote-streaming
```

## Local Development Environment

### Requirements

- Python 3.11
- Poetry (dependency management)
- Docker Desktop (for Neo4j)
- Windows 11 (primary development environment)

## Dependency Management with Poetry

The project uses Poetry for professional dependency and environment management.
Virtual environments are created in-project (`.venv`).

Install dependencies:

```bash
poetry install
```

Activate the environment (optional):

```bash
poetry shell
```

Run development tools:

```bash
poetry run pytest
poetry run black src tests
poetry run ruff check src tests
poetry run mypy src
```

## Execution

Run the end-to-end pipeline for the current legislature:

```bash
poetry run python src/main.py
```

## Graph Layer (Neo4j)

The Gold layer relies on Neo4j for graph persistence and relationship analytics.

Once the Neo4j container is running locally:

- **Browser Interface:** http://localhost:7474
- **Bolt Connection:** bolt://localhost:7687

Detailed Docker setup instructions will be added in future iterations.

## LLM Integration

The enrichment layer integrates external Language Model APIs to perform advanced reasoning tasks on parliamentary discourse:

- Topic extraction
- Argument summarization
- Language detection
- Discourse framing identification

Model configuration is designed to support low-cost development usage and future production-grade deployment.

## Speaker Resolution & Quality Control

The project implements a sophisticated resolution engine to map textual labels (e.g., "El señor PRESIDENTE DEL GOBIERNO") to unique person identities.

### Person-Centric Deduplication
To ensure analytical integrity, the system enforces a **single canonical entry per person**. It uses an iterative consolidation process with the following rules:
- **Name Swapping**: Correctly identifies that "PEREZ SANCHEZ" and "SANCHEZ PEREZ" are the same person.
- **Subset Matching**: Unifies "GRANDE MARLASKA" and "GRANDE MARLASKA GOMEZ".
- **Fuzzy/OCR Matching**: High-confidence fuzzy matching (85%+) catches OCR errors and joined words (e.g., "SANCHEZCASTEJON").

### Manual Government Mapping (`data/reference/`)
The system maintains a `government_manual_mapping.csv` for each legislature. This dictionary:
- Separates **Personal Identity** (`canonical_person_key`) from **Institutional Roles** (`preferred_cargo`).
- Normalizes all names by removing treatments (e.g., "El señor") and roles.
- Tracks all textual variants as **Aliases** for traceability.

### Review Reports (`speaker_review.txt`)
After each enrichment run, a `speaker_review.txt` is generated in the Silver layer. It groups all intervention labels under their consolidated canonical identity, allowing humans to audit and refine the resolution with minimal effort.

## Roadmap

- Full historical ingestion
- Automated topic modeling pipeline
- Advanced discourse complexity metrics
- Sentiment trajectory modeling over time
- Inter-party narrative divergence analysis
- Public web-based exploration layer

## Vision

This project bridges computational linguistics, political analysis, and knowledge graph engineering to create a structured, reproducible, and scalable analytical framework for parliamentary discourse.

It serves both as a research-grade analytical platform and as a professional demonstration of:

- NLP pipeline engineering
- Medallion data architecture
- Knowledge graph construction
- Graph-based political analytics
