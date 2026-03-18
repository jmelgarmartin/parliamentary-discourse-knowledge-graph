# PROJECT SNAPSHOT — Parliamentary Discourse Knowledge Graph

## Repository
- **Name**: parliamentary-discourse-knowledge-graph
- **Scope**: Spanish Congress of Deputies (Congreso de los Diputados de España)
- Public GitHub repo
- Professional-grade engineering standards

## Architectural Principles

### Data Architecture
- **Medallion pattern**: Bronze / Silver / Gold
  - **Bronze**: Raw HTML, immutable, append-only
  - **Silver**: Normalized Parquet, partitioned by legislature/date, idempotent
  - **Gold**: LLM enrichment + DuckDB analytics + Neo4j knowledge graph

### State Management
- DuckDB controls document processing state
- Checksum-based reprocessing
- Full overwrite strategy on document update
- Only latest active version exists in Silver/Gold

## Core Conceptual Model

### Graph Philosophy
- Word-centric knowledge graph
- **Central concept**: `Deputy` → uses → `Term` → within → `Intervention`

### Main Entities
- Deputy
- Group
- Session
- AgendaItem
- Intervention
- Term (lemma-based)
- Document (for traceability & idempotency)

### Design Constraints
- Avoid token-level explosion in Neo4j
- Store full lexical detail in Parquet
- Materialize aggregated relationships in Neo4j
- Every graph entity traceable to source document

## NLP Strategy
- Lemma-based normalization
- Stopword filtering
- Language detection (keep Spanish translation)
- Topic modeling (unsupervised)
- Co-occurrence modeling per intervention
- LLM via API (development: low-cost model, future: production-grade)

## Tooling & Engineering Standards

### Environment
- Windows 11
- Python 3.11
- Poetry (in-project `.venv`)

### Quality Controls
- `pre-commit` enabled
- Ruff (lint + format)
- MyPy strict mode
- Pytest + coverage
- 100% typed code target
- High test coverage target

## Strategic Objective
Build a reproducible, scalable NLP + Knowledge Graph system capable of:
- Longitudinal discourse analysis
- Vocabulary evolution tracking
- Narrative and framing detection
- Inter-party lexical divergence analysis
- Political complexity measurement

This project is both:
- A research-grade analytical system
- A professional portfolio demonstrating:
  - Graph engineering
  - NLP pipelines
  - Medallion architecture
  - Idempotent data processing

## Usage in New Conversations
Start a new chat with:
> Continue working on Parliamentary Discourse Knowledge Graph. Use the PROJECT SNAPSHOT as context.

That is enough to restore full alignment.
