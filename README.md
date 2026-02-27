# Parliamentary Discourse Knowledge Graph

Parliamentary Discourse Knowledge Graph (PDKG) is an NLP-driven data engineering framework designed to model, enrich, and analyze parliamentary debate transcripts from the Spanish Congress of Deputies (Congreso de los Diputados de España) through a structured knowledge graph approach.

The system ingests official session diaries (Diarios de Sesiones), processes them through a Bronze/Silver/Gold medallion architecture, enriches the discourse using Large Language Models (LLMs), and materializes structured relationships in a Neo4j knowledge graph.

The primary objective is to enable reproducible, scalable, and longitudinal computational analysis of political discourse complexity, thematic evolution, narrative framing, and party positioning in Spain.

## Project Scope

This project transforms unstructured parliamentary transcripts into structured, queryable analytical assets.

Core capabilities include:

- Speaker identification and intervention structuring
- Discourse normalization and metadata extraction
- Topic modeling and thematic clustering
- Sentiment and discourse framing analysis
- Linguistic and structural complexity measurement
- Knowledge graph construction
- Longitudinal political discourse analysis

The current MVP processes the latest legislature, with a scalable design intended to support full historical ingestion.

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
- Partitioned by legislature and date (e.g. `legislature=XV/date=YYYY-MM-DD/`)
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
