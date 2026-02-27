# Congreso Data Storage

This directory manages local data partitions adhering to the Medallion Architecture.

## Layers

- **Bronze**: Found in `data/bronze/`. Contains raw, unedited, versioned documents (HTML). Data is immutable and never deleted.
- **Silver**: Found in `data/silver/`. Contains normalized, structured records stored as Parquet files. This data is partitioned (e.g. by legislature/date) and only the latest active version per document is kept (partition overwrite).
- **Gold**: Represented conceptually. This data is enriched via LLM and loaded into a Graph Database (Neo4j) for correlational insights. DuckDB views are also used locally to query Silver models directly.
