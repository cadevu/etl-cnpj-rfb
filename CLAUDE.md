# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync --all-groups              # install deps, incl. dev group (ruff, pytest, pytest-cov, pre-commit)
uv run pre-commit install         # activate git hooks (ruff --fix, ruff-format, trailing-whitespace,
                                   # end-of-file-fixer, check-yaml/toml, check-added-large-files 1024kb)

make check                        # lint + test (lint via ruff check, then full pytest run)
make lint                         # uv run ruff check .
make format                       # uv run ruff format .
make test                         # uv run pytest -v

uv run pytest tests/unit/test_schemas.py -v                  # single file
uv run pytest tests/unit/test_schemas.py::test_fixture_column_count_matches_schema -v  # single test
uv run pytest -k empresas -v                                 # by keyword (parametrized cases)

uv run python scripts/run_pipeline.py                         # bronze -> silver, all entities, real data
uv run python scripts/run_pipeline.py --entities empresas,socios  # subset (see file docstring)
```

CI (`.github/workflows/ci.yml`, on push/PR to `main`) runs `uv sync --all-groups`, `ruff check .`,
`ruff format --check .`, then `pytest -v` — run all three locally before considering work done.

Python is pinned to `>=3.12,<3.13` (`.python-version`, enforced in `pyproject.toml`) because Airflow
(Fase 3) doesn't yet support newer versions. `uv` installs/selects the right interpreter automatically
— don't bump this without checking Airflow compatibility first.

## Architecture

Medallion pipeline (bronze → silver → gold) over the Receita Federal's monthly open CNPJ dataset.

- **bronze** (`data/bronze/`, gitignored, never committed — files are multi-GB): the raw monthly zip
  exactly as downloaded. Contains one nested zip per numbered shard of each large entity
  (`Empresas0-9.zip`, `Estabelecimentos0-9.zip`, `Socios0-9.zip`), a single `Simples.zip`, and 6 small
  domain-table zips (`Cnaes`, `Municipios`, `Naturezas`, `Paises`, `Qualificacoes`, `Motivos`). Every
  inner file is `;`-delimited, double-quoted, Latin-1 encoded, and **has no header row**.
- **`data/_work/`** (gitignored): scratch area between bronze and silver — the flat CSVs produced by
  extracting the nested zips (see below), not a data-lake layer in its own right.
- **silver** (`data/silver/`, gitignored): typed, Hive-partitioned Parquet (by `mes`, plus `uf` for
  `estabelecimentos`). Produced entirely by DuckDB bulk SQL (`transform/pipeline.py`) — no Python UDFs
  and no row-at-a-time processing, by design (volume is tens of millions of rows per entity).
- **gold** (Postgres): serving layer for point lookups by CNPJ/sócio, with `pg_trgm` trigram indexes;
  loaded via a staging table + idempotent upsert. Not implemented yet (Fase 2).

`src/etl_cnpj/extraction/bronze_zip.py` resolves the zip-inside-a-zip indirection: `extract_bronze(
bronze_zip, dest_dir, entities=None)` copies each nested zip to a temp file on disk before reopening it
as a `ZipFile` (a `ZipExtFile` stream from inside another zip isn't reliably seekable, and some nested
zips exceed 2 GB uncompressed, so this avoids loading them fully into memory), then streams the single
CSV inside to `dest_dir/<entidade>.csv` or `dest_dir/<entidade>/<shard>.csv`. The optional `entities`
set restricts extraction to a subset — needed in practice, since a full extract is tens of GB.

`src/etl_cnpj/transform/pipeline.py` has one `transform_<entidade>` function per bronze table (plus a
single generic `transform_dominio(entity, ...)` for the 6 code→description domain tables, since they
share one layout). Each reads the extracted CSVs via DuckDB's `read_csv` (Latin-1, `;`-delimited, all
columns read as `VARCHAR` first), applies the business-rule casts as plain SQL (comma-decimal
`capital_social`, `AAAAMMDD` dates with sentinel handling, CNAE-secundária split into a list), and
writes Parquet via `COPY ... (FORMAT PARQUET, PARTITION_BY (...))`. **Non-obvious DuckDB behavior**: its
CSV reader turns a quoted-but-empty field (`""`) into SQL `NULL`, not an empty string, for every column
— which is why the "missing date" sentinel for `Estabelecimentos`/`Socios` (empty string) needs no
explicit `CASE WHEN`, while `Simples`'s sentinel (the literal `"00000000"`) does (`NULLIF(col,
'00000000')` before `TRY_STRPTIME`). `TRY_STRPTIME`, not `STRPTIME`, is used throughout so a malformed
date in real data becomes `NULL` instead of aborting the whole load.

The SQL casts in `pipeline.py` intentionally **duplicate** the semantics already tested in
`transform/rules.py` (`parse_capital_social`, `parse_data`, `split_cnae_secundaria`) rather than calling
those functions as DuckDB UDFs — a Python UDF runs row-by-row and would give up the vectorized-SQL
performance that's the whole reason DuckDB was chosen over row-at-a-time Python. `rules.py` stays as the
tested reference semantics, and `tests/integration/test_pipeline_*.py` includes parity tests
(`*_matches_rules_py`) that re-derive the expected value with `rules.py` from the same fixture rows and
assert the SQL output agrees — so a fix applied to one side and forgotten on the other fails CI.

`src/etl_cnpj/transform/schemas.py` is the single source of truth for the raw column layout (name +
order) of all 13 bronze tables — it exists because the source files have no header, so this is the only
place that knows column N of `Estabelecimentos` is `cnae_fiscal_secundaria`. It's metadata only (a
`Column(name, description)` tuple per table), not a per-row validation model — rows are never
materialized as individual Python objects in this pipeline, by design (volume is tens of GB, so bulk
DuckDB operations are used instead of row-at-a-time Python). Any new code that reads bronze CSVs should
get its column list from here rather than hardcoding column order again.

`src/etl_cnpj/config.py` centralizes configuration via `pydantic-settings`, reading from `.env` (see
`.env.example` for the documented variables: `RF_BASE_URL`, `POSTGRES_DSN`). Data-lake paths
(`bronze_dir`, `work_dir`, `silver_dir`) are derived from `REPO_ROOT`, not hardcoded per-module.

`scripts/run_pipeline.py` is the manual entry point that wires extraction + `pipeline.py` together
against a real bronze zip (auto-discovered in `data/bronze/` if there's exactly one), with a
`--entities` flag to run a subset — the full zip is 7.6+ GB compressed (`Estabelecimentos` alone is
~5.3 GB compressed), so a full run is slow enough that iterating on a subset matters. Timing is just
`time.perf_counter()` per stage, printed to stdout; a real metrics/instrumentation layer is Fase 5, not
this script's job.

## Domain pitfalls (Receita Federal CNPJ data)

These come from `cnpj-metadados.pdf` (official layout doc) and inspection of real files — they're easy
to get wrong because they contradict the "obvious" typing choice:

- Every code field (`CNPJ`, `CEP`, município, CNAE, etc.) must stay `VARCHAR`, never `INTEGER` — casting
  drops leading zeros silently.
- `CAPITAL_SOCIAL` uses a comma as the decimal separator (`"5000,00"`), no thousands separator.
- Dates are `AAAAMMDD`, but the "missing" sentinel is inconsistent across entities: `Estabelecimentos`
  uses an empty string for missing optional fields (`pais`, `nome_cidade_exterior`), while `Simples`
  uses the literal string `"00000000"` for missing dates. A single "is this date missing" helper needs
  to handle both.
- `CNAE_FISCAL_SECUNDARIA` packs multiple codes into one comma-joined string field — must be split in
  the transform layer, not treated as a single value.
- In `Socios`: `CPF` (`cnpj_cpf_socio`, `representante_legal`) already arrives masked from the source
  (`***123456**`) — this is not something this pipeline needs to do. A sócio PJ's `CNPJ`, by contrast,
  is **not** masked. A sócio estrangeiro (`identificador_socio == 3`) has an empty document field.
  `FAIXA_ETARIA` is likewise already computed by Receita in the source file, not derived here.
- The `Municipios` domain table has no UF column — UF has to come from a join (e.g. via
  `Estabelecimentos`), not from `Municipios` alone.
- `ENTE_FEDERATIVO_RESPONSAVEL` (in `Empresas`) is only populated when `NATUREZA_JURIDICA` is in the
  `1XXX` range; blank for every other natureza.
- Sócios have no unique natural key.

## Testing conventions

- Bronze-format fixtures live in `tests/fixtures/bronze/<entity>.csv` — small (a handful of rows),
  hand-written, but byte-faithful to the real format (Latin-1, `;`-delimited, quoted, no header) rather
  than a trimmed real extract. They're written to exercise specific edge cases (e.g. sócio estrangeiro,
  multiple CNAEs secundárias, both "missing date" conventions), and are validated against
  `transform/schemas.py` in `tests/unit/test_schemas.py`.
- `pyproject.toml` sets `pythonpath = ["src"]` for pytest, so `etl_cnpj` imports work in tests without
  an editable install.
- `tests/integration/` holds tests that touch a real DuckDB engine and the filesystem (via `tmp_path`)
  instead of pure functions — one `test_pipeline_<entidade>.py` per `transform_*` function. These
  reuse the same `tests/fixtures/bronze/*.csv` files as the unit tests, plus (for entities with a
  business-rule cast) a `*_matches_rules_py` parity test — see the `pipeline.py` note above.

## Status

See `README.md`'s "Status" section for the current phase. As of now, Fase 1 (bronze → silver) is
functionally complete for all 10 bronze tables (4 large entities + 6 domain tables) — schemas, pure
business rules, nested-zip extraction, and the DuckDB pipeline are all implemented and tested, and
`scripts/run_pipeline.py` has been run against the real monthly zip. Known open items: no CLI/DAG
wiring beyond the manual script, one real-data anomaly under investigation (~3k `Empresas` rows with
`porte_empresa` NULL), and Fases 2+ (Postgres load, Airflow, API, instrumentation, Azure) not started.
