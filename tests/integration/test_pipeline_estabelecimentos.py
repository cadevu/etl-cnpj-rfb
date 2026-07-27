import csv
from datetime import date
from pathlib import Path

import duckdb

from etl_cnpj.transform.pipeline import transform_estabelecimentos
from etl_cnpj.transform.rules import parse_data, split_cnae_secundaria

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bronze"
_FIXTURE_PATH = FIXTURES_DIR / "estabelecimentos.csv"


def test_transform_estabelecimentos(tmp_path: Path):
    out_dir = transform_estabelecimentos(
        csv_paths=[_FIXTURE_PATH],
        dest_dir=tmp_path / "silver",
        reference_month="2026-07",
    )

    assert (out_dir / "mes=2026-07" / "uf=SP").is_dir()
    assert (out_dir / "mes=2026-07" / "uf=__HIVE_DEFAULT_PARTITION__").is_dir()

    con = duckdb.connect()
    relation = con.sql(
        f"SELECT * FROM read_parquet('{out_dir.as_posix()}/**/*.parquet') ORDER BY cnpj_basico"
    )
    columns = relation.columns
    rows = relation.fetchall()
    assert len(rows) == 4

    by_cnpj = dict(zip((r[columns.index("cnpj_basico")] for r in rows), rows, strict=True))

    matriz_exterior = by_cnpj["61685999"]
    assert matriz_exterior[columns.index("uf")] is None
    assert matriz_exterior[columns.index("pais")] == "249"
    assert matriz_exterior[columns.index("cnae_fiscal_secundaria")] == []

    multi_cnae = by_cnpj["61685894"]
    assert multi_cnae[columns.index("cnae_fiscal_secundaria")] == ["8599603", "8219999"]


def test_transform_estabelecimentos_matches_rules_py(tmp_path: Path):
    """Paridade entre as datas/CNAE secundária calculadas em SQL e as mesmas contas em Python puro.

    As duas implementações existem por motivos de performance (ver docstring de
    `transform_estabelecimentos`), não porque a lógica deveria divergir — este teste garante que
    elas concordam para cada linha da fixture, usando a leitura crua da fixture (via `csv.reader`,
    igual a `test_schemas.py`) como fonte comum de verdade.
    """
    out_dir = transform_estabelecimentos(
        csv_paths=[_FIXTURE_PATH],
        dest_dir=tmp_path / "silver",
        reference_month="2026-07",
    )

    con = duckdb.connect()
    relation = con.sql(
        f"SELECT * FROM read_parquet('{out_dir.as_posix()}/**/*.parquet') ORDER BY cnpj_basico"
    )
    columns = relation.columns
    rows = relation.fetchall()
    by_cnpj = {r[columns.index("cnpj_basico")]: r for r in rows}

    with _FIXTURE_PATH.open(encoding="latin-1", newline="") as f:
        raw_rows = list(csv.reader(f, delimiter=";", quotechar='"'))

    for raw in raw_rows:
        cnpj_basico = raw[0]
        sql_row = by_cnpj[cnpj_basico]

        expected_situacao: date | None = parse_data(raw[6])
        expected_inicio: date | None = parse_data(raw[10])
        expected_especial: date | None = parse_data(raw[29])
        expected_cnae = split_cnae_secundaria(raw[12])

        assert sql_row[columns.index("data_situacao_cadastral")] == expected_situacao
        assert sql_row[columns.index("data_inicio_atividade")] == expected_inicio
        assert sql_row[columns.index("data_situacao_especial")] == expected_especial
        assert sql_row[columns.index("cnae_fiscal_secundaria")] == expected_cnae
