import csv
from datetime import date
from pathlib import Path

import duckdb

from etl_cnpj.transform.pipeline import transform_simples
from etl_cnpj.transform.rules import parse_data

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bronze"
_FIXTURE_PATH = FIXTURES_DIR / "simples.csv"


def test_transform_simples(tmp_path: Path):
    out_dir = transform_simples(
        csv_paths=[_FIXTURE_PATH],
        dest_dir=tmp_path / "silver",
        reference_month="2026-07",
    )

    assert (out_dir / "mes=2026-07").is_dir()

    con = duckdb.connect()
    relation = con.sql(
        f"SELECT * FROM read_parquet('{out_dir.as_posix()}/**/*.parquet') ORDER BY cnpj_basico"
    )
    columns = relation.columns
    rows = relation.fetchall()
    assert len(rows) == 3

    # "00000000" (sentinela do Simples) vira NULL, não uma data literal em 0000-00-00
    empresa_sem_exclusao = next(r for r in rows if r[columns.index("cnpj_basico")] == "00000011")
    assert empresa_sem_exclusao[columns.index("data_exclusao_simples")] is None
    assert empresa_sem_exclusao[columns.index("data_opcao_simples")] == date(2007, 7, 1)

    # quando a exclusão é real, a data é preservada normalmente
    empresa_excluida = next(r for r in rows if r[columns.index("cnpj_basico")] == "00000006")
    assert empresa_excluida[columns.index("data_exclusao_simples")] == date(2019, 12, 31)


def test_transform_simples_matches_rules_py(tmp_path: Path):
    out_dir = transform_simples(
        csv_paths=[_FIXTURE_PATH],
        dest_dir=tmp_path / "silver",
        reference_month="2026-07",
    )

    con = duckdb.connect()
    relation = con.sql(f"SELECT * FROM read_parquet('{out_dir.as_posix()}/**/*.parquet')")
    columns = relation.columns
    by_cnpj = {r[columns.index("cnpj_basico")]: r for r in relation.fetchall()}

    with _FIXTURE_PATH.open(encoding="latin-1", newline="") as f:
        raw_rows = list(csv.reader(f, delimiter=";", quotechar='"'))

    date_columns = {
        "data_opcao_simples": 2,
        "data_exclusao_simples": 3,
        "data_opcao_mei": 5,
        "data_exclusao_mei": 6,
    }

    for raw in raw_rows:
        sql_row = by_cnpj[raw[0]]
        for column, raw_index in date_columns.items():
            assert sql_row[columns.index(column)] == parse_data(raw[raw_index])
