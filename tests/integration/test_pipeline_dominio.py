from pathlib import Path

import duckdb
import pytest

from etl_cnpj.transform.pipeline import transform_dominio

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bronze"

_ENTITIES = ["cnaes", "municipios", "naturezas", "paises", "qualificacoes", "motivos"]


@pytest.mark.parametrize("entity", _ENTITIES)
def test_transform_dominio(entity: str, tmp_path: Path):
    out_dir = transform_dominio(
        entity,
        csv_paths=[FIXTURES_DIR / f"{entity}.csv"],
        dest_dir=tmp_path / "silver",
        reference_month="2026-07",
    )

    assert (out_dir / "mes=2026-07").is_dir()

    con = duckdb.connect()
    rows = con.sql(
        f"SELECT codigo, descricao FROM read_parquet('{out_dir.as_posix()}/**/*.parquet')"
    )

    fixture_text = (FIXTURES_DIR / f"{entity}.csv").read_text(encoding="latin-1")
    fixture_rows = fixture_text.strip().count("\n") + 1
    assert len(rows.fetchall()) == fixture_rows


def test_transform_dominio_rejects_non_dominio_entity(tmp_path: Path):
    with pytest.raises(ValueError, match="não é uma tabela de domínio"):
        transform_dominio(
            "empresas",
            csv_paths=[FIXTURES_DIR / "empresas.csv"],
            dest_dir=tmp_path / "silver",
            reference_month="2026-07",
        )
