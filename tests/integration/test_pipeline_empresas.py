from decimal import Decimal
from pathlib import Path

import duckdb

from etl_cnpj.transform.pipeline import transform_empresas

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bronze"


def test_transform_empresas(tmp_path: Path):
    # dest_dir com dois níveis inexistentes (nem "silver/" existe ainda) — reproduz o bug real de
    # `transform_empresas` não criar o diretório de saída antes do COPY.
    out_dir = transform_empresas(
        csv_paths=[FIXTURES_DIR / "empresas.csv"],
        dest_dir=tmp_path / "silver",
        reference_month="2026-07",
    )

    assert (out_dir / "mes=2026-07").is_dir()

    con = duckdb.connect()
    rows = con.sql(
        f"SELECT * FROM read_parquet('{out_dir.as_posix()}/**/*.parquet') ORDER BY cnpj_basico"
    ).fetchall()

    assert len(rows) == 4

    by_cnpj = {row[0]: row for row in rows}

    # capital social com vírgula decimal vira Decimal, código de município/natureza seguem VARCHAR
    irenilda = by_cnpj["41273589"]
    assert irenilda[4] == Decimal("5000.00")
    assert irenilda[2] == "2135"  # natureza_juridica continua string, não vira int

    # ente_federativo_responsavel só é preenchido pra natureza_juridica na faixa 1XXX
    fazenda = by_cnpj["00394460"]
    assert fazenda[6] == "UNIAO"
    assert irenilda[6] is None

    # coluna de particionamento é preservada na leitura
    assert all(row[7] == "2026-07" for row in rows)
