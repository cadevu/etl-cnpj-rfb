from pathlib import Path

import duckdb

from etl_cnpj.transform.pipeline import transform_socios

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bronze"


def test_transform_socios(tmp_path: Path):
    out_dir = transform_socios(
        csv_paths=[FIXTURES_DIR / "socios.csv"],
        dest_dir=tmp_path / "silver",
        reference_month="2026-07",
    )

    assert (out_dir / "mes=2026-07").is_dir()

    con = duckdb.connect()
    relation = con.sql(f"SELECT * FROM read_parquet('{out_dir.as_posix()}/**/*.parquet')")
    columns = relation.columns
    rows = relation.fetchall()
    assert len(rows) == 3

    # sócio estrangeiro (identificador_socio == 3): sem CPF/CNPJ, com país preenchido
    estrangeiro = next(r for r in rows if r[columns.index("identificador_socio")] == "3")
    assert estrangeiro[columns.index("cnpj_cpf_socio")] is None
    assert estrangeiro[columns.index("pais")] == "249"

    # sócio PJ: CNPJ não é mascarado, ao contrário do CPF de sócio PF
    socio_pj = next(r for r in rows if r[columns.index("identificador_socio")] == "1")
    assert socio_pj[columns.index("cnpj_cpf_socio")] == "07882217000111"

    # sócio PF: CPF já vem mascarado da fonte, não é papel deste pipeline mascarar
    socio_pf = next(r for r in rows if r[columns.index("identificador_socio")] == "2")
    assert socio_pf[columns.index("cnpj_cpf_socio")] == "***441403**"
