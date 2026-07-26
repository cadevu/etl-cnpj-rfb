import csv
from pathlib import Path

import pytest

from etl_cnpj.transform.schemas import TABLES

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bronze"


@pytest.mark.parametrize("entity", sorted(TABLES))
def test_fixture_column_count_matches_schema(entity: str):
    expected = len(TABLES[entity])
    fixture_path = FIXTURES_DIR / f"{entity}.csv"

    with fixture_path.open(encoding="latin-1", newline="") as f:
        rows = list(csv.reader(f, delimiter=";", quotechar='"'))

    assert rows, f"fixture {fixture_path.name} está vazia"
    for i, row in enumerate(rows):
        assert (
            len(row) == expected
        ), f"{fixture_path.name} linha {i}: {len(row)} colunas, esperado {expected}"
