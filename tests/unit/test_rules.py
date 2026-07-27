from datetime import date
from decimal import Decimal, InvalidOperation

import pytest

from etl_cnpj.transform.rules import parse_capital_social, parse_data, split_cnae_secundaria


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("5000,00", Decimal("5000.00")),
        ("0,00", Decimal("0.00")),
        ("1234567,89", Decimal("1234567.89")),
    ],
)
def test_parse_capital_social(raw: str, expected: Decimal):
    assert parse_capital_social(raw) == expected


def test_parse_capital_social_rejects_empty_string():
    with pytest.raises(InvalidOperation):
        parse_capital_social("")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("20250710", date(2025, 7, 10)),
        ("", None),  # sentinela de data ausente em Estabelecimentos
        ("00000000", None),  # sentinela de data ausente em Simples
    ],
)
def test_parse_data(raw: str, expected: date | None):
    assert parse_data(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("8599603,8219999", ["8599603", "8219999"]),
        ("8219999", ["8219999"]),
        ("", []),
    ],
)
def test_split_cnae_secundaria(raw: str, expected: list[str]):
    assert split_cnae_secundaria(raw) == expected
