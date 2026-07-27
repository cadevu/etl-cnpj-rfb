import io
import zipfile
from pathlib import Path

import pytest

from etl_cnpj.extraction.bronze_zip import extract_bronze, parse_entry_name

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bronze"


@pytest.mark.parametrize(
    ("entry_name", "expected_entity", "expected_shard"),
    [
        ("2026-07/Empresas0.zip", "empresas", 0),
        ("2026-07/Estabelecimentos3.zip", "estabelecimentos", 3),
        ("2026-07/Socios9.zip", "socios", 9),
        ("2026-07/Simples.zip", "simples", None),
        ("2026-07/Cnaes.zip", "cnaes", None),
        ("2026-07/Municipios.zip", "municipios", None),
    ],
)
def test_parse_entry_name(entry_name: str, expected_entity: str, expected_shard: int | None):
    assert parse_entry_name(entry_name) == (expected_entity, expected_shard)


def test_parse_entry_name_rejects_unknown_entity():
    with pytest.raises(ValueError, match="entidade desconhecida"):
        parse_entry_name("2026-07/Naozipado.zip")


def test_parse_entry_name_rejects_unexpected_format():
    with pytest.raises(ValueError, match="inesperado"):
        parse_entry_name("2026-07/leiame.txt")


def _add_nested_zip(
    outer: zipfile.ZipFile, entry_name: str, inner_name: str, content: bytes
) -> None:
    nested_buffer = io.BytesIO()
    with zipfile.ZipFile(nested_buffer, "w") as nested:
        nested.writestr(inner_name, content)
    outer.writestr(f"2026-07/{entry_name}", nested_buffer.getvalue())


def _build_synthetic_bronze_zip(dest: Path) -> None:
    """Monta um zip bronze sintético reaproveitando as fixtures de bronze já existentes."""
    with zipfile.ZipFile(dest, "w") as bronze:
        empresas_csv = (FIXTURES_DIR / "empresas.csv").read_bytes()
        for shard in (0, 1):
            _add_nested_zip(bronze, f"Empresas{shard}.zip", "K3241.EMPRECSV", empresas_csv)

        _add_nested_zip(
            bronze, "Simples.zip", "F.SIMPLES.CSV", (FIXTURES_DIR / "simples.csv").read_bytes()
        )
        _add_nested_zip(bronze, "Cnaes.zip", "F.CNAECSV", (FIXTURES_DIR / "cnaes.csv").read_bytes())


def test_extract_bronze(tmp_path: Path):
    bronze_zip = tmp_path / "2026-07.zip"
    _build_synthetic_bronze_zip(bronze_zip)

    dest_dir = tmp_path / "work"
    extracted = extract_bronze(bronze_zip, dest_dir)

    assert sorted(p.relative_to(dest_dir).as_posix() for p in extracted) == [
        "cnaes.csv",
        "empresas/0.csv",
        "empresas/1.csv",
        "simples.csv",
    ]

    empresas_csv = (FIXTURES_DIR / "empresas.csv").read_bytes()
    assert (dest_dir / "empresas" / "0.csv").read_bytes() == empresas_csv
    assert (dest_dir / "empresas" / "1.csv").read_bytes() == empresas_csv
    assert (dest_dir / "simples.csv").read_bytes() == (FIXTURES_DIR / "simples.csv").read_bytes()
    assert (dest_dir / "cnaes.csv").read_bytes() == (FIXTURES_DIR / "cnaes.csv").read_bytes()


def test_extract_bronze_filters_by_entity(tmp_path: Path):
    bronze_zip = tmp_path / "2026-07.zip"
    _build_synthetic_bronze_zip(bronze_zip)

    dest_dir = tmp_path / "work"
    extracted = extract_bronze(bronze_zip, dest_dir, entities={"empresas"})

    assert sorted(p.relative_to(dest_dir).as_posix() for p in extracted) == [
        "empresas/0.csv",
        "empresas/1.csv",
    ]
