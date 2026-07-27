"""Roda o pipeline bronze -> silver para uma ou mais entidades, medindo tempo por etapa.

Uso:
    uv run python scripts/run_pipeline.py                              # todas as entidades
    uv run python scripts/run_pipeline.py --entities empresas,socios
    uv run python scripts/run_pipeline.py data/bronze/2026-07.zip --entities simples

Aviso de tamanho: o zip mensal completo passa de 7,6 GB comprimidos — só Estabelecimentos são
~5,3 GB comprimidos (várias dezenas de GB descomprimidos). Rodar tudo de uma vez pode levar bem
mais tempo que os ~2 min de Empresas sozinha, e ocupa bastante espaço em `data/_work/` (CSVs
extraídos) e `data/silver/` (Parquet final). Medição de tempo aqui é propositalmente simples (só
`time.perf_counter` por etapa); instrumentação de verdade é escopo da Fase 5.
"""

import argparse
import sys
import time
from collections.abc import Callable
from functools import partial
from pathlib import Path

from etl_cnpj.config import settings
from etl_cnpj.extraction.bronze_zip import extract_bronze
from etl_cnpj.transform.pipeline import (
    transform_dominio,
    transform_empresas,
    transform_estabelecimentos,
    transform_simples,
    transform_socios,
)

_SHARDED_ENTITIES = {"empresas", "estabelecimentos", "socios"}
_DOMINIO_ENTITIES = ("cnaes", "municipios", "naturezas", "paises", "qualificacoes", "motivos")

_TRANSFORMS: dict[str, Callable[[list[Path], Path, str], Path]] = {
    "empresas": transform_empresas,
    "estabelecimentos": transform_estabelecimentos,
    "socios": transform_socios,
    "simples": transform_simples,
    **{entity: partial(transform_dominio, entity) for entity in _DOMINIO_ENTITIES},
}


def _resolve_bronze_zip(arg: str | None) -> Path:
    if arg:
        return Path(arg)

    candidates = sorted(settings.bronze_dir.glob("*.zip"))
    if len(candidates) != 1:
        raise SystemExit(
            f"esperava exatamente 1 zip em {settings.bronze_dir}, achei {len(candidates)} — "
            "informe o caminho explicitamente"
        )
    return candidates[0]


def _csv_paths_for(entity: str, work_dir: Path) -> list[Path]:
    if entity in _SHARDED_ENTITIES:
        return sorted((work_dir / entity).glob("*.csv"))
    return [work_dir / f"{entity}.csv"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("bronze_zip", nargs="?", help="Caminho do zip mensal bronze")
    parser.add_argument(
        "--entities",
        help=f"Subconjunto separado por vírgula (padrão: todas). Opções: {', '.join(_TRANSFORMS)}",
    )
    args = parser.parse_args()

    entities = args.entities.split(",") if args.entities else list(_TRANSFORMS)
    unknown = sorted(set(entities) - _TRANSFORMS.keys())
    if unknown:
        raise SystemExit(f"entidade(s) desconhecida(s): {unknown} — opções: {list(_TRANSFORMS)}")

    bronze_zip = _resolve_bronze_zip(args.bronze_zip)
    reference_month = bronze_zip.stem

    print(f"zip bronze: {bronze_zip}")
    print(f"mês de referência: {reference_month}")
    print(f"entidades: {', '.join(entities)}")

    start = time.perf_counter()
    extract_bronze(bronze_zip, settings.work_dir, entities=set(entities))
    extract_elapsed = time.perf_counter() - start
    print(f"extração total: {extract_elapsed:.1f}s")

    total_transform = 0.0
    for entity in entities:
        csv_paths = _csv_paths_for(entity, settings.work_dir)
        start = time.perf_counter()
        out_dir = _TRANSFORMS[entity](csv_paths, settings.silver_dir, reference_month)
        elapsed = time.perf_counter() - start
        total_transform += elapsed
        print(f"  {entity}: {len(csv_paths)} arquivo(s) -> {out_dir} em {elapsed:.1f}s")

    print(f"transformação total: {total_transform:.1f}s")
    print(f"total geral: {extract_elapsed + total_transform:.1f}s")


if __name__ == "__main__":
    sys.exit(main())
