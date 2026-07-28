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
import logging
import sys
import time
from collections.abc import Callable
from datetime import datetime
from functools import partial
from pathlib import Path

from etl_cnpj.config import settings
from etl_cnpj.extraction.bronze_zip import extract_bronze
from etl_cnpj.logging_config import setup_logging
from etl_cnpj.transform.pipeline import (
    transform_dominio,
    transform_empresas,
    transform_estabelecimentos,
    transform_simples,
    transform_socios,
)

logger = logging.getLogger("etl_cnpj.pipeline")
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

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

    log_path = setup_logging(reference_month)

    run_start_dt = datetime.now()
    logger.info("log desta execução em: %s", log_path)
    logger.info("início da execução: %s", run_start_dt.strftime(_TIMESTAMP_FORMAT))
    logger.info("zip bronze: %s", bronze_zip)
    logger.info("mês de referência: %s", reference_month)
    logger.info("entidades: %s", ", ".join(entities))

    try:
        extract_start_dt = datetime.now()
        start = time.perf_counter()
        extract_bronze(bronze_zip, settings.work_dir, entities=set(entities))
        extract_elapsed = time.perf_counter() - start
        extract_end_dt = datetime.now()
        logger.info(
            "extração total: %s -> %s (%.1fs)",
            extract_start_dt.strftime(_TIMESTAMP_FORMAT),
            extract_end_dt.strftime(_TIMESTAMP_FORMAT),
            extract_elapsed,
        )

        total_transform = 0.0
        for entity in entities:
            csv_paths = _csv_paths_for(entity, settings.work_dir)
            entity_start_dt = datetime.now()
            start = time.perf_counter()
            out_dir = _TRANSFORMS[entity](csv_paths, settings.silver_dir, reference_month)
            elapsed = time.perf_counter() - start
            entity_end_dt = datetime.now()
            total_transform += elapsed
            logger.info(
                "  %s: %d arquivo(s) -> %s | %s -> %s (%.1fs)",
                entity,
                len(csv_paths),
                out_dir,
                entity_start_dt.strftime(_TIMESTAMP_FORMAT),
                entity_end_dt.strftime(_TIMESTAMP_FORMAT),
                elapsed,
            )
    except Exception:
        logger.exception("execução interrompida por erro")
        raise

    run_end_dt = datetime.now()
    logger.info("transformação total: %.1fs", total_transform)
    logger.info("total geral: %.1fs", extract_elapsed + total_transform)
    logger.info(
        "fim da execução: %s (início %s, %.1fs no total)",
        run_end_dt.strftime(_TIMESTAMP_FORMAT),
        run_start_dt.strftime(_TIMESTAMP_FORMAT),
        (run_end_dt - run_start_dt).total_seconds(),
    )


if __name__ == "__main__":
    sys.exit(main())
