"""Configuração de logging para scripts de execução manual (ex.: `scripts/run_pipeline.py`).

Grava simultaneamente no console (para acompanhar a execução em andamento) e em um arquivo em
`settings.logs_dir` (para conferir depois, mesmo que o terminal tenha sido fechado ou o processo
tenha morrido no meio). Um arquivo por execução, nomeado com o mês de referência e o horário de
início, para não sobrescrever logs de execuções anteriores.
"""

import logging
from datetime import datetime
from pathlib import Path

from etl_cnpj.config import settings

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(reference_month: str, *, logger_name: str = "etl_cnpj") -> Path:
    """Configura console + arquivo para `logger_name` e retorna o caminho do arquivo de log."""
    settings.logs_dir.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_path = settings.logs_dir / f"pipeline_{reference_month}_{run_id}.log"

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False

    return log_path
