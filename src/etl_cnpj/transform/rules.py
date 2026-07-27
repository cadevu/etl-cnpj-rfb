"""Funções puras de regra de negócio aplicadas linha a linha durante bronze -> silver.

Escopo deliberadamente pequeno: só entram aqui campos que exigem uma conversão real. Máscara de
CPF e cálculo de FAIXA_ETARIA (sócios), por exemplo, já vêm prontos da Receita Federal no arquivo
bruto e por isso não têm função equivalente neste módulo.
"""

from datetime import date
from decimal import Decimal


def parse_capital_social(raw: str) -> Decimal:
    """Converte capital social com vírgula decimal ("5000,00") para Decimal."""
    return Decimal(raw.strip().replace(",", "."))


def parse_data(raw: str) -> date | None:
    """Converte data AAAAMMDD para `date`, tratando os dois sentinelas de "sem data" da fonte.

    Estabelecimentos usa string vazia para datas ausentes; Simples usa o literal "00000000".
    """
    raw = raw.strip()
    if not raw or raw == "00000000":
        return None
    return date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))


def split_cnae_secundaria(raw: str) -> list[str]:
    """Separa a string "cod,cod,cod" de CNAE_FISCAL_SECUNDARIA em lista de códigos."""
    raw = raw.strip()
    if not raw:
        return []
    return raw.split(",")
