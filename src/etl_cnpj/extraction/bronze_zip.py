"""Extração dos zips aninhados dentro do zip mensal bronze da Receita Federal.

O zip baixado do portal de Dados Abertos contém, dentro de si, um zip por entidade/shard (ex.:
"Empresas0.zip", "Estabelecimentos3.zip", "Simples.zip", "Cnaes.zip"), cada um envolvendo um único
CSV cujo nome não tem relação com a entidade (ex.: "K3241.K03200Y0.D60711.EMPRECSV"). Este módulo
resolve essa indireção, extraindo cada CSV interno para um caminho previsível baseado no nome da
entidade (as chaves de `transform.schemas.TABLES`) e, quando houver, no número do shard.
"""

import re
import shutil
import tempfile
import zipfile
from pathlib import Path

from etl_cnpj.transform.schemas import TABLES

_ENTRY_PATTERN = re.compile(r"^(?P<entity>[A-Za-z]+)(?P<shard>\d*)\.zip$")


def parse_entry_name(entry_name: str) -> tuple[str, int | None]:
    """Extrai entidade e shard do nome de um zip aninhado: "Empresas3.zip" -> ("empresas", 3)."""
    filename = Path(entry_name).name
    match = _ENTRY_PATTERN.match(filename)
    if not match:
        raise ValueError(f"nome de entrada inesperado no zip bronze: {entry_name!r}")

    entity = match.group("entity").lower()
    if entity not in TABLES:
        raise ValueError(f"entidade desconhecida no zip bronze: {entity!r} (de {entry_name!r})")

    shard = int(match.group("shard")) if match.group("shard") else None
    return entity, shard


def _dest_path(dest_dir: Path, entity: str, shard: int | None) -> Path:
    if shard is None:
        return dest_dir / f"{entity}.csv"
    return dest_dir / entity / f"{shard}.csv"


def extract_bronze(
    bronze_zip: Path, dest_dir: Path, entities: set[str] | None = None
) -> list[Path]:
    """Extrai os CSVs aninhados do zip bronze para `dest_dir`, retornando os caminhos gerados.

    Cada zip aninhado é copiado primeiro para um arquivo temporário em disco antes de ser reaberto
    como ZipFile, em vez de carregado direto em memória: o `zipfile` precisa procurar o final do
    arquivo pra ler o índice central, e o stream de uma entrada compactada dentro de outro zip não
    é seekable. Os maiores desses zips aninhados passam de 2 GB descomprimidos, então evitar
    carregar tudo em memória de uma vez é deliberado, não só estilo.

    `entities`, se informado, restringe a extração às entidades daquele conjunto (ex.:
    `{"empresas"}`) — útil pra testar/rodar uma tabela sem esperar a extração de todo o zip
    mensal, que passa de 60 GB descomprimido.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []

    with zipfile.ZipFile(bronze_zip) as outer:
        for info in outer.infolist():
            if info.is_dir() or not info.filename.endswith(".zip"):
                continue

            entity, shard = parse_entry_name(info.filename)
            if entities is not None and entity not in entities:
                continue

            with tempfile.NamedTemporaryFile(suffix=".zip") as nested_tmp:
                with outer.open(info) as nested_stream:
                    shutil.copyfileobj(nested_stream, nested_tmp)
                nested_tmp.flush()

                with zipfile.ZipFile(nested_tmp.name) as inner:
                    members = [m for m in inner.infolist() if not m.is_dir()]
                    if len(members) != 1:
                        raise ValueError(
                            f"esperava 1 arquivo dentro de {info.filename}, achei {len(members)}"
                        )

                    dest_path = _dest_path(dest_dir, entity, shard)
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    with inner.open(members[0]) as src, dest_path.open("wb") as dst:
                        shutil.copyfileobj(src, dst)

            extracted.append(dest_path)

    return extracted
