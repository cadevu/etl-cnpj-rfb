"""Pipeline bronze -> silver: leitura em bulk via DuckDB e escrita em Parquet particionado.

As conversões feitas aqui (capital social com vírgula decimal, datas com sentinela ausente, split
de CNAE secundária) espelham a semântica já testada em `transform/rules.py`, mas reimplementadas
como SQL para rodar vetorizado sobre dezenas de milhões de linhas em vez de invocar Python linha a
linha — ver a decisão registrada em CLAUDE.md ("bulk SQL, não UDF"). `tests/integration/` garante
que as duas implementações concordam nos casos cobertos pelas fixtures.
"""

from pathlib import Path

import duckdb

from etl_cnpj.transform.schemas import column_names

# DuckDB's 'latin-1' alias rejects some real RF shards as "not latin-1 encoded" even though
# ISO-8859-1 is a total mapping over all byte values; '8859_1' (an ICU alias for the same charset)
# doesn't have that bug and reads the identical shards cleanly.
_CSV_OPTIONS = "header = false, sep = ';', quote = '\"', encoding = '8859_1', all_varchar = true"

_DOMINIO_ENTITIES = {"cnaes", "municipios", "naturezas", "paises", "qualificacoes", "motivos"}


def _read_raw(
    con: duckdb.DuckDBPyConnection, csv_paths: list[Path], entity: str
) -> duckdb.DuckDBPyRelation:
    """Lê os CSVs brutos de `entity` como uma relação com todas as colunas VARCHAR.

    Todas as colunas entram como VARCHAR nesta etapa — os casts de negócio (datas, decimais)
    ficam a cargo de cada função `transform_*`, que sabe quais colunas precisam de conversão.
    """
    files_sql = repr([str(p) for p in csv_paths])
    names_sql = repr(column_names(entity))
    return con.sql(f"SELECT * FROM read_csv({files_sql}, {_CSV_OPTIONS}, names = {names_sql})")


def _write_silver(
    con: duckdb.DuckDBPyConnection, select_sql: str, out_dir: Path, partition_by: list[str]
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    partition_cols = ", ".join(partition_by)
    con.sql(
        f"COPY ({select_sql}) TO {str(out_dir)!r} "
        f"(FORMAT PARQUET, PARTITION_BY ({partition_cols}), OVERWRITE_OR_IGNORE true)"
    )
    return out_dir


def transform_empresas(csv_paths: list[Path], dest_dir: Path, reference_month: str) -> Path:
    """Lê os CSVs brutos de Empresas, tipa e escreve Parquet particionado por mês em `dest_dir`."""
    con = duckdb.connect()
    _read_raw(con, csv_paths, "empresas").create_view("raw_empresas")

    return _write_silver(
        con,
        f"""
        SELECT
            cnpj_basico,
            razao_social,
            natureza_juridica,
            qualificacao_responsavel,
            CAST(REPLACE(capital_social, ',', '.') AS DECIMAL(18, 2)) AS capital_social,
            porte_empresa,
            ente_federativo_responsavel,
            {reference_month!r} AS mes
        FROM raw_empresas
        """,
        dest_dir / "empresas",
        partition_by=["mes"],
    )


def transform_estabelecimentos(csv_paths: list[Path], dest_dir: Path, reference_month: str) -> Path:
    """Lê os CSVs brutos de Estabelecimentos, tipa e escreve Parquet particionado por mês/UF.

    Datas ausentes chegam como NULL direto do `read_csv` (o sentinela de Estabelecimentos é a
    string vazia, e o leitor CSV do DuckDB já trata campo vazio entre aspas como NULL), então
    `TRY_STRPTIME` sobre NULL já retorna NULL sem precisar de um CASE WHEN explícito — diferente
    do sentinela "00000000" de Simples, que não é ambíguo com "campo vazio" (ver
    `transform_simples`). Uso TRY_STRPTIME (não STRPTIME) porque, em dezenas de milhões de linhas
    reais, prefiro uma data malformada virar NULL a abortar a carga inteira.
    """
    con = duckdb.connect()
    _read_raw(con, csv_paths, "estabelecimentos").create_view("raw_estabelecimentos")

    return _write_silver(
        con,
        f"""
        SELECT
            cnpj_basico,
            cnpj_ordem,
            cnpj_dv,
            identificador_matriz_filial,
            nome_fantasia,
            situacao_cadastral,
            TRY_STRPTIME(data_situacao_cadastral, '%Y%m%d')::DATE AS data_situacao_cadastral,
            motivo_situacao_cadastral,
            nome_cidade_exterior,
            pais,
            TRY_STRPTIME(data_inicio_atividade, '%Y%m%d')::DATE AS data_inicio_atividade,
            cnae_fiscal_principal,
            COALESCE(STRING_SPLIT(cnae_fiscal_secundaria, ','), []) AS cnae_fiscal_secundaria,
            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            bairro,
            cep,
            uf,
            municipio,
            ddd_1,
            telefone_1,
            ddd_2,
            telefone_2,
            ddd_fax,
            fax,
            correio_eletronico,
            situacao_especial,
            TRY_STRPTIME(data_situacao_especial, '%Y%m%d')::DATE AS data_situacao_especial,
            {reference_month!r} AS mes
        FROM raw_estabelecimentos
        """,
        dest_dir / "estabelecimentos",
        partition_by=["mes", "uf"],
    )


def transform_socios(csv_paths: list[Path], dest_dir: Path, reference_month: str) -> Path:
    """Lê os CSVs brutos de Sócios, tipa e escreve Parquet particionado por mês.

    Máscara de CPF e FAIXA_ETARIA já vêm prontas da Receita no arquivo bruto — não há regra de
    negócio a aplicar além do cast de `data_entrada_sociedade` (mesmo raciocínio de
    `transform_estabelecimentos`: TRY_STRPTIME sobre NULL já cobre o campo ausente). Sócios não
    têm UF (não há endereço nessa entidade), por isso particiona só por mês.
    """
    con = duckdb.connect()
    _read_raw(con, csv_paths, "socios").create_view("raw_socios")

    return _write_silver(
        con,
        f"""
        SELECT
            cnpj_basico,
            identificador_socio,
            nome_socio_razao_social,
            cnpj_cpf_socio,
            qualificacao_socio,
            TRY_STRPTIME(data_entrada_sociedade, '%Y%m%d')::DATE AS data_entrada_sociedade,
            pais,
            representante_legal,
            nome_representante,
            qualificacao_representante_legal,
            faixa_etaria,
            {reference_month!r} AS mes
        FROM raw_socios
        """,
        dest_dir / "socios",
        partition_by=["mes"],
    )


def transform_simples(csv_paths: list[Path], dest_dir: Path, reference_month: str) -> Path:
    """Lê os CSVs brutos do Simples, tipa e escreve Parquet particionado por mês.

    Aqui o sentinela de data ausente é o literal "00000000", não campo vazio (ver docstring de
    `transform_estabelecimentos`) — `NULLIF(col, '00000000')` converte esse literal em NULL antes
    do TRY_STRPTIME, então data ausente e data malformada acabam no mesmo resultado (NULL).
    """
    con = duckdb.connect()
    _read_raw(con, csv_paths, "simples").create_view("raw_simples")

    return _write_silver(
        con,
        f"""
        SELECT
            cnpj_basico,
            opcao_simples,
            TRY_STRPTIME(NULLIF(data_opcao_simples, '00000000'), '%Y%m%d')::DATE
                AS data_opcao_simples,
            TRY_STRPTIME(NULLIF(data_exclusao_simples, '00000000'), '%Y%m%d')::DATE
                AS data_exclusao_simples,
            opcao_mei,
            TRY_STRPTIME(NULLIF(data_opcao_mei, '00000000'), '%Y%m%d')::DATE AS data_opcao_mei,
            TRY_STRPTIME(NULLIF(data_exclusao_mei, '00000000'), '%Y%m%d')::DATE
                AS data_exclusao_mei,
            {reference_month!r} AS mes
        FROM raw_simples
        """,
        dest_dir / "simples",
        partition_by=["mes"],
    )


def transform_dominio(
    entity: str, csv_paths: list[Path], dest_dir: Path, reference_month: str
) -> Path:
    """Lê e escreve uma das 6 tabelas de domínio código->descrição, sem regra de negócio nenhuma.

    Cobre Cnaes, Municipios, Naturezas, Paises, Qualificacoes e Motivos — todas compartilham o
    layout de 2 colunas `DOMINIO` em `schemas.py`. `entity` precisa ser uma dessas 6 chaves.
    """
    if entity not in _DOMINIO_ENTITIES:
        raise ValueError(f"'{entity}' não é uma tabela de domínio; opções: {_DOMINIO_ENTITIES}")

    con = duckdb.connect()
    _read_raw(con, csv_paths, entity).create_view("raw_dominio")

    return _write_silver(
        con,
        f"SELECT codigo, descricao, {reference_month!r} AS mes FROM raw_dominio",
        dest_dir / entity,
        partition_by=["mes"],
    )
