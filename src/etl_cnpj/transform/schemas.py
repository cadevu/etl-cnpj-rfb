"""Layout das colunas dos arquivos brutos (bronze) dos Dados Abertos do CNPJ.

Fonte: PDF de metadados da Receita Federal (`cnpj-metadados.pdf`). Os arquivos-fonte não têm
header — a ordem das colunas abaixo é o único jeito de interpretá-los corretamente. Este módulo
não valida nem tipa linhas individualmente (ver docstring de `Column`); ele só descreve o layout
usado para configurar a leitura em bulk (DuckDB) e para checar as fixtures de teste.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    """Metadado de uma coluna do layout bruto — não representa uma linha de dado."""

    name: str
    description: str


EMPRESAS: tuple[Column, ...] = (
    Column("cnpj_basico", "Número base de inscrição no CNPJ (8 primeiros dígitos)"),
    Column("razao_social", "Nome empresarial da pessoa jurídica"),
    Column("natureza_juridica", "Código da natureza jurídica"),
    Column("qualificacao_responsavel", "Qualificação da pessoa física responsável"),
    Column("capital_social", "Capital social da empresa (vírgula decimal)"),
    Column("porte_empresa", "Código do porte: 00 não informado, 01 micro, 03 pequeno, 05 demais"),
    Column("ente_federativo_responsavel", "Preenchido só para natureza jurídica 1XXX"),
)

ESTABELECIMENTOS: tuple[Column, ...] = (
    Column("cnpj_basico", "Número base de inscrição no CNPJ (8 primeiros dígitos)"),
    Column("cnpj_ordem", "Número do estabelecimento (dígitos 9-12 do CNPJ)"),
    Column("cnpj_dv", "Dígito verificador do CNPJ (2 últimos dígitos)"),
    Column("identificador_matriz_filial", "1 matriz, 2 filial"),
    Column("nome_fantasia", "Nome fantasia do estabelecimento"),
    Column("situacao_cadastral", "01 nula, 2 ativa, 3 suspensa, 4 inapta, 08 baixada"),
    Column("data_situacao_cadastral", "Data do evento da situação cadastral (AAAAMMDD)"),
    Column("motivo_situacao_cadastral", "Código do motivo da situação cadastral"),
    Column("nome_cidade_exterior", "Nome da cidade no exterior, se aplicável"),
    Column("pais", "Código do país, se estabelecimento no exterior"),
    Column("data_inicio_atividade", "Data de início da atividade (AAAAMMDD)"),
    Column("cnae_fiscal_principal", "Código da atividade econômica principal"),
    Column("cnae_fiscal_secundaria", "Códigos secundários separados por vírgula"),
    Column("tipo_logradouro", "Descrição do tipo de logradouro"),
    Column("logradouro", "Nome do logradouro"),
    Column("numero", "Número do endereço ('S/N' quando não houver)"),
    Column("complemento", "Complemento do endereço"),
    Column("bairro", "Bairro"),
    Column("cep", "Código de endereçamento postal"),
    Column("uf", "Sigla da unidade da federação"),
    Column("municipio", "Código do município de jurisdição"),
    Column("ddd_1", "DDD do telefone 1"),
    Column("telefone_1", "Número do telefone 1"),
    Column("ddd_2", "DDD do telefone 2"),
    Column("telefone_2", "Número do telefone 2"),
    Column("ddd_fax", "DDD do fax"),
    Column("fax", "Número do fax"),
    Column("correio_eletronico", "E-mail do contribuinte"),
    Column("situacao_especial", "Situação especial da empresa"),
    Column("data_situacao_especial", "Data da situação especial (AAAAMMDD)"),
)

SOCIOS: tuple[Column, ...] = (
    Column("cnpj_basico", "Número base de inscrição no CNPJ (8 primeiros dígitos)"),
    Column("identificador_socio", "1 pessoa jurídica, 2 pessoa física, 3 estrangeiro"),
    Column("nome_socio_razao_social", "Nome do sócio PF ou razão social do sócio PJ/estrangeiro"),
    Column("cnpj_cpf_socio", "CNPJ ou CPF do sócio (mascarado); vazio se estrangeiro"),
    Column("qualificacao_socio", "Código da qualificação do sócio"),
    Column("data_entrada_sociedade", "Data de entrada na sociedade (AAAAMMDD)"),
    Column("pais", "Código do país do sócio estrangeiro"),
    Column("representante_legal", "CPF do representante legal (mascarado)"),
    Column("nome_representante", "Nome do representante legal"),
    Column("qualificacao_representante_legal", "Código da qualificação do representante legal"),
    Column("faixa_etaria", "Faixa etária do sócio, já calculada pela Receita (0 a 9)"),
)

SIMPLES: tuple[Column, ...] = (
    Column("cnpj_basico", "Número base de inscrição no CNPJ (8 primeiros dígitos)"),
    Column("opcao_simples", "S sim, N não, vazio outros"),
    Column("data_opcao_simples", "Data de opção pelo Simples (AAAAMMDD)"),
    Column("data_exclusao_simples", "Data de exclusão do Simples (AAAAMMDD ou '00000000')"),
    Column("opcao_mei", "S sim, N não, vazio outros"),
    Column("data_opcao_mei", "Data de opção pelo MEI (AAAAMMDD)"),
    Column("data_exclusao_mei", "Data de exclusão do MEI (AAAAMMDD ou '00000000')"),
)

# Cnaes, Municipios, Naturezas, Paises, Qualificacoes e Motivos são todas tabelas de domínio
# código -> descrição, com o mesmo layout de 2 colunas.
DOMINIO: tuple[Column, ...] = (
    Column("codigo", "Código do registro de domínio"),
    Column("descricao", "Descrição do registro de domínio"),
)

TABLES: dict[str, tuple[Column, ...]] = {
    "empresas": EMPRESAS,
    "estabelecimentos": ESTABELECIMENTOS,
    "socios": SOCIOS,
    "simples": SIMPLES,
    "cnaes": DOMINIO,
    "municipios": DOMINIO,
    "naturezas": DOMINIO,
    "paises": DOMINIO,
    "qualificacoes": DOMINIO,
    "motivos": DOMINIO,
}


def column_names(entity: str) -> list[str]:
    return [column.name for column in TABLES[entity]]
