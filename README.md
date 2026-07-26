# etl-cnpj

Pipeline de dados dos **Dados Abertos do CNPJ** (Receita Federal): baixa o snapshot mensal completo
do cadastro de empresas/estabelecimentos/sócios, transforma em Parquet tipado (camada silver) e
carrega num Postgres consultável por CNPJ e por sócio (camada gold). Orquestrado por Airflow.
Projeto de estudo — arquitetura e ferramental escolhidos para serem representativos do mercado.

## Arquitetura

Medalhão (bronze → silver → gold):

- **bronze** (`data/bronze/`): zip mensal cru, exatamente como baixado da Receita Federal. Imutável.
- **silver** (`data/silver/`): Parquet particionado por entidade/mês, tipado, com regras de negócio
  aplicadas (ex.: máscara de CPF, split de CNAE secundária). Gerado via DuckDB (bulk SQL) + Polars
  (funções puras testáveis em `transform/rules.py`).
- **gold** (Postgres): camada de serving, com índices trigram para busca por nome de sócio/razão
  social/fantasia. Carga via staging + upsert idempotente.

Decisões de arquitetura (por quê):

- **DuckDB + Polars, não Spark**: o volume (dezenas de GB descomprimidos) cabe numa stack
  single-node, com custo e complexidade operacional muito menores que um cluster Spark.
- **Apache Airflow, não Dagster/cron**: é o orquestrador mais pedido no mercado de dados no Brasil —
  o objetivo aqui inclui aprender a ferramenta em si.
- **PostgreSQL como serving layer**: consultas pontuais por CNPJ/sócio, não OLAP puro — RDBMS com
  índice trigram (`pg_trgm`) resolve isso melhor que um data warehouse colunar.
- **Local-first**: todo o pipeline roda via Docker Compose antes de qualquer coisa ir para a Azure,
  para não gastar créditos iterando no design.

Pitfalls de domínio documentados em [`.claude/plans/`](.) (ver plano de implementação) — os mais
importantes: todo código (CNPJ, CEP, município, CNAE) é `VARCHAR` (nunca `INTEGER`, perde zero à
esquerda); `CAPITAL_SOCIAL` usa vírgula decimal; a tabela de domínio `Municipios` não tem UF; sócios
não têm chave natural única.

## Desenvolvimento local

Pré-requisitos: [`uv`](https://docs.astral.sh/uv/) (gerencia a versão do Python automaticamente —
não precisa ter Python 3.12 instalado manualmente, o `uv sync` baixa o interpretador necessário).

```bash
uv sync --all-groups        # instala dependências (e o Python 3.12, se preciso)
uv run pre-commit install   # ativa os hooks de lint no git commit
make check                  # lint + testes
```

> **Nota sobre versão do Python**: o projeto pina Python 3.12 (`.python-version`), mesmo que o
> sistema tenha uma versão mais nova — o Apache Airflow (Fase 3) ainda não suporta as versões mais
> recentes do Python no momento em que este projeto foi iniciado. O `uv` isola isso automaticamente,
> sem precisar mexer no Python do sistema.

## Estrutura

```
src/etl_cnpj/
├── config.py         # configuração via variáveis de ambiente (pydantic-settings)
├── extraction/        # download do zip mensal + extração dos zips aninhados
├── transform/          # parsing/tipagem/regras de negócio -> Parquet (silver)
├── load/               # DDL do schema gold + carga staging/upsert no Postgres
├── metrics/            # instrumentação de duração/linhas processadas por task
└── api/                 # API de consulta (FastAPI)
dags/                    # DAG(s) Airflow
tests/                   # unit / integration / data_quality
scripts/                 # utilitários de linha de comando (fixture, run local, custo)
infra/terraform/         # esboço de infraestrutura Azure (não aplicado ainda)
```

## Status

- [x] Fase 0 — scaffolding e tooling
- [ ] Fase 1 — transformação (bronze → silver) + testes unitários
- [ ] Fase 2 — carga Postgres (silver → gold)
- [ ] Fase 3 — orquestração Airflow local
- [ ] Fase 4 — API de consulta
- [ ] Fase 5 — instrumentação de tempo/custo
- [ ] Fase 6 — esboço de migração Azure
