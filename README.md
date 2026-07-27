# etl-cnpj

Pipeline de dados dos **Dados Abertos do CNPJ** (Receita Federal): baixa o snapshot mensal completo
do cadastro de empresas/estabelecimentos/sócios, transforma em Parquet tipado (camada silver) e
carrega num Postgres consultável por CNPJ e por sócio (camada gold). Orquestrado por Airflow.
Projeto de estudo — arquitetura e ferramental escolhidos para serem representativos do mercado.

## Arquitetura

Medalhão (bronze → silver → gold):

- **bronze** (`data/bronze/`): zip mensal cru, exatamente como baixado da Receita Federal. Imutável.
- **silver** (`data/silver/`): Parquet tipado, particionado por mês (e por UF, no caso de
  `estabelecimentos`), com as regras de negócio já aplicadas (datas `AAAAMMDD` com os dois sentinelas
  de "ausente", split de CNAE secundária, capital social com vírgula decimal). Gerado 100% em SQL bulk
  do DuckDB (`transform/pipeline.py`) — as mesmas regras também existem como funções Python puras e
  testadas em `transform/rules.py`, usadas como referência de comportamento esperado e conferidas
  contra o SQL por testes de paridade, não chamadas em produção (rodar por linha em Python jogaria fora
  o ganho de performance de usar DuckDB).
- **gold** (Postgres): camada de serving, com índices trigram para busca por nome de sócio/razão
  social/fantasia. Carga via staging + upsert idempotente. Ainda não implementada (Fase 2).

Decisões de arquitetura (por quê):

- **DuckDB, não Spark**: o volume (dezenas de GB descomprimidos) cabe numa stack single-node, com
  custo e complexidade operacional muito menores que um cluster Spark. Cogitamos usar Polars para as
  regras de negócio, mas acabou não sendo necessário — ver bullet da camada silver acima.
- **Apache Airflow, não Dagster/cron**: é o orquestrador mais pedido no mercado de dados no Brasil —
  o objetivo aqui inclui aprender a ferramenta em si.
- **PostgreSQL como serving layer**: consultas pontuais por CNPJ/sócio, não OLAP puro — RDBMS com
  índice trigram (`pg_trgm`) resolve isso melhor que um data warehouse colunar.
- **Local-first**: todo o pipeline roda via Docker Compose antes de qualquer coisa ir para a Azure,
  para não gastar créditos iterando no design.

Pitfalls de domínio documentados em [`CLAUDE.md`](CLAUDE.md) — os mais importantes: todo código (CNPJ,
CEP, município, CNAE) é `VARCHAR` (nunca `INTEGER`, perde zero à esquerda); `CAPITAL_SOCIAL` usa vírgula
decimal; a tabela de domínio `Municipios` não tem UF; sócios não têm chave natural única.

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
├── config.py            # configuração via variáveis de ambiente (pydantic-settings)
├── extraction/          # extração dos zips aninhados do zip bronze (bronze_zip.py)
├── transform/
│   ├── schemas.py       # layout das colunas brutas (as fontes não têm header)
│   ├── rules.py         # regras de negócio puras/testadas (referência, não chamado em produção)
│   └── pipeline.py      # bronze -> silver via DuckDB, uma função por entidade
├── load/                # DDL do schema gold + carga staging/upsert no Postgres (Fase 2, não criado)
├── metrics/             # instrumentação de duração/linhas processadas por task (Fase 5, não criado)
└── api/                 # API de consulta, FastAPI (Fase 4, não criado)
dags/                    # DAG(s) Airflow (Fase 3, não criado)
tests/                   # unit / integration (data_quality ainda não existe)
scripts/                 # run_pipeline.py — executa bronze -> silver contra o zip real
infra/terraform/         # esboço de infraestrutura Azure (não aplicado ainda)
```

## Status

- [x] Fase 0 — scaffolding e tooling
- [x] Fase 1 — transformação (bronze → silver) + testes unitários (10 tabelas, testado com o zip real
      via `scripts/run_pipeline.py`; investigação aberta: ~3 mil linhas de `Empresas` com
      `porte_empresa` nulo)
- [ ] Fase 2 — carga Postgres (silver → gold)
- [ ] Fase 3 — orquestração Airflow local
- [ ] Fase 4 — API de consulta
- [ ] Fase 5 — instrumentação de tempo/custo
- [ ] Fase 6 — esboço de migração Azure
