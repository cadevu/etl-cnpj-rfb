-- Extensões necessárias à camada gold.
-- Roda automaticamente no primeiro boot de um volume vazio (docker-entrypoint-initdb.d).
--
-- pg_trgm: índices por trigrama, usados na busca textual por razão social,
-- nome fantasia e nome de sócio. Sem ele, `ILIKE '%termo%'` faz seq scan.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- unaccent: normaliza acentuação na busca ("JOSE" encontra "JOSÉ").
-- Os dados da Receita vêm em Latin-1 com acentuação preservada.
CREATE EXTENSION IF NOT EXISTS unaccent;
