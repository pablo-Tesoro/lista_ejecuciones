-- ==========================================================
-- CITIUS LPE — GOLD
-- Nombres de esquema, tablas y columnas según el diseño de draw.io.
-- Gold usa camelCase; dp_update_ts se mantiene tal cual.
-- Sustituye ${catalog} por tu catálogo.
--
-- Gold expone SÓLO el estado vigente. El historial completo y las
-- consultas a fecha viven en silver, que es SCD2:
--   SELECT * FROM silver_master_executions_list.execution_list
--    WHERE valid_from_date <= :d
--      AND (valid_to_date IS NULL OR valid_to_date > :d);
--
-- El nombre del esquema lleva mayúsculas, así que va entre acentos
-- graves: Unity Catalog compara sin distinguir may/min, pero las
-- comillas evitan sorpresas al crearlo.
-- ==========================================================

CREATE SCHEMA IF NOT EXISTS ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`;


-- ----------------------------------------------------------
-- GOLD · execution_list
-- Grano: una fila por clave de negocio vigente.
-- Proyección de silver WHERE is_current.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.execution_list (
    caseNumber       STRING        NOT NULL COMMENT 'PK',
    name             STRING        NOT NULL COMMENT 'PK',
    debtAmount       DECIMAL(23,8) NOT NULL COMMENT 'PK',
    reason           STRING        NOT NULL COMMENT 'PK',
    letter           STRING        NOT NULL COMMENT 'FK -> letter_control.letter',
    court            STRING,
    orgUnit          STRING,
    enforcementAgent STRING,
    terminationDate  DATE,
    inclusionDate    DATE,
    rowHash          STRING        NOT NULL,
    dp_update_ts     TIMESTAMP     NOT NULL
)
USING DELTA
CLUSTER BY (letter, caseNumber)
COMMENT 'Estado vigente de la lista de ejecuciones';


-- ----------------------------------------------------------
-- GOLD · letter_control
-- Grano: una fila por letra (sólo la carga vigente).
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.letter_control (
    letter         STRING    NOT NULL COMMENT 'PK',
    totalRecords   INT       NOT NULL,
    lastVerifiedTs TIMESTAMP NOT NULL COMMENT 'Ultima vez que la letra se descargo del origen',
    dp_update_ts   TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Cobertura por letra: cuantos registros tiene y cuando se verifico';


-- ==========================================================
-- RESTRICCIONES
-- ==========================================================
ALTER TABLE ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.letter_control
  ADD CONSTRAINT pk_gold_letter_control PRIMARY KEY (letter);

ALTER TABLE ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.execution_list
  ADD CONSTRAINT pk_gold_execution_list PRIMARY KEY
      (caseNumber, name, debtAmount, reason);

ALTER TABLE ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.execution_list
  ADD CONSTRAINT fk_gold_execution_list_letter FOREIGN KEY (letter)
      REFERENCES ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.letter_control (letter);


-- ==========================================================
-- CONSTRUCCIÓN  (silver -> gold). Reconstrucción completa: gold es
-- derivada, barata de recalcular, y un rebuild elimina cualquier deriva.
-- ==========================================================
-- INSERT OVERWRITE ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.execution_list
-- SELECT s.case_number       AS caseNumber,
--        s.name              AS name,
--        s.debt_amount       AS debtAmount,
--        s.reason            AS reason,
--        s.letter            AS letter,
--        s.court             AS court,
--        s.org_unit          AS orgUnit,
--        s.enforcement_agent AS enforcementAgent,
--        s.termination_date  AS terminationDate,
--        s.inclusion_date    AS inclusionDate,
--        s.row_hash          AS rowHash,
--        current_timestamp() AS dp_update_ts
--   FROM ${catalog}.silver_master_executions_list.execution_list s
--  WHERE s.is_current;
--
-- INSERT OVERWRITE ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.letter_control
-- SELECT c.letter, c.total_records AS totalRecords,
--        c.last_verified_ts AS lastVerifiedTs, current_timestamp() AS dp_update_ts
--   FROM ${catalog}.silver_master_executions_list.letter_control c
--  WHERE c.is_current;


-- ==========================================================
-- CONSULTAS DE EXPLOTACIÓN
-- ==========================================================
-- Búsqueda por nombre (screening). El modelo no incluye columnas de
-- nombre normalizado ni tokens, así que la búsqueda va sobre `name`
-- en crudo; para tolerar acentos y orden hay que normalizar en la
-- consulta o añadir columnas derivadas al modelo.
--   SELECT * FROM ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.execution_list
--    WHERE upper(name) = upper(:q);
--
-- Cobertura por letra. lastVerifiedTs es la última descarga de la letra, no
-- su última comprobación: una letra estable se comprueba cada día sin dejar
-- rastro, así que la frescura de una respuesta negativa hay que tomarla del
-- historial de ejecuciones del job. dp_update_ts es el instante del rebuild.
--   SELECT letter, totalRecords, lastVerifiedTs
--     FROM ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.letter_control
--    ORDER BY lastVerifiedTs;
--
-- Estado a una fecha pasada (contra silver, no contra gold):
--   SELECT * FROM ${catalog}.silver_master_executions_list.execution_list
--    WHERE valid_from_date <= :d
--      AND (valid_to_date IS NULL OR valid_to_date > :d);


-- ==========================================================
-- MIGRACIÓN (sólo si letter_control de gold ya existía)
-- Gold se reconstruye entero en cada ejecución, así que basta con
-- recrearla: el siguiente rebuild la rellena desde Silver.
-- ==========================================================
-- DROP TABLE IF EXISTS ${catalog}.`gold_iberinformacional_crawlersPt_executionsList_001`.letter_control;
-- (y relanzar scripts/00_setup_tables.py)
