-- Gold de executions_list: el estado vigente de Silver, en camelCase, para el
-- screening. Lo publica executions_list_gold.py con INSERT OVERWRITE.
-- {catalog} es el catálogo del entorno.

CREATE SCHEMA IF NOT EXISTS {catalog}.gold_executions_list;

CREATE TABLE IF NOT EXISTS {catalog}.gold_executions_list.execution_list (
    caseNumber        STRING        NOT NULL,
    name              STRING        NOT NULL,
    debtAmount        DECIMAL(23,8) NOT NULL,
    reason            STRING        NOT NULL,
    letter            STRING        NOT NULL,
    court             STRING,
    orgUnit           STRING,
    enforcementAgent  STRING,
    terminationDate   DATE,
    inclusionDate     DATE,
    validFromDate     TIMESTAMP     NOT NULL,
    dp_update_ts      TIMESTAMP     NOT NULL,
    CONSTRAINT pk_gold_execution_list PRIMARY KEY (caseNumber, name, debtAmount, reason)
) USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.gold_executions_list.letter_control (
    letter          STRING    NOT NULL,
    totalRecords    INT       NOT NULL,
    validFromDate   TIMESTAMP NOT NULL,
    lastVerifiedTs  TIMESTAMP NOT NULL,
    dp_update_ts    TIMESTAMP NOT NULL,
    CONSTRAINT pk_gold_letter_control PRIMARY KEY (letter)
) USING DELTA;

-- Tabla de salida de DQ: una fila por control y ejecución; todas las filas de
-- una ejecución comparten dp_update_ts. decision: publish | block.
CREATE TABLE IF NOT EXISTS {catalog}.gold_executions_list.dq_results (
    process_id       STRING    NOT NULL,
    check_name       STRING    NOT NULL,
    severity         STRING    NOT NULL,
    target_table     STRING    NOT NULL,
    status           STRING    NOT NULL,
    failed_rows      BIGINT    NOT NULL,
    checked_rows     BIGINT    NOT NULL,
    description      STRING,
    source_snapshot  STRING,
    decision         STRING    NOT NULL,
    dp_update_ts     TIMESTAMP NOT NULL
) USING DELTA;
