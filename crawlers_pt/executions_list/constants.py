"""Constantes del listado de Citius: no dependen del entorno."""

# Tablas sin catálogo: el catálogo llega como parámetro, igual que en el
# resto de procesos de crawlers_pt.
BRONZE_SCHEMA = "bronze_raw_executions_list"
SILVER_SCHEMA = "silver_master_executions_list"
# Lleva mayúsculas: va entre acentos graves, como en sql/gold.sql.
GOLD_SCHEMA = "`gold_iberinformacional_crawlersPt_executionsList_001`"

BRONZE_TABLE = f"{BRONZE_SCHEMA}.execution_list"
SILVER_TABLE = f"{SILVER_SCHEMA}.execution_list"
LETTER_CONTROL_TABLE = f"{SILVER_SCHEMA}.letter_control"
PROCESS_CONTROL_TABLE = f"{SILVER_SCHEMA}.process_control"
GOLD_TABLE = f"{GOLD_SCHEMA}.execution_list"
GOLD_LETTER_CONTROL_TABLE = f"{GOLD_SCHEMA}.letter_control"

# Clave del watermark en process_control: la escribe la transformación y la lee
# la ingesta. Si no coinciden, la ingesta no ve el watermark y borra Bronze.
PROCESS_ID = "Executions"

LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

# Campos del formulario ASP.NET
FIELD_NAME = "ctl00$ContentPlaceHolder1$txtNomeExecutado"
FIELD_ALPHA = "ctl00$ContentPlaceHolder1$ddlAlfabeto"
FIELD_DOC = "ctl00$ContentPlaceHolder1$txtDocumento"
FIELD_TIPO_DOC = "ctl00$ContentPlaceHolder1$rblTipoDocumento"
FIELD_PROC = "ctl00$ContentPlaceHolder1$txtNrProcesso"
FIELD_DIAS = "ctl00$ContentPlaceHolder1$rblDias"

BTN_SEARCH_NAME = "ctl00$ContentPlaceHolder1$btnSearch"
BTN_SEARCH_VALUE = "Pesquisar"

PERIOD_FORM_VALUES = {"15": "15", "30": "30", "todos": "todos"}

TIMEZONE = "Europe/Lisbon"
