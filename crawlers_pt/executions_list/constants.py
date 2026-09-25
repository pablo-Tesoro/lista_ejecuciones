"""Constantes del listado de Citius: no dependen del entorno."""

# Tablas sin catálogo: el catálogo llega como parámetro, igual que en el
# resto de procesos de crawlers_pt.
BRONZE_SCHEMA = "bronze_raw_executions_list"
SILVER_SCHEMA = "silver_master_executions_list"

BRONZE_TABLE = f"{BRONZE_SCHEMA}.execution_list"
SILVER_TABLE = f"{SILVER_SCHEMA}.execution_list"
LETTER_CONTROL_TABLE = f"{SILVER_SCHEMA}.letter_control"
PROCESS_CONTROL_TABLE = f"{SILVER_SCHEMA}.process_control"

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
