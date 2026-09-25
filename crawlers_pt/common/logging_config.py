"""
Configuración de logging de crawlers_pt.

ESTE FICHERO ES UN SUSTITUTO: si en el repo real ya existe este módulo
compartido, borra este y quédate con el vuestro. La ingesta sólo llama a
`setup_logging("<nombre del proceso>")` una vez, en el punto de entrada.
"""
from __future__ import annotations

import logging
import sys
import threading

_LOCK = threading.Lock()


def setup_logging(process_name: str, level: str = "INFO") -> None:
    """Deja el logger raíz escribiendo a stdout, que es lo que captura Databricks."""
    with _LOCK:
        root = logging.getLogger()
        root.setLevel(getattr(logging, level.upper(), logging.INFO))
        if any(getattr(h, "_crawlers_pt", False) for h in root.handlers):
            return
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            f"%(asctime)s [%(levelname)s] [{process_name}] [%(name)s] %(message)s"))
        handler._crawlers_pt = True
        root.addHandler(handler)
