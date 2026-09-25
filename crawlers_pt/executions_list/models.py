from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True)
class ParsedRecord:
    nome: str = ""
    valor_divida: str = ""
    n_processo: str = ""
    tribunal: str = ""
    un_org: str = ""
    agente_execucao: str = ""
    extincao: str = ""
    inclusao: str = ""
    motivo: str = ""


@dataclass(slots=True)
class PageResult:
    letter: str
    period: str
    page_number: int
    records_found: int
    record_count_label: Optional[int]
    has_next_page: bool
    page_signature: str
    raw_html_path: Optional[str] = None


@dataclass(slots=True)
class SegmentJob:
    id: int
    letter: str
    period: str
    status: str
    current_page: int
    attempts: int
    locked_by: Optional[str]
    lock_time: Optional[str]
    last_error: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
    updated_at: Optional[str]