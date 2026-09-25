from __future__ import annotations

import hashlib
import json
import random
import re
import time
import unicodedata
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .constants import TIMEZONE


# ==========================================================
# TIME
# ==========================================================
# O cluster corre em UTC. Sem fuso explícito, now_iso()/today_ymd()
# mudavam de valor face ao ambiente local e deslocavam o run_date
# (e os nomes das pastas) perto da meia-noite.
_TZ = ZoneInfo(TIMEZONE)


def local_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(_TZ)


def now_iso() -> str:
    return local_now().isoformat(timespec="seconds")


def today_ymd() -> str:
    return local_now().strftime("%Y-%m-%d")


def format_elapsed(total_seconds: int | float | None) -> str:
    if total_seconds is None:
        return "n/d"

    total_seconds = max(0, int(total_seconds))

    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)

    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)


# ==========================================================
# NORMALIZAÇÃO
# ==========================================================
def normalize_text(value: Any) -> str:
    if value is None:
        return ""

    text = str(value)
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text_for_identity(value: Any) -> str:
    text = normalize_text(value).upper()

    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))

    return re.sub(r"\s+", " ", text).strip()


def normalize_case_number(value: Any) -> str:
    return normalize_text_for_identity(value).replace(" ", "")


def normalize_money(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    text = text.replace("\u00A0", " ").replace("€", "").strip()

    # 🔥 melhoria: só remove '.' se existir vírgula (milhares PT)
    if "," in text:
        text = text.replace(".", "")

    text = text.replace(" ", "").replace(",", ".")

    try:
        amount = Decimal(text)
        return format(amount.quantize(Decimal("0.01")), "f")
    except (InvalidOperation, ValueError):
        return normalize_text_for_identity(value)


def normalize_date(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    match = re.fullmatch(r"(\d{2})[-/](\d{2})[-/](\d{4})", text)
    if match:
        dd, mm, yyyy = match.groups()
        return f"{yyyy}-{mm}-{dd}"

    return normalize_text_for_identity(value)


# ==========================================================
# HASHING CORE
# ==========================================================
def canonical_record_identity(
    *,
    nome: Any,
    n_processo: Any,
    valor_divida: Any = "",
    tribunal: Any = "",
    un_org: Any = "",
    agente_execucao: Any = "",
    extincao: Any = "",
    inclusao: Any = "",
    motivo: Any = "",
) -> str:
    parts = [
        normalize_case_number(n_processo),
        normalize_text_for_identity(nome),
        normalize_money(valor_divida),
        normalize_text_for_identity(motivo),
        normalize_date(extincao),
        normalize_date(inclusao),
        normalize_text_for_identity(tribunal),
        normalize_text_for_identity(un_org),
        normalize_text_for_identity(agente_execucao),
    ]
    return "||".join(parts)


def record_hash(*parts: Any) -> str:
    normalized = "||".join(normalize_text_for_identity(p) for p in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def record_hash_from_identity(
    *,
    nome: Any,
    n_processo: Any,
    valor_divida: Any = "",
    tribunal: Any = "",
    un_org: Any = "",
    agente_execucao: Any = "",
    extincao: Any = "",
    inclusao: Any = "",
    motivo: Any = "",
) -> str:
    canonical = canonical_record_identity(
        nome=nome,
        n_processo=n_processo,
        valor_divida=valor_divida,
        tribunal=tribunal,
        un_org=un_org,
        agente_execucao=agente_execucao,
        extincao=extincao,
        inclusao=inclusao,
        motivo=motivo,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def content_hash_from_record(
    *,
    nome: Any,
    n_processo: Any,
    valor_divida: Any = "",
    tribunal: Any = "",
    un_org: Any = "",
    agente_execucao: Any = "",
    extincao: Any = "",
    inclusao: Any = "",
    motivo: Any = "",
    letter: Any = "",
    period: Any = "",
) -> str:
    parts = [
        normalize_text_for_identity(letter),
        normalize_text_for_identity(period),
        normalize_case_number(n_processo),
        normalize_text_for_identity(nome),
        normalize_money(valor_divida),
        normalize_text_for_identity(tribunal),
        normalize_text_for_identity(un_org),
        normalize_text_for_identity(agente_execucao),
        normalize_date(extincao),
        normalize_date(inclusao),
        normalize_text_for_identity(motivo),
    ]
    return hashlib.sha256("||".join(parts).encode("utf-8")).hexdigest()


# ==========================================================
# HASH DE LISTAS
# ==========================================================
def stable_hash(items: Iterable[Any]) -> str:
    normalized = [normalize_text_for_identity(i) for i in items]
    normalized.sort()
    return hashlib.sha256("\n".join(normalized).encode()).hexdigest()


def normalize_page_record(record: dict[str, Any], fields) -> str:
    parts = []

    for field in fields:
        value = record.get(field, "")
        f = field.lower()

        if "processo" in f:
            parts.append(normalize_case_number(value))
        elif "valor" in f:
            parts.append(normalize_money(value))
        elif "data" in f or f in {"inclusao", "extincao"}:
            parts.append(normalize_date(value))
        else:
            parts.append(normalize_text_for_identity(value))

    return "||".join(parts)


def stable_hash_from_records(records, fields) -> str:
    rows = [normalize_page_record(r, fields) for r in records]
    rows.sort()
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


# ==========================================================
# JSON
# ==========================================================
def json_dumps_sorted(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


# ==========================================================
# SLEEP
# ==========================================================
def jitter_sleep(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))


def backoff_sleep(base_s: float, attempt: int, max_extra_jitter: float = 1.0) -> None:
    delay = base_s * (2 ** max(0, attempt - 1))
    delay += random.uniform(0, max_extra_jitter)
    time.sleep(delay)


# ==========================================================
# FILESYSTEM
# ==========================================================


