from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import requests
from bs4 import BeautifulSoup

from .constants import (
    BTN_SEARCH_NAME,
    BTN_SEARCH_VALUE,
    FIELD_ALPHA,
    FIELD_DIAS,
    FIELD_DOC,
    FIELD_NAME,
    FIELD_PROC,
    PERIOD_FORM_VALUES,
)
from .settings import CrawlerSettings
from .parser import (
    build_soup,
    detect_next_page_number,
    extract_all_updatepanels,
    find_next_page_event_target,
    find_results_table,
    get_hidden_fields,
    page_signature_from_table,
    parse_record_count,
    parse_results_table,
)
from .utils import (
    backoff_sleep,
    jitter_sleep,
    stable_hash_from_records,
)


@dataclass(slots=True)
class SearchPage:
    soup: BeautifulSoup
    raw_html: str
    parsed_rows: list
    record_count: Optional[int]
    page_signature: str
    next_page_number: Optional[int]
    pager_event_target: Optional[str]


class CitiusCrawler:
    def __init__(self, logger: logging.Logger, settings: CrawlerSettings):
        self.logger = logger
        self.settings = settings
        self.session = self._build_session()

    # ==========================================================
    # SESSION
    # ==========================================================
    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
                "Referer": self.settings.url,
                "Origin": "https://www.citius.mj.pt",
                "Connection": "keep-alive",
            }
        )

        proxies = {}
        if self.settings.http_proxy:
            proxies["http"] = self.settings.http_proxy
        if self.settings.https_proxy:
            proxies["https"] = self.settings.https_proxy
        if proxies:
            session.proxies.update(proxies)

        return session

    def reset_session(self) -> None:
        self.logger.info("Reset da sessão HTTP.")
        try:
            self.session.close()
        finally:
            self.session = self._build_session()

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def _request_timeout(self) -> tuple[int, int]:
        return (self.settings.connect_timeout_s, self.settings.read_timeout_s)

    # ==========================================================
    # HTTP CORE
    # ==========================================================
    def _get_initial_page(self) -> BeautifulSoup:
        response = self.session.get(self.settings.url, timeout=self._request_timeout())
        response.raise_for_status()
        return build_soup(response.text)

    def _post(self, payload: dict) -> requests.Response:
        response = self.session.post(self.settings.url, data=payload, timeout=self._request_timeout())
        response.raise_for_status()
        return response

    def _extract_effective_html(self, response_text: str) -> str:
        raw = extract_all_updatepanels(response_text)

        if raw and raw.strip():
            return raw

        if response_text and response_text.strip():
            return response_text

        raise RuntimeError("Resposta vazia ou sem HTML útil.")

    # ==========================================================
    # HTML DEBUG SAVE
    # ==========================================================
    # ==========================================================
    # BUILD PAGE
    # ==========================================================
    def _build_search_page(self, raw_html: str, current_page: int) -> SearchPage:
        soup = build_soup(raw_html)
        table = find_results_table(soup)

        record_count = parse_record_count(soup)
        rows = parse_results_table(table) if table else []
        signature = page_signature_from_table(table) if table else ""
        next_page = detect_next_page_number(soup, current_page=current_page)
        event_target = find_next_page_event_target(soup) if next_page else None

        return SearchPage(
            soup=soup,
            raw_html=raw_html,
            parsed_rows=rows,
            record_count=record_count,
            page_signature=signature,
            next_page_number=next_page,
            pager_event_target=event_target,
        )

    # ==========================================================
    # FIRST PAGE
    # ==========================================================
    def fetch_first_page(self, letter: str, period: str) -> SearchPage:
        period_value = PERIOD_FORM_VALUES[period]
        last_error: Optional[Exception] = None

        for attempt in range(1, self.settings.retry_count + 2):
            try:
                landing = self._get_initial_page()
                hidden = get_hidden_fields(landing)

                payload = dict(hidden)
                payload["__EVENTTARGET"] = ""
                payload["__EVENTARGUMENT"] = ""
                payload[FIELD_NAME] = ""
                payload[FIELD_ALPHA] = letter
                payload[FIELD_DOC] = ""
                payload[FIELD_PROC] = ""
                payload[FIELD_DIAS] = period_value
                payload[BTN_SEARCH_NAME] = BTN_SEARCH_VALUE

                response = self._post(payload)
                raw = self._extract_effective_html(response.text)


                return self._build_search_page(raw, 1)

            except Exception as e:
                last_error = e


                self.logger.warning(f"Erro primeira página {letter}: {e}")

                if attempt <= self.settings.retry_count:
                    backoff_sleep(self.settings.backoff_base_s, attempt)
                    self.reset_session()

        raise RuntimeError(last_error)

    # ==========================================================
    # NEXT PAGE
    # ==========================================================
    def fetch_page(self, current_soup, target_page: int, letter: str) -> SearchPage:
        last_error: Optional[Exception] = None

        for attempt in range(1, self.settings.retry_count + 2):
            try:
                hidden = get_hidden_fields(current_soup)
                event_target = find_next_page_event_target(current_soup)

                payload = dict(hidden)
                payload["__EVENTTARGET"] = event_target
                payload["__EVENTARGUMENT"] = ""

                response = self._post(payload)
                raw = self._extract_effective_html(response.text)


                return self._build_search_page(raw, target_page)

            except Exception as e:
                last_error = e


                self.logger.warning(f"Erro página {target_page}: {e}")

                if attempt <= self.settings.retry_count:
                    backoff_sleep(self.settings.backoff_base_s, attempt)
                    self.reset_session()

        raise RuntimeError(last_error)

    # ==========================================================
    # SNAPSHOT (RECONCILIAÇÃO)
    # ==========================================================
    def _row_to_snapshot_dict(self, row) -> dict:
        fields = [
            "n_processo",
            "nome",
            "valor_divida",
            "motivo",
            "tribunal",
            "un_org",
            "agente_execucao",
            "extincao",
            "inclusao",
        ]
        return {f: getattr(row, f, "") for f in fields}

    def _snapshot_hash_from_rows(self, rows) -> str:
        if not rows:
            return stable_hash_from_records([], ["n_processo", "nome"])

        normalized = [self._row_to_snapshot_dict(r) for r in rows]

        fields = [
            "n_processo",
            "nome",
            "valor_divida",
            "motivo",
            "tribunal",
            "un_org",
            "agente_execucao",
            "extincao",
            "inclusao",
        ]

        available = [
            f for f in fields
            if any(r.get(f, "") not in ("", None) for r in normalized)
        ]

        if not available:
            available = ["n_processo", "nome"]

        return stable_hash_from_records(normalized, available)

    def fetch_letter_site_snapshot(self, *, letter: str, period: str) -> dict:
        first_page = self.fetch_first_page(letter=letter, period=period)

        total_results = int(first_page.record_count or 0)

        page1_rows = first_page.parsed_rows

        rows_per_page = len(page1_rows) if page1_rows else 10
        total_pages = max(1, (total_results + rows_per_page - 1) // rows_per_page)

        current = first_page
        current_page_number = 1

        while current_page_number < total_pages:
            next_page = current.next_page_number
            if next_page is None:
                break

            current = self.fetch_page(current.soup, next_page, letter)
            current_page_number = next_page

        last_rows = current.parsed_rows

        return {
            "total_results": total_results,
            "total_pages": current_page_number,
            "page1_hash": self._snapshot_hash_from_rows(page1_rows),
            "last_page_hash": self._snapshot_hash_from_rows(last_rows),
            "page1_count": len(page1_rows),
            "last_page_count": len(last_rows),
        }

    def polite_pause(self):
        jitter_sleep(self.settings.delay_min_s, self.settings.delay_max_s)