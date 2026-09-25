import hashlib
import re
from typing import Optional

from bs4 import BeautifulSoup, Tag

from .models import ParsedRecord


GRID_ID = "ctl00_ContentPlaceHolder1_grdView"
RECORD_COUNT_ID = "ctl00_ContentPlaceHolder1_lblRecordCount"


# ==========================================================
# AJAX / UPDATE PANEL
# ==========================================================
def extract_all_updatepanels(text: str) -> str:
    """
    Respostas ASP.NET AJAX podem vir no formato pipe-delimited.
    Reconstrói o HTML dos update panels.
    """
    if "|updatePanel|" not in text:
        return text

    parts = text.split("|")
    chunks = []

    for i in range(len(parts) - 2):
        if parts[i] == "updatePanel":
            panel_id = parts[i + 1]
            panel_html = parts[i + 2]
            chunks.append(f"<!-- updatePanel:{panel_id} -->\n{panel_html}\n")

    return "\n".join(chunks) if chunks else text


def build_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


# ==========================================================
# FORM STATE
# ==========================================================
def get_hidden_fields(soup: BeautifulSoup) -> dict[str, str]:
    hidden = {}
    for inp in soup.select("input[type=hidden]"):
        name = inp.get("name")
        if name:
            hidden[name] = inp.get("value", "")
    return hidden


# ==========================================================
# RECORD COUNT
# ==========================================================
def parse_record_count(soup: BeautifulSoup) -> Optional[int]:
    sp = soup.find("span", id=RECORD_COUNT_ID)
    if not sp:
        return None

    txt = sp.get_text(" ", strip=True)

    # exemplo: "17 231 Registos encontrados"
    compact = re.sub(r"[^\d]", "", txt)

    if not compact:
        return None

    return int(compact)


# ==========================================================
# TABLE
# ==========================================================
def find_results_table(soup: BeautifulSoup) -> Optional[Tag]:
    table = soup.find("table", id=GRID_ID)
    return table if isinstance(table, Tag) else None


def _extract_labeled_value(td: Tag, label: str) -> str:
    target = label.strip().lower()

    for strong in td.find_all("strong"):
        strong_txt = strong.get_text(" ", strip=True).strip().lower()

        if strong_txt == target:
            nxt = strong.next_sibling

            for _ in range(30):
                if nxt is None:
                    break

                if getattr(nxt, "name", None) == "span":
                    return nxt.get_text(" ", strip=True)

                if isinstance(nxt, str):
                    txt = nxt.strip()
                    if txt:
                        return txt

                nxt = getattr(nxt, "next_sibling", None)

            break

    return ""


def parse_results_table(table: Tag) -> list[ParsedRecord]:
    rows: list[ParsedRecord] = []

    trs = table.find_all("tr")
    if len(trs) <= 1:
        return rows

    for tr in trs[1:]:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        try:
            nome = tds[0].get_text(" ", strip=True)
            valor = tds[1].get_text(" ", strip=True)

            proc_td = tds[2]
            n_processo = _extract_labeled_value(proc_td, "Processo:")
            tribunal = _extract_labeled_value(proc_td, "Tribunal:")
            un_org = _extract_labeled_value(proc_td, "Un. Org.:")
            agente = _extract_labeled_value(proc_td, "Agente Execução:")

            date_spans = tds[3].find_all("span")
            extincao = date_spans[0].get_text(" ", strip=True) if len(date_spans) >= 1 else ""
            inclusao = date_spans[1].get_text(" ", strip=True) if len(date_spans) >= 2 else ""

            motivo = tds[4].get_text(" ", strip=True)

            record = ParsedRecord(
                nome=nome,
                valor_divida=valor,
                n_processo=n_processo,
                tribunal=tribunal,
                un_org=un_org,
                agente_execucao=agente,
                extincao=extincao,
                inclusao=inclusao,
                motivo=motivo,
            )

            # filtro defensivo
            if record.nome or record.n_processo or record.motivo:
                rows.append(record)

        except Exception:
            # nunca rebenta o parsing global
            continue

    return rows


# ==========================================================
# PAGE SIGNATURE
# ==========================================================
def page_signature_from_table(table: Optional[Tag]) -> str:
    if table is None:
        return ""

    text = table.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) > 20000:
        text = text[:20000]

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ==========================================================
# PAGINATION (CRÍTICO)
# ==========================================================
def _is_next_disabled(soup: BeautifulSoup) -> bool:
    btn = soup.find("input", id="ctl00_ContentPlaceHolder1_Pager1_btnNextPage")
    if btn:
        if btn.has_attr("disabled"):
            return True
        if "seguinte_gray" in (btn.get("src") or "").lower():
            return True

    btn = soup.find("input", id="ctl00_ContentPlaceHolder1_Pager2_btnNextPage")
    if btn:
        if btn.has_attr("disabled"):
            return True
        if "seguinte_gray" in (btn.get("src") or "").lower():
            return True

    return False


def detect_next_page_number(soup: BeautifulSoup, current_page: int) -> Optional[int]:
    if _is_next_disabled(soup):
        return None

    if soup.find("a", id="ctl00_ContentPlaceHolder1_Pager1_lnkNext"):
        return current_page + 1

    if soup.find("a", id="ctl00_ContentPlaceHolder1_Pager2_lnkNext"):
        return current_page + 1

    return None


# ==========================================================
# EVENT TARGET
# ==========================================================
def _extract_event_target_from_href(href: str) -> Optional[str]:
    if not href:
        return None

    m = re.search(r"__doPostBack\('([^']+)'", href)
    return m.group(1) if m else None


def find_next_page_event_target(soup: BeautifulSoup) -> Optional[str]:
    if _is_next_disabled(soup):
        return None

    for pager_id in [
        "ctl00_ContentPlaceHolder1_Pager1_lnkNext",
        "ctl00_ContentPlaceHolder1_Pager2_lnkNext",
    ]:
        a = soup.find("a", id=pager_id)
        if a:
            href = a.get("href", "") or ""
            target = _extract_event_target_from_href(href)
            if target:
                return target

    return None