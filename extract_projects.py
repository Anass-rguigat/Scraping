# -*- coding: utf-8 -*-
"""
Extract investment projects from CRI sector PDFs.
Uses only direct text extraction, deterministic regex rules, and simple calculations.
No NLP, ML, or AI-based interpretation.
Processes the Agro-Alimentaire PDF only; outputs output_projects.csv.
"""

import csv
import re
import unicodedata
from pathlib import Path

import pdfplumber
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_CSV = SCRIPT_DIR / "output_projects.csv"
CURRENCY = "MAD"
LANGUAGE = "FR"
SOURCE_TYPE = "CRI"

# PDF to process (Agro-Alimentaire only)
PDF_FILENAMES = [
    "Fiches-de-projet-Agro-Alimentaire-23052025.pdf",
]

# Investment range thresholds (MAD)
INV_LOW_MAX = 5_000_000
INV_MEDIUM_MAX = 20_000_000

# Standard ROI assumption when payback not in PDF
ROI_ASSUMPTION_YEARS = 6

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _remove_accents(s: str) -> str:
    """Remove accents deterministically (NFD and strip combining marks)."""
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def normalize_for_reference(s: str) -> str:
    """Uppercase, replace spaces with hyphens, remove accents. For project_reference."""
    if not s or not s.strip():
        return ""
    s = _remove_accents(s.strip())
    s = re.sub(r"[^\w\s\-]", "", s)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s.upper() if s else ""


def _fix_missing_spaces_description(block: str) -> str:
    """
    Reinsert spaces using only structural rules (no word list). Dynamic for any project.
    - Comma without space: ,letter -> , letter
    - Apostrophe (élision): letter + ' + letter -> letter + space + ' + letter when letter is not l/d
      (so "quel'ail" -> "que l'ail", "huiled'ail" -> "huile d'ail", without breaking "l'ail" / "d'ail")
    - Period/paren followed by letter: .letter -> . letter, )letter -> ) letter
    """
    if not block or len(block) < 2:
        return block
    # 1) Comma without space after
    block = re.sub(r",([a-zàâäéèêëïîôùûüç])", r", \1", block, flags=re.IGNORECASE)
    # 2) Apostrophe: (letter not l/d) + apostrophe + letter -> letter + space + apostrophe + letter
    #    Handles quel'ail -> que l'ail, huiled'ail -> huile d'ail; keeps l'ail, d'ail intact
    block = re.sub(
        r"((?![ld])[a-zàâäéèêëïîôùûüç])([''\u2019])([a-zàâäéèêëïîôùûüç])",
        r"\1 \2\3",
        block,
        flags=re.IGNORECASE,
    )
    # 3) Period or closing paren followed by letter (no space)
    block = re.sub(r"\.([a-zàâäéèêëïîôùûüç])", r". \1", block, flags=re.IGNORECASE)
    block = re.sub(r"\)([a-zàâäéèêëïîôùûüç])", r") \1", block, flags=re.IGNORECASE)
    # 4) Opening paren preceded by letter: letter( -> letter (
    block = re.sub(r"([a-zàâäéèêëïîôùûüç])\(([A-Z])", r"\1 (\2", block, flags=re.IGNORECASE)
    return block


def _sector_from_filename(pdf_path: Path) -> str:
    """Infer sector label from PDF filename when FILIÈRE not in text."""
    name = pdf_path.stem.lower()
    if "agro-alimentaire" in name or "agroalimentaire" in name:
        return "AGROALIMENTAIRE"
    if "agriculture" in name:
        return "AGRICULTURE"
    if "tourisme" in name:
        return "TOURISME"
    if "artisanat" in name:
        return "ARTISANAT"
    if "industrie" in name:
        return "INDUSTRIE"
    if "nouvelles-technologies" in name or "technologies" in name:
        return "NOUVELLES TECHNOLOGIES"
    return "CRI"


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------
def extract_text_per_page(pdf_path: Path) -> list[tuple[int, str]]:
    """Extract raw text from each page. Returns list of (page_number_1based, text)."""
    pages_text = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            pages_text.append((i + 1, text or ""))
    return pages_text


def find_project_start_pages(pages_text: list[tuple[int, str]]) -> list[int]:
    """
    Detect pages where a new project starts.
    Matches: PROJET N°36, PROJET N°A-001, PROJET N°T 001, or PROJET :
    Returns 1-based page numbers.
    """
    starts = []
    for page_num, text in pages_text:
        if page_num == 1:
            continue
        # PROJET N°36, N°A-001, N°T 001, N°AR-001, N° NT-001
        if re.search(r"PROJET\s+N[°\s]*[A-Za-z]*[\-\s]*\d+", text, re.IGNORECASE):
            starts.append(page_num)
    if starts:
        return sorted(starts)
    # Fallback: pages with "PROJET" and (FILIÈRE or DESCRIPTION)
    for page_num, text in pages_text:
        if page_num == 1:
            continue
        if re.search(r"PROJET\s*[:\-]", text, re.IGNORECASE) and (
            "FILIÈRE" in text or "FILIERE" in text or "DESCRIPTION" in text
        ):
            starts.append(page_num)
    return sorted(set(starts))


def get_project_text_block(pages_text: list[tuple[int, str]], start_page: int) -> str:
    """Get concatenated text for a project: start_page and next page (if any)."""
    block = []
    for pnum, text in pages_text:
        if pnum == start_page or pnum == start_page + 1:
            block.append(text)
    return "\n".join(block)


# Left column ratio for two-column layout (description left, PRÉREQUIS right). No keyword logic.
LEFT_COLUMN_RATIO = 0.5
# Horizontal gap (points) between chars to treat as word boundary (insert space). From PDF layout.
CHAR_GAP_SPACE_THRESHOLD = 2.5


def extract_text_from_left_column(
    pdf_path: Path,
    start_page: int,
    num_pages: int = 2,
    left_ratio: float = LEFT_COLUMN_RATIO,
) -> str:
    """
    Extract text from the LEFT side of the page only (layout-based, no keywords).
    Uses page.chars + horizontal gap: insert space when gap between consecutive
    chars exceeds CHAR_GAP_SPACE_THRESHOLD. Preserves PDF layout spacing; dynamic for any project.
    """
    parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for i in range(num_pages):
            page_idx = (start_page - 1) + i
            if page_idx < 0 or page_idx >= len(pdf.pages):
                continue
            page = pdf.pages[page_idx]
            w = float(page.width)
            x_max_left = w * left_ratio
            chars = getattr(page, "chars", None)
            if not chars:
                # Fallback: extract_text on full page then crop by line (less reliable)
                text = page.extract_text() or ""
                parts.append(text.strip())
                continue
            left_chars = [c for c in chars if c["x1"] <= x_max_left]
            left_chars.sort(key=lambda c: (round(c["top"], 1), c["x0"]))
            if not left_chars:
                continue
            buf = []
            prev = left_chars[0]
            buf.append(prev.get("text", prev.get("char", "")))
            for c in left_chars[1:]:
                gap = c["x0"] - prev["x1"]
                top_diff = c["top"] - prev["top"]
                if top_diff > 3:
                    buf.append("\n")
                elif gap > CHAR_GAP_SPACE_THRESHOLD:
                    buf.append(" ")
                buf.append(c.get("text", c.get("char", "")))
                prev = c
            if buf:
                parts.append("".join(buf).replace(" \n ", "\n").strip())
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Field extraction (deterministic regex only)
# ---------------------------------------------------------------------------
def _first_match(text: str, pattern: str, group: int = 1) -> str | None:
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if m and m.lastindex >= group:
        return m.group(group).strip() or None
    return None


def _first_match_int(text: str, pattern: str, group: int = 1) -> int | None:
    s = _first_match(text, pattern, group)
    if s is None:
        return None
    s = re.sub(r"[\s\u00a0]", "", s).replace(",", ".")
    try:
        return int(float(s))
    except ValueError:
        return None


def _first_match_float(text: str, pattern: str, group: int = 1) -> float | None:
    s = _first_match(text, pattern, group)
    if s is None:
        return None
    s = re.sub(r"[\s\u00a0]", "", s).replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extract_project_title(text: str) -> str | None:
    # "PROJET N°XX : TITLE" or "PROJET N° T-002 : TITLE" / "PROJET N°A-003 : TITLE" / "PROJET N° NT-001 : TITLE"
    m = re.search(
        r"PROJET\s+N[°\s]*[A-Za-z\-\s]*\d+\s*[:\-]?\s*(.+?)(?=FILIÈRE|FILIERE|Contact\s*:|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        t = m.group(1).strip()
        t = re.sub(r"\s+", " ", t).strip()
        t = re.sub(r"\s+Contact\s*:.*$", "", t, flags=re.IGNORECASE).strip()
        if t and len(t) > 2:
            return t
    # Fallback: "PROJET : TITLE" or "PROJET N° T-002 STATION THERMALE" (second page, no colon before title)
    m = re.search(
        r"PROJET\s*[:\-]\s*(.+?)(?=MARCHÉ|MARCHE|FONCIER|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        t = m.group(1).strip()
        t = re.sub(r"\s+", " ", t).strip()
        t = re.sub(r"\s+Contact\s*:.*$", "", t, flags=re.IGNORECASE).strip()
        if t and len(t) > 2:
            return t
    # Second fallback: "PROJET N° X-XXX TITLE" on second page (e.g. "PROJET N° T -002 STATION THERMALE À MOULAY YACOUB")
    m = re.search(
        r"PROJET\s+N[°\s]*[A-Za-z\-\s]*\d+\s+([A-Za-zÀ-ÿ\s\-]+?)(?=MARCHÉ|MARCHE|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        t = m.group(1).strip()
        t = re.sub(r"\s+", " ", t).strip()
        if t and len(t) > 2:
            return t
    return None


def extract_sector(text: str) -> str | None:
    s = _first_match(text, r"FILIÈRE\s*[:\-]\s*([^\n]+)")
    if s:
        # Stop at Contact / Email / Tél (layout artifact)
        for sep in ["Contact", "Email", "Tél"]:
            if sep in s:
                s = s.split(sep)[0].strip()
        return s.strip() or None
    return None


def extract_sub_sector(text: str) -> str | None:
    # Full sub-sector: capture until DESCRIPTION or INDICATEURS (so "L'AIL" on next line is included)
    m = re.search(
        r"SOUS-FILIÈRE\s*[:\-]\s*(.+?)(?=DESCRIPTION|INDICATEURS|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        block = m.group(1).strip()
        # Remove only lines that are Contact/Email/Tél (keep lines like "L'AIL")
        lines = block.split("\n")
        kept = [
            line.strip()
            for line in lines
            if line.strip()
            and not re.match(r"^(Contact|Email|Tél)\s*:", line.strip(), re.IGNORECASE)
        ]
        s = " ".join(kept)
        # Remove " Email : address@domain" and " Tél/Tél: +212..." (any format)
        s = re.sub(r"\s+Email\s*:\s*\S+@\S+", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+T[ée]l\s*:?\s*\+?[\d\s\-]+", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+Contact\s*:.*", "", s, flags=re.IGNORECASE)
        # Remove trailing "PRÉREQUIS DU PROJET" or similar
        s = re.sub(r"\s*PR[ÉE]REQUIS\s+DU\s+PROJET\s*$", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+", " ", s).strip()
        return s or None
    return None


def extract_estimated_investment_mad(text: str) -> float | None:
    # "Investissement potentiel(hors foncier) : 11 MDH" or "Investissementpotentiel" (no space, some PDFs)
    m = re.search(
        r"Investissement\s*potentiel\s*\([^)]*\)\s*[:\-]\s*([\d\s,\.]+)\s*MDH",
        text,
        re.IGNORECASE,
    )
    if m:
        s = re.sub(r"[\s\u00a0]", "", m.group(1)).replace(",", ".")
        try:
            val = float(s)
            return val * 1_000_000 if val < 1000 else val  # MDH -> MAD
        except ValueError:
            pass
    # "X DH" (dirhams, value in MAD: 900 000 DH = 900000 MAD, 500 000 DH = 500000 MAD)
    m = re.search(
        r"Investissement\s*potentiel\s*\([^)]*\)\s*[:\-]\s*([\d\s,\.]+)\s*DH\b",
        text,
        re.IGNORECASE,
    )
    if m:
        s = re.sub(r"[\s\u00a0]", "", m.group(1)).replace(",", ".")
        try:
            val = float(s)
            return val  # already in MAD
        except ValueError:
            pass
    return None


def extract_project_description_from_layout(pdf_path: Path, start_page: int) -> str | None:
    """
    Extract description from the LEFT column only (layout-based, no keywords).
    Crop page to left half → get only description column; same logic for all projects.
    """
    left_text = extract_text_from_left_column(pdf_path, start_page, num_pages=2)
    if not left_text or not left_text.strip():
        return None
    # In left column, take block between DESCRIPTION and INDICATEURS (or end).
    # Stop at line-start INDICATEURS (section title) so "indicateurs" mid-sentence does not cut.
    m = re.search(
        r"DESCRIPTION\s*(.+?)(?=\n\s*INDICATEURS\b|$)",
        left_text,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    block = m.group(1).strip()
    # Strip header line "PRÉREQUIS DU PROJET" when on same line as DESCRIPTION
    block = re.sub(r"^PR.REQUIS\s+DU\s+PROJET\s*", "", block, flags=re.IGNORECASE).strip()
    # Strip trailing "Contact : ..." (layout artifact in left column)
    block = re.sub(r"\s+Contact\s*:.*$", "", block, flags=re.IGNORECASE).strip()
    block = re.sub(r"\s+", " ", block).strip()
    block = _fix_missing_spaces_description(block)
    if block and len(block) >= 20:
        return block
    return None


def extract_project_description(text: str, pdf_path: Path | None = None, start_page: int | None = None) -> str | None:
    """
    Prefer layout-based extraction (left column only) when pdf_path and start_page are given.
    Fallback: take DESCRIPTION...INDICATEURS from full text (may contain mixed columns).
    """
    if pdf_path is not None and start_page is not None:
        desc = extract_project_description_from_layout(pdf_path, start_page)
        if desc:
            return desc
    # Fallback: full text block (mixed columns). Stop at line-start INDICATEURS only.
    m = re.search(
        r"DESCRIPTION\s*(.+?)(?=\n\s*INDICATEURS\b|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        block = re.sub(r"^PR.REQUIS\s+DU\s+PROJET\s*", "", m.group(1).strip(), flags=re.IGNORECASE)
        block = re.sub(r"\s+Contact\s*:.*$", "", block, flags=re.IGNORECASE).strip()
        block = re.sub(r"\s+", " ", block).strip()
        block = _fix_missing_spaces_description(block)
        if block and len(block) >= 20:
            return block
    return None


def extract_payback_period_years(text: str) -> float | None:
    # "Retour sur investissement(ROI) : 5 à 6 ans" or "6 ans" or "5 - 6 ans"
    m = re.search(
        r"Retour\s+sur\s+investissement\s*\([^)]*\)\s*[:\-]\s*(\d+)\s*(?:à|-)\s*(\d+)\s*ans",
        text,
        re.IGNORECASE,
    )
    if m:
        try:
            a, b = int(m.group(1)), int(m.group(2))
            return (a + b) / 2.0
        except ValueError:
            pass
    m = re.search(r"Retour\s+sur\s+investissement\s*\([^)]*\)\s*[:\-]\s*(\d+)\s*ans", text, re.IGNORECASE)
    if m:
        return _first_match_float(text, r"Retour\s+sur\s+investissement\s*\([^)]*\)\s*[:\-]\s*(\d+)\s*ans", 1)
    return None


def extract_required_land_area_m2(text: str) -> float | None:
    # m2
    m = re.search(r"Superficie\s+souhait[ée]e\s+du\s+terrain\s*[:\-]\s*([\d\s,\.]+)\s*m2", text, re.IGNORECASE)
    if m:
        s = re.sub(r"[\s\u00a0]", "", m.group(1)).replace(",", ".")
        try:
            return float(s)
        except ValueError:
            pass
    m = re.search(r"terrain\s*[:\-]?\s*([\d\s,\.]+)\s*m2", text, re.IGNORECASE)
    if m:
        s = re.sub(r"\s", "", m.group(1)).replace(",", ".")
        try:
            return float(s)
        except ValueError:
            pass
    # Ha (hectares): 1 Ha = 10 000 m2 (Agriculture PDFs)
    m = re.search(r"Superficie\s+souhait[ée]e\s+du\s+terrain\s*[:\-]\s*([\d\s,\.]+)\s*Ha\b", text, re.IGNORECASE)
    if m:
        s = re.sub(r"[\s\u00a0]", "", m.group(1)).replace(",", ".")
        try:
            return float(s) * 10_000
        except ValueError:
            pass
    m = re.search(r"terrain\s*[:\-]?\s*([\d\s,\.]+)\s*Ha\b", text, re.IGNORECASE)
    if m:
        s = re.sub(r"\s", "", m.group(1)).replace(",", ".")
        try:
            return float(s) * 10_000
        except ValueError:
            pass
    # "Superficie souhaitée 200 à 300 m2" or "200 à 300m2" (take first number)
    m = re.search(r"Superficie\s+souhait[ée]e\s*([\d\s,\.]+)(?:\s*[à\-]\s*[\d\s,\.]+)?\s*m2", text, re.IGNORECASE)
    if m:
        s = re.sub(r"[\s\u00a0]", "", m.group(1)).replace(",", ".")
        try:
            return float(s)
        except ValueError:
            pass
    return None


def extract_required_building_area_m2(text: str) -> float | None:
    m = re.search(r"constructions?\s+de\s+([\d\s]+)\s*m2", text, re.IGNORECASE)
    if m:
        s = re.sub(r"\s", "", m.group(1))
        try:
            return float(s)
        except ValueError:
            pass
    return None


def extract_region(text: str) -> str | None:
    # Document is from fesmeknesinvest.ma; "Fès" appears in text
    if re.search(r"Fès|Fes-Meknès|Fès-Meknès|fesmeknes", text, re.IGNORECASE):
        return "Fès-Meknès"
    m = re.search(r"R[ée]gion\s+[:\-]?\s*([A-Za-zÀ-ÿ\s\-]+?)(?:\n|$)", text)
    if m:
        return m.group(1).strip() or None
    return None


def extract_province(text: str) -> str | None:
    m = re.search(r"province\s+[:\-]?\s*([A-Za-zÀ-ÿ\s\-]+?)(?:\n|\.|,|$)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip() or None
    # Province names (specific first: El Hajeb, Taounate before Fès/Meknès from region name)
    for name in ["El Hajeb", "Taounate", "Sefrou", "Ifrane", "Moulay Yacoub", "Meknès", "Fès"]:
        if re.search(rf"\b{re.escape(name)}\b", text, re.IGNORECASE):
            return name
    # El Hajeb sometimes without space (ElHajeb) or after apostrophe (d'El Hajeb)
    if re.search(r"El\s*Hajeb|Hajeb\s*principale", text, re.IGNORECASE):
        return "El Hajeb"
    return None


def extract_industrial_zone(text: str) -> str | None:
    zones = []
    for z in ["AGROPOLIS", "ZI AIN BIDA", "ZI AIN CHEGAG"]:
        if z in text.upper():
            zones.append(z)
    return "; ".join(zones) if zones else None


def extract_publication_date(
    pages_text: list[tuple[int, str]], pdf_path: Path
) -> str | None:
    # From cover (page 1)
    cover = next((t for p, t in pages_text if p == 1), "")
    m = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", cover)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    m = re.search(r"(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})", cover)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    # From filename: 8 digits ddmmyyyy (23052025 -> 2025-05-23) or 6 digits ddmmyy (230225 -> 2023-02-25)
    name = pdf_path.name
    dm8 = re.search(r"(\d{2})(\d{2})(\d{4})\D", name)  # DD MM YYYY
    if dm8:
        return f"{dm8.group(3)}-{dm8.group(2)}-{dm8.group(1)}"
    dm6 = re.search(r"(\d{2})(\d{2})(\d{2})\D", name)  # DD MM YY
    if dm6:
        return f"20{dm6.group(3)}-{dm6.group(2)}-{dm6.group(1)}"
    if "23 05 2025" in cover:
        return "2025-05-23"
    return None


# ---------------------------------------------------------------------------
# Calculated fields
# ---------------------------------------------------------------------------
def investment_range_label(estimated_mad: float | None) -> str | None:
    if estimated_mad is None:
        return None
    if estimated_mad < INV_LOW_MAX:
        return "Low"
    if estimated_mad <= INV_MEDIUM_MAX:
        return "Medium"
    return "High"


def roi_estimated_only(payback_years: float | None) -> float | None:
    """Compute roi_estimated: 100/payback if present, else 100/6."""
    if payback_years is not None and payback_years > 0:
        return 100.0 / payback_years
    return 100.0 / ROI_ASSUMPTION_YEARS


def min_investment_mad(estimated_mad: float | None) -> float | None:
    if estimated_mad is None:
        return None
    return round(estimated_mad * 0.8, 2)


# ---------------------------------------------------------------------------
# Build one project record
# ---------------------------------------------------------------------------
def build_record(
    project_index: int,
    start_page: int,
    text: str,
    pages_text: list[tuple[int, str]],
    pdf_path: Path,
    publication_date: str | None,
) -> dict:
    title = extract_project_title(text)
    sector = extract_sector(text)
    if not sector:
        sector = _sector_from_filename(pdf_path)
    region = extract_region(text)
    if not region and "fesmeknes" in str(pdf_path).lower():
        region = "Fès-Meknès"

    est_mad = extract_estimated_investment_mad(text)
    payback = extract_payback_period_years(text)
    roi_est = roi_estimated_only(payback)

    # project_id: integer, auto-incremented
    project_id = project_index

    # project_reference: Region-Title-Index, uppercase, hyphens, no accents, unique
    _region_norm = normalize_for_reference(region or "UNKNOWN")
    _title_norm = normalize_for_reference(title or "PROJET")
    project_reference = f"{_region_norm}-{_title_norm}-{project_index}"

    # project_bank_category: high-level from FILIÈRE or filename, uppercase
    project_bank_category = (sector or _sector_from_filename(pdf_path)).upper().strip()

    return {
        "project_id": project_id,
        "project_reference": project_reference,
        "project_title": title,
        "project_description": extract_project_description(text, pdf_path, start_page),
        "sector": sector,
        "sub_sector": extract_sub_sector(text),
        "project_bank_category": project_bank_category,
        "is_project_bank": True,
        "region": region,
        "province": extract_province(text),
        "industrial_zone": extract_industrial_zone(text),
        "estimated_investment_mad": round(est_mad, 2) if est_mad is not None else None,
        "min_investment_mad": min_investment_mad(est_mad),
        "investment_range": investment_range_label(est_mad),
        "payback_period_years": round(payback, 2) if payback is not None else None,
        "roi_estimated": round(roi_est, 2) if roi_est is not None else None,
        "required_land_area_m2": extract_required_land_area_m2(text),
        "required_building_area_m2": extract_required_building_area_m2(text),
        "has_pdf": True,
        "pdf_url": str(pdf_path.resolve()),
        "pdf_page_number": start_page,
        "publication_date": publication_date,
        "last_update": publication_date,
        "language": LANGUAGE,
        "currency": CURRENCY,
        "source_type": f"CRI {region}" if region else SOURCE_TYPE,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    rows = []
    global_index = 1

    for filename in PDF_FILENAMES:
        pdf_path = SCRIPT_DIR / filename
        if not pdf_path.exists():
            print(f"Skip (not found): {filename}")
            continue

        pages_text = extract_text_per_page(pdf_path)
        project_pages = find_project_start_pages(pages_text)
        project_pages = [p for p in project_pages if p > 1]

        publication_date = extract_publication_date(pages_text, pdf_path)

        for start_page in project_pages:
            text = get_project_text_block(pages_text, start_page)
            record = build_record(
                global_index, start_page, text, pages_text, pdf_path, publication_date
            )
            rows.append(record)
            global_index += 1

        print(f"  {filename}: {len(project_pages)} projects")

    df = pd.DataFrame(rows)

    column_order = [
        "project_id",
        "project_reference",
        "project_title",
        "project_description",
        "sector",
        "sub_sector",
        "project_bank_category",
        "is_project_bank",
        "region",
        "province",
        "industrial_zone",
        "estimated_investment_mad",
        "min_investment_mad",
        "investment_range",
        "payback_period_years",
        "roi_estimated",
        "required_land_area_m2",
        "required_building_area_m2",
        "has_pdf",
        "pdf_url",
        "pdf_page_number",
        "publication_date",
        "last_update",
        "language",
        "currency",
        "source_type",
    ]
    df = df[[c for c in column_order if c in df.columns]]

    df.to_csv(
        OUTPUT_CSV,
        index=False,
        encoding="utf-8-sig",
        sep=",",
        na_rep="NULL",
        quoting=csv.QUOTE_NONNUMERIC,
    )
    print(f"Extracted {len(df)} projects total to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
