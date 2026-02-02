import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import re
import unicodedata
from pathlib import Path
import pdfplumber
import pandas as pd
import glob
from urllib.parse import urlparse
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- CONFIGURATION ---
URL_PROJETS = "https://coeurdumaroc.ma/fr/projects"
SCRIPT_DIR = Path(__file__).resolve().parent
DOSSIER_CIBLE = SCRIPT_DIR / "banque_projets_bk"
OUTPUT_CSV = SCRIPT_DIR / "output_projects.csv"
CURRENCY = "MAD"
LANGUAGE = "FR"
SOURCE_TYPE = "CRI Béni Mellal-Khénifra"

# Investment range thresholds (MAD)
INV_LOW_MAX = 5_000_000
INV_MEDIUM_MAX = 20_000_000
ROI_ASSUMPTION_YEARS = 6
MAX_WORKERS = max(1, min((os.cpu_count() or 2), 6))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _remove_accents(s: str) -> str:
    """Remove accents deterministically (NFD and strip combining marks)."""
    if not s:
        return ""
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")

def normalize_for_reference(s: str) -> str:
    """Uppercase, replace spaces with hyphens, remove accents. For project_reference."""
    if not s or not s.strip():
        return ""
    s = _remove_accents(s.strip())
    s = re.sub(r"[^\w\s\-]", "", s)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s.upper()

def clean_text(text: str) -> str:
    """Clean extra spaces and newlines."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def _strip_label_prefix(s: str) -> str:
    s = clean_text(s)
    s = re.sub(r"^(?:Secteur\s*économique|Filières\s*de\s*production)\s*:\s*", "", s, flags=re.IGNORECASE)
    return s.strip()

def _dedupe_repeated_title(title: str) -> str:
    """
    Some PDFs repeat the title twice in extracted text. If the title is "X X" (same block repeated),
    keep only one.
    """
    t = clean_text(title)
    if not t:
        return t
    words = t.split()
    if len(words) >= 6 and len(words) % 2 == 0:
        half = len(words) // 2
        if words[:half] == words[half:]:
            return " ".join(words[:half])
    return t

def extract_industrial_zone(text: str) -> str | None:
    """
    Extract industrial zones / location anchors without swallowing adjacent narrative.
    Returns a '; '-joined string or None.
    """
    if not text:
        return None

    t = text.replace("", " ").replace("\u2022", " ")
    stop = r"(?:Future|ZI|ZAE|Sup|Lieu|PROGRAMME|CAPACIT[ÉE]|EMPLOIS|TRI|PBP|CA|Cha[iî]nes?|Mat[ée]riel|Equipements?|Emballages?|BESOINS|Web|Contact)"

    patterns = [
        rf"(?:Agro-?p[oô]le|Agropole)\s+[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]{{0,40}}?(?=\b{stop}\b|[;\n]|$)",
        rf"Future\s+ZI\s+[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]{{0,40}}?(?=\b{stop}\b|[;\n]|$)",
        rf"\bZAE\s+[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]{{0,40}}?(?=\b{stop}\b|[;\n]|$)",
        rf"\bZI\s+[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]{{0,40}}?(?=\b{stop}\b|[;\n]|$)",
    ]

    def _trim_zone(z: str) -> str:
        z = clean_text(z).strip(" -–—.,;:/\\")
        if not z:
            return z

        # If a dash is used as part of the zone name, keep it (common in "Future ZI ... - ...")
        if re.search(r"\s[-–—]\s", z):
            # Still trim any trailing noise after a repeated zone marker
            z = re.sub(rf"\b({stop})\b.*$", "", z, flags=re.IGNORECASE).strip(" -–—.,;:/\\")
            return z

        tokens = z.split()
        low = [t.lower() for t in tokens]

        # Future ZI <name>
        if len(tokens) >= 3 and low[0] == "future" and low[1] == "zi":
            return " ".join(tokens[:3])
        # ZI <name>
        if len(tokens) >= 2 and low[0] == "zi":
            return " ".join(tokens[:3]) if len(tokens) >= 3 else z
        # ZAE <name>
        if len(tokens) >= 2 and low[0] == "zae":
            return " ".join(tokens[:3]) if len(tokens) >= 3 else z
        # Agro-pôle/Agropole <name>
        if low[0].startswith("agro") or low[0].startswith("agropole"):
            return " ".join(tokens[:3]) if len(tokens) >= 3 else z

        return z

    zones: list[str] = []
    for pat in patterns:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            z = _trim_zone(m.group(0))
            if z and z not in zones:
                zones.append(z)

    return "; ".join(zones) if zones else None


def extract_province(text: str) -> str | None:
    """
    Extract province deterministically:
    - Prefer known provinces if present anywhere.
    - Else try Lieu: ... and keep a short clean value.
    """
    if not text:
        return None

    known_provinces = ["Béni Mellal", "Azilal", "Fquih Ben Salah", "Khénifra", "Khouribga"]
    for p in known_provinces:
        if re.search(rf"\b{re.escape(p)}\b", text, re.IGNORECASE):
            return p

    m_loc = re.search(
        r"Lieu\s*:\s*(.+?)(?=\b(?:Future|ZAE|ZI|Sup|PROGRAMME|CAPACIT[ÉE]|EMPLOIS|TRI|PBP|INVESTISSEMENT|BESOINS|Web|Contact)\b|\n|$)",
        text,
        re.IGNORECASE,
    )
    if not m_loc:
        return None

    loc_text = clean_text(m_loc.group(1))
    # Keep first chunk only (before dash/semicolon)
    loc_text = re.split(r"\s*[;–—-]\s*", loc_text)[0].strip()

    # If too noisy/long, discard
    if len(loc_text) > 40 or re.search(r"[\d%]", loc_text):
        return None

    return loc_text or None

def get_next_project_id(csv_path: Path) -> int:
    """Read CSV to find the max project_id and return max + 1."""
    if not csv_path.exists():
        return 1
    try:
        df = pd.read_csv(csv_path)
        if "project_id" in df.columns and not df.empty:
            return df["project_id"].max() + 1
    except Exception:
        pass
    return 1

# ---------------------------------------------------------------------------
# Extraction Logic
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Extraction Logic
# ---------------------------------------------------------------------------
def extract_text_with_layout(pdf_path: Path) -> dict:
    """
    Extract text using layout analysis to separate Left and Right columns.
    Returns a dict with 'full_text', 'left_text', 'right_text'.
    """
    result = {"full_text": "", "left_text": "", "right_text": ""}
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return result

            # Some BK PDFs have key numeric blocks on page 2.
            max_pages = min(2, len(pdf.pages))
            full_parts: list[str] = []
            left_parts: list[str] = []
            right_parts: list[str] = []

            for page in pdf.pages[:max_pages]:
                width = page.width
                mid_point = width / 2

                full_parts.append(page.extract_text() or "")

                try:
                    left_crop = page.crop((0, 0, mid_point, page.height))
                    left_parts.append(left_crop.extract_text() or "")
                except Exception:
                    pass

                try:
                    right_crop = page.crop((mid_point, 0, width, page.height))
                    right_parts.append(right_crop.extract_text() or "")
                except Exception:
                    pass

            result["full_text"] = "\n".join([p for p in full_parts if p]).strip()
            result["left_text"] = "\n".join([p for p in left_parts if p]).strip()
            result["right_text"] = "\n".join([p for p in right_parts if p]).strip()
                
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        
    return result

def extract_project_title(text: str, filename: str) -> str:
    # "Projet N° : PR100" ... "CENTRE D’APPEL"
    # Title seems to be on the second line or after Project N°
    
    # Strategy 1: Look for line after "Projet N°"
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if "Projet N°" in line or "Projet N" in line:
            # The title is likely on the next line if it's uppercase and meaningful
            if i + 1 < len(lines):
                candidate = lines[i+1].strip()
                if len(candidate) > 3 and not "Secteur" in candidate:
                    return _dedupe_repeated_title(candidate)
    
    # Strategy 2: Regex fallback
    m_title = re.search(r"Projet\s*N°\s*:\s*(?:PR\d+)?\s*\n(.+?)(?=\n|Secteur)", text, re.IGNORECASE | re.DOTALL)
    if m_title:
        return _dedupe_repeated_title(m_title.group(1))

    # Fallback to filename
    return filename.replace(".pdf", "").replace("-", " ")

def extract_sector_from_layout(left_text: str, full_text: str) -> str:
    # Sector is usually on the LEFT column under "Secteur économique :"
    # It might be separate from "Filières" now.
    
    # Try Left Text first
    m_sector = re.search(r"Secteur\s*économique\s*:\s*(.+?)(?=Filières|ACTIVITÉ|AnalySe|$)", left_text, re.IGNORECASE | re.DOTALL)
    if m_sector:
        return _strip_label_prefix(m_sector.group(1))
        
    # Fallback to Full Text (risk of merging)
    m_sector_full = re.search(r"Secteur\s*économique\s*:\s*(.+?)(?=Filières|ACTIVITÉ|AnalySe)", full_text, re.IGNORECASE | re.DOTALL)
    if m_sector_full:
        return _strip_label_prefix(m_sector_full.group(1))
        
    return "CRI"

def extract_sub_sector_from_layout(right_text: str, full_text: str) -> str | None:
    # Sub-sector "Filières de production :" is usually on the RIGHT or next to Sector
    
    # Try Right Text first
    m_sub = re.search(r"Filières\s*de\s*production\s*:\s*(.+?)(?=Secteur|ACTIVITÉ|AnalySe|$)", right_text, re.IGNORECASE | re.DOTALL)
    if m_sub:
        return _strip_label_prefix(m_sub.group(1))
        
    # Fallback Full Text
    m_sub_full = re.search(r"Filières\s*de\s*production\s*:\s*(.+?)(?=Secteur|ACTIVITÉ|AnalySe)", full_text, re.IGNORECASE | re.DOTALL)
    if m_sub_full:
        return _strip_label_prefix(m_sub_full.group(1))
        
    return None

def extract_description_from_layout(left_text: str, full_text: str) -> str | None:
    """
    Prefer short description like in `cri_fes_mekness.py`:
    - If "DESCRIPTION DU PROJET" exists, stop at next section heading.
    - Otherwise, take the first narrative paragraph and stop before headings like Code/PRODUIT/SUPERFICIE/PROGRAMME...
    """
    stop = r"(?:\n\s*(?:Code\s*(?:HS|SH)|PRODUIT\s+PRINCIPAL|D[ÉE]BOUCH[ÉE]S|SUPERFICIE|PROGRAMME|CAPACIT[ÉE]|EMPLOIS|TRI|PBP|INVESTISSEMENT|BESOINS|Sup\s*:|Lieu\s*:|Web\s*:|Contact\s*:))"

    key_pattern = rf"(?:ACTIVIT[ÉE]\s*-\s*)?DESCRIPTION\s*DU\s*PROJET\s*(.+?)(?={stop}|$)"
    m_desc = re.search(key_pattern, left_text, re.IGNORECASE | re.DOTALL)
    if m_desc:
        return clean_text(m_desc.group(1))

    m_desc_full = re.search(key_pattern, full_text, re.IGNORECASE | re.DOTALL)
    if m_desc_full:
        return clean_text(m_desc_full.group(1))

    # Fallback: first narrative paragraph
    narrative = rf"(?:^|\n)\s*(?:Un|Une|Le|La|L['’])\s+(.+?)(?={stop}|$)"
    m_narr = re.search(narrative, left_text, re.IGNORECASE | re.DOTALL)
    if m_narr:
        return clean_text(m_narr.group(0))

    m_narr_full = re.search(narrative, full_text, re.IGNORECASE | re.DOTALL)
    if m_narr_full:
        return clean_text(m_narr_full.group(0))

    return None

def extract_numeric_fields(text: str) -> dict:
    data = {}

    def _norm(s: str) -> str:
        """
        Normalize PDF extracted text for robust numeric regex:
        - bullets/NBSP/replacement char
        - dash variants
        - collapse spaced-letter tokens back into keywords (MDH/MDHS/PBP/TRI/CA)
        """
        if not s:
            return ""
        s = s.replace("\u00a0", " ").replace("", " ").replace("\u2022", " ")
        s = s.replace("\ufffd", "-")
        s = s.replace("–", "-").replace("—", "-").replace("−", "-")
        s = re.sub(r"\bM\s*D\s*H\s*S?\b", lambda m: re.sub(r"\s+", "", m.group(0)), s, flags=re.IGNORECASE)
        s = re.sub(r"\bP\s*B\s*P\b", "PBP", s, flags=re.IGNORECASE)
        s = re.sub(r"\bT\s*R\s*I\b", "TRI", s, flags=re.IGNORECASE)
        s = re.sub(r"\bC\s*A\b", "CA", s, flags=re.IGNORECASE)
        # sometimes "D H S" appears
        s = re.sub(r"\bD\s*H\s*S\b", "DHS", s, flags=re.IGNORECASE)
        s = re.sub(r"\bD\s*H\b", "DH", s, flags=re.IGNORECASE)
        # sometimes "K D H S" appears (thousand dirhams)
        s = re.sub(r"\bK\s*D\s*H\s*S\b", "KDHS", s, flags=re.IGNORECASE)
        # OCR-like confusion: PB0P
        s = re.sub(r"\bP\s*B\s*0\s*P\b", "PBP", s, flags=re.IGNORECASE)
        s = re.sub(r"\bPB0P\b", "PBP", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+", " ", s)
        return s

    t = _norm(text)
    
    # Investment (MDH/MDHS)
    # Pattern: 15 - 35 MDHS or 20 MDHS
    # Some PDFs add "Mn/Mns" (millions) between value and unit: "50 - 75 Mn MDHS"
    m_inv = re.search(
        r"(\d+(?:[.,]\d+)?)\s*(?:-|à)\s*(\d+(?:[.,]\d+)?)\s*(?:Mns?|Mn)?\s*MDH(?:S)?\b",
        t,
        re.IGNORECASE,
    )
    est_mad = None

    # Investment in KDHS (thousand dirhams)
    m_kdhs = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:-|à)\s*(\d+(?:[.,]\d+)?)\s*KDHS?\b", t, re.IGNORECASE)
    if m_kdhs:
        try:
            a = float(m_kdhs.group(1).replace(",", "."))
            b = float(m_kdhs.group(2).replace(",", "."))
            est_mad = ((a + b) / 2) * 1000.0
        except Exception:
            pass
    else:
        m_kdhs_single = re.search(r"\b(\d+(?:[.,]\d+)?)\s*KDHS?\b", t, re.IGNORECASE)
        if m_kdhs_single:
            try:
                est_mad = float(m_kdhs_single.group(1).replace(",", ".")) * 1000.0
            except Exception:
                pass

    # Investment in MDH/MDHS
    if m_inv:
        try:
            low = float(m_inv.group(1).replace(",", "."))
            high = float(m_inv.group(2).replace(",", "."))
            est_mad = ((low + high) / 2) * 1_000_000
        except: pass
    else:
        m_inv_single = re.search(
            r"(?:INVESTISSEMENT|D['’]INVESTISSEMENT)\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*(?:Mns?|Mn)?\s*MDH(?:S)?\b",
            t,
            re.IGNORECASE,
        )
        if not m_inv_single:
            m_inv_single = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:Mns?|Mn)?\s*MDH(?:S)?\b", t, re.IGNORECASE)
        if m_inv_single and est_mad is None:
            try:
                val = float(m_inv_single.group(1).replace(",", "."))
                est_mad = val * 1_000_000
            except: pass

    # Investment in DH/DHS (some service projects)
    if est_mad is None:
        # Typical line: "- 150 000 DHS - CA: 200 000-250 000 DHS"
        m_dh = re.search(r"-\s*([\d\s.,]+)\s*DHS?\s*-\s*CA\b", t, re.IGNORECASE)
        if not m_dh:
            m_dh = re.search(r"(?:INVESTISSEMENT|D['’]INVESTISSEMENT)\b.*?([\d\s.,]+)\s*DHS?\b", t, re.IGNORECASE)
        if not m_dh:
            # Fallback: first standalone DHS amount
            m_dh = re.search(r"\b([\d\s.,]{3,})\s*DHS?\b", t, re.IGNORECASE)
        if m_dh:
            try:
                s = m_dh.group(1)
                s = s.replace(" ", "").replace("\u00a0", "").replace(",", ".")
                est_mad = float(s)
            except Exception:
                pass
    data["estimated_investment_mad"] = est_mad

    # Surface (m2)
    m_surf_range = re.search(
        r"Sup\s*[:\-]?\s*(\d+(?:[\s.,]\d+)?)(?:\s*m\s*[²2])?\s*(?:-|à)\s*(\d+(?:[\s.,]\d+)?)(?:\s*m\s*[²2])?\b",
        t,
        re.IGNORECASE,
    )
    if m_surf_range:
        try:
            s1 = float(m_surf_range.group(1).replace(" ", "").replace(",", "."))
            s2 = float(m_surf_range.group(2).replace(" ", "").replace(",", "."))
            data["required_land_area_m2"] = (s1 + s2) / 2
        except Exception:
            pass
    else:
        m_surf_single = re.search(
            r"Sup\s*[:\-]?\s*(\d+(?:[\s.,]\d+)?)\s*m\s*[²2]\b",
            t,
            re.IGNORECASE,
        )
        if m_surf_single:
            try:
                data["required_land_area_m2"] = float(m_surf_single.group(1).replace(" ", "").replace(",", "."))
            except Exception:
                pass

    # Surface in Ha (hectares): 1 Ha = 10 000 m2
    if data.get("required_land_area_m2") is None:
        m_ha = re.search(
            r"Sup\s*[:\-]?\s*(\d+(?:[.,]\d+)?)(?:\s*(?:-|à)\s*(\d+(?:[.,]\d+)?))?\s*Ha\b",
            t,
            re.IGNORECASE,
        )
        if m_ha:
            try:
                h1 = float(m_ha.group(1).replace(",", "."))
                h2 = m_ha.group(2)
                if h2:
                    h2 = float(h2.replace(",", "."))
                    data["required_land_area_m2"] = ((h1 + h2) / 2) * 10_000
                else:
                    data["required_land_area_m2"] = h1 * 10_000
            except Exception:
                pass

    # ROI / TRI
    # ROI / TRI (often appears without ":" in BK PDFs)
    m_roi = re.search(r"TRI\s*(?:moyen)?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*(?:-|à)\s*(\d+(?:[.,]\d+)?)\s*%?", t, re.IGNORECASE)
    if not m_roi:
        m_roi = re.search(r"TRI\s*(?:moyen)?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*%?", t, re.IGNORECASE)
    if m_roi:
        try:
            r1 = float(m_roi.group(1).replace(",", "."))
            r2 = m_roi.group(2) if m_roi.lastindex and m_roi.lastindex >= 2 else None
            if r2:
                r2 = float(r2.replace(",", "."))
                data["roi_estimated"] = (r1 + r2) / 2
            else:
                data["roi_estimated"] = r1
        except Exception:
            pass

    # Payback (PBP)
    m_pbp = re.search(r"PBP\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*(?:-|à)?\s*(\d+(?:[.,]\d+)?)?\s*ans", t, re.IGNORECASE)
    if m_pbp:
        try:
            p1 = float(m_pbp.group(1).replace(",", "."))
            p2 = m_pbp.group(2)
            if p2:
                p2 = float(p2.replace(",", "."))
                data["payback_period_years"] = (p1 + p2) / 2
            else:
                data["payback_period_years"] = p1
        except: pass

    # Some PDFs use "ROI : 4-5ans" as payback period (not ROI percentage)
    if data.get("payback_period_years") is None:
        m_roi_years = re.search(
            r"\bROI\b\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*(?:-|à)?\s*(\d+(?:[.,]\d+)?)?\s*ans",
            t,
            re.IGNORECASE,
        )
        if m_roi_years:
            try:
                a = float(m_roi_years.group(1).replace(",", "."))
                b = m_roi_years.group(2)
                if b:
                    b = float(b.replace(",", "."))
                    data["payback_period_years"] = (a + b) / 2
                else:
                    data["payback_period_years"] = a
            except Exception:
                pass

    # Deterministic fills for recommendation downstream
    try:
        if data.get("roi_estimated") is None and data.get("payback_period_years") is not None:
            pb = float(data["payback_period_years"])
            if pb > 0:
                data["roi_estimated"] = round(100.0 / pb, 2)
    except Exception:
        pass

    try:
        if data.get("payback_period_years") is None and data.get("roi_estimated") is not None:
            tri = float(data["roi_estimated"])
            if 0 < tri <= 100:
                data["payback_period_years"] = round(100.0 / tri, 2)
    except Exception:
        pass
            
    # Province + zones
    data["province"] = extract_province(text)
    data["industrial_zone"] = extract_industrial_zone(text)
                
    return data

def extract_fields(filename: str, layout_data: dict) -> dict:
    data = {}
    full = layout_data["full_text"]
    left = layout_data["left_text"]
    right = layout_data["right_text"]
    
    data["project_title"] = extract_project_title(full, filename) # Title usually crosses columns or is top
    data["sector"] = extract_sector_from_layout(left, full)
    data["sub_sector"] = extract_sub_sector_from_layout(right, full)
    data["project_description"] = extract_description_from_layout(left, full)
    
    # Numeric fields can be anywhere, but usually bottom half. 
    # Searching full text is usually safe for distinct patterns like "MDH", "m²", "PBP :"
    numeric_data = extract_numeric_fields("\n".join([full or "", left or "", right or ""]))
    data.update(numeric_data)
    
    return data

def process_one_pdf(pdf_file_path: str, project_id: int) -> dict | None:
    """
    Top-level worker function for multiprocessing (must be picklable on Windows).
    """
    path_obj = Path(pdf_file_path)
    filename = path_obj.name

    layout_data = extract_text_with_layout(path_obj)
    if not layout_data["full_text"]:
        return None

    fields = extract_fields(filename, layout_data)

    title = fields.get("project_title", "Projet Sans Titre")
    region = "Béni Mellal-Khénifra"

    ref_title = normalize_for_reference(title)
    ref_region = normalize_for_reference(region)
    reference = f"{ref_region}-{ref_title}-{project_id}"
    if len(reference) > 100:
        reference = f"{ref_region}-{ref_title[:50]}-{project_id}"

    est_mad = fields.get("estimated_investment_mad")

    return {
        "project_id": project_id,
        "project_reference": reference,
        "project_title": title,
        "project_description": fields.get("project_description"),
        "sector": fields.get("sector", "CRI"),
        "sub_sector": fields.get("sub_sector"),
        "project_bank_category": fields.get("sector", "CRI"),
        "is_project_bank": True,
        "region": region,
        "province": fields.get("province"),
        "industrial_zone": fields.get("industrial_zone"),
        "estimated_investment_mad": est_mad,
        "min_investment_mad": min_investment_mad(est_mad),
        "investment_range": investment_range_label(est_mad),
        "payback_period_years": fields.get("payback_period_years"),
        "roi_estimated": fields.get("roi_estimated"),
        "required_land_area_m2": fields.get("required_land_area_m2"),
        "required_building_area_m2": None,
        "has_pdf": True,
        "pdf_url": str(path_obj.resolve()),
        "pdf_page_number": 1,
        "publication_date": None,
        "last_update": None,
        "language": LANGUAGE,
        "currency": CURRENCY,
        "source_type": SOURCE_TYPE,
    }

def investment_range_label(estimated_mad: float | None) -> str | None:
    if estimated_mad is None:
        return None
    if estimated_mad < INV_LOW_MAX:
        return "Low"
    if estimated_mad <= INV_MEDIUM_MAX:
        return "Medium"
    return "High"

def min_investment_mad(estimated_mad: float | None) -> float | None:
    if estimated_mad is None:
        return None
    return round(estimated_mad * 0.8, 2)

# ---------------------------------------------------------------------------
# Output CSV helpers (refresh only this source, keep others)
# ---------------------------------------------------------------------------
OUTPUT_COLUMNS = [
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


def _safe_read_existing_output(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    try:
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        print(f"Warning: cannot read {csv_path}: {e}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _max_project_id(df: pd.DataFrame) -> int:
    try:
        if "project_id" in df.columns and not df.empty:
            v = pd.to_numeric(df["project_id"], errors="coerce").max()
            return int(v) if pd.notna(v) else 0
    except Exception:
        pass
    return 0


def _remove_rows_for_this_source(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else OUTPUT_COLUMNS)

    if "source_type" in df.columns:
        kept = df[df["source_type"].astype(str) != SOURCE_TYPE].copy()
        return kept

    # Fallback (if schema changed): filter by region value
    if "region" in df.columns:
        kept = df[df["region"].astype(str) != "Béni Mellal-Khénifra"].copy()
        return kept

    return df.copy()


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df[columns]


def _write_output_csv(df: pd.DataFrame, csv_path: Path) -> None:
    """
    Overwrite output_projects.csv safely.
    If the file is open (PermissionError on Windows), write to output_projects_new.csv.
    """
    df_to_write = _ensure_columns(df, OUTPUT_COLUMNS)
    try:
        df_to_write.to_csv(
            csv_path,
            index=False,
            encoding="utf-8-sig",
            sep=",",
            quoting=csv.QUOTE_NONNUMERIC,
        )
        print(f"Wrote {len(df_to_write)} rows to {csv_path}")
    except OSError as e:
        # Windows: file might be open in Excel
        if getattr(e, "errno", None) == 13:
            fallback = csv_path.parent / "output_projects_new.csv"
            df_to_write.to_csv(
                fallback,
                index=False,
                encoding="utf-8-sig",
                sep=",",
                quoting=csv.QUOTE_NONNUMERIC,
            )
            print(f"Wrote {len(df_to_write)} rows to {fallback}")
            print("(output_projects.csv is open elsewhere — close it, then rename output_projects_new.csv if needed.)")
        else:
            raise

# ---------------------------------------------------------------------------
# Main Logic
# ---------------------------------------------------------------------------
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _is_same_site(url: str) -> bool:
    try:
        return urlparse(url).netloc.endswith("coeurdumaroc.ma")
    except Exception:
        return False


def _normalize_url(u: str) -> str:
    # Remove fragments, keep query (pagination sometimes uses query).
    try:
        p = urlparse(u)
        return p._replace(fragment="").geturl()
    except Exception:
        return u


def collect_pdf_urls_from_site(start_url: str) -> set[str]:
    """
    Crawl listing + detail pages (1-level deep) to collect all PDF URLs.
    Handles pagination heuristically (links containing 'page=' and staying under /fr/projects).
    """
    session = requests.Session()
    pdf_urls: set[str] = set()
    visited: set[str] = set()
    queue: list[str] = [start_url]

    # Safety bounds to avoid crawling too much
    MAX_PAGES = 200

    while queue and len(visited) < MAX_PAGES:
        url = _normalize_url(queue.pop(0))
        if url in visited:
            continue
        visited.add(url)

        try:
            resp = session.get(url, headers=DEFAULT_HEADERS, timeout=25)
            resp.raise_for_status()
        except Exception as e:
            print(f"Warning: failed to fetch {url}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            abs_url = _normalize_url(urljoin(url, href))
            if not _is_same_site(abs_url):
                continue

            if ".pdf" in abs_url.lower():
                pdf_urls.add(abs_url)
                continue

            # Stay within the projects area (listing, pagination, detail pages)
            if "/fr/projects" in abs_url:
                # Pagination links often contain page=...
                if "page=" in abs_url or abs_url.rstrip("/") == start_url.rstrip("/"):
                    queue.append(abs_url)
                # Detail pages usually under /fr/projects/<slug>
                elif abs_url.startswith(start_url.rstrip("/") + "/"):
                    queue.append(abs_url)

        # Also follow <link rel="next"> if present
        for link in soup.find_all("link", href=True):
            rel = " ".join(link.get("rel") or []).lower()
            if "next" in rel:
                next_url = _normalize_url(urljoin(url, link["href"]))
                if _is_same_site(next_url) and "/fr/projects" in next_url:
                    queue.append(next_url)

    return pdf_urls


def _url_to_filename(pdf_url: str) -> str:
    name = Path(urlparse(pdf_url).path).name
    if not name:
        name = "document.pdf"
    # Ensure .pdf extension
    if not name.lower().endswith(".pdf"):
        name = f"{name}.pdf"
    return name


def download_pdf(pdf_url: str, dest_dir: Path) -> Path | None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = _url_to_filename(pdf_url)
    dest_path = dest_dir / filename

    # If file exists and seems complete, try to skip when same size
    try:
        if dest_path.exists() and dest_path.stat().st_size > 0:
            try:
                h = requests.head(pdf_url, headers=DEFAULT_HEADERS, timeout=15, allow_redirects=True)
                remote_size = int(h.headers.get("Content-Length", "0") or "0")
                if remote_size > 0 and remote_size == dest_path.stat().st_size:
                    return dest_path
            except Exception:
                # If HEAD fails, we keep local file (avoid re-downloading everything).
                return dest_path
    except Exception:
        pass

    try:
        r = requests.get(pdf_url, stream=True, headers=DEFAULT_HEADERS, timeout=60)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return dest_path
    except Exception as e:
        print(f"Error downloading {pdf_url}: {e}")
        return None


def downloader_tous_les_projets():
    print(f"Analyse de la page des projets : {URL_PROJETS}")
    pdf_urls = collect_pdf_urls_from_site(URL_PROJETS)

    total = len(pdf_urls)
    if total == 0:
        print("Aucun document PDF n'a été trouvé.")
        return

    print(f"{total} documents trouvés. Début du téléchargement...\n")

    for i, url in enumerate(sorted(pdf_urls), start=1):
        filename = _url_to_filename(url)
        print(f"[{i}/{total}] Téléchargement : {filename}")
        download_pdf(url, DOSSIER_CIBLE)

    print(f"\nTerminé ! {total} fichiers sont disponibles dans le dossier '{DOSSIER_CIBLE}'.")

def process_pdfs():
    # Load existing CSV and compute a global next_id (keeps IDs unique across sources and runs)
    df_existing_full = _safe_read_existing_output(OUTPUT_CSV)
    global_max_id = _max_project_id(df_existing_full)
    next_id = global_max_id + 1

    # Remove only this source rows (keep data from other scripts like cri_fes_mekness.py)
    df_kept = _remove_rows_for_this_source(df_existing_full)
    print(f"Existing rows: {len(df_existing_full)} | kept (other sources): {len(df_kept)} | next_id: {next_id}")

    # 2. List PDFs
    pdf_files = glob.glob(str(DOSSIER_CIBLE / "*.pdf"))
    if not pdf_files:
        print(f"No PDFs found in {DOSSIER_CIBLE}")
        return

    pdf_files = sorted(pdf_files)

    extracted_rows: list[dict] = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = []
        for idx, pdf_file in enumerate(pdf_files):
            project_id = next_id + idx
            futures.append(ex.submit(process_one_pdf, pdf_file, project_id))

        done = 0
        total = len(futures)
        for fut in as_completed(futures):
            done += 1
            try:
                row = fut.result()
                if row:
                    extracted_rows.append(row)
            except Exception as e:
                print(f"  Error processing PDF (worker): {e}", flush=True)
            if done % 20 == 0 or done == total:
                print(f"Processed {done}/{total} PDFs...", flush=True)

    extracted_rows.sort(key=lambda r: r.get("project_id", 0))

    # Overwrite output_projects.csv: kept rows + new rows (this source refreshed)
    df_new = pd.DataFrame(extracted_rows)
    df_new = _ensure_columns(df_new, OUTPUT_COLUMNS)
    df_kept = _ensure_columns(df_kept, OUTPUT_COLUMNS)
    df_out = pd.concat([df_kept, df_new], ignore_index=True)
    _write_output_csv(df_out, OUTPUT_CSV)

if __name__ == "__main__":
    # If DOSSIER_CIBLE doesn't exist or is empty, maybe download first?
    # But user asked for extraction. Let's assume files are there.
    if not DOSSIER_CIBLE.exists() or not any(DOSSIER_CIBLE.iterdir()):
         downloader_tous_les_projets()
    
    process_pdfs()