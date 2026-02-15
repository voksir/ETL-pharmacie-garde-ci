from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dateutil.parser import parse as dtparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("annuaireci_scrape")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
URL = "https://annuaireci.com/pharmacies-de-garde/"

CACHE_DIR = Path(__file__).resolve().parent / ".cache"

WEEK_RE = re.compile(
    r"Semaine\s+du\s+(\d{2}/\d{2}/\d{4})\s+au\s+(\d{2}/\d{2}/\d{4})", re.I
)
PHONE_LIKE_RE = re.compile(r"\d{2}(?:[\s.-]?\d{2}){3,}")  # ex: 07 69 35 39 09

# Balises que l'on inspecte lors du parcours DOM
HEADING_TAGS = {"h2", "h3", "h4"}
CONTENT_TAGS = {"p", "div", "span", "li", "ul", "ol", "address", "strong", "em", "a"}

# Titres qui signalent la fin de la section pharmacies de garde
STOP_TITLES = frozenset(
    {
        "Urgence",
        "Horaires",
        "Localisation",
        "Rechercher une pharmacie",
        "Questions fréquentes",
    }
)

# Éléments structurels attendus sur la page (pour le monitoring)
EXPECTED_MARKERS = [
    ("week_range", WEEK_RE, "Période 'Semaine du … au …' introuvable"),
    (
        "anchor",
        re.compile(r"Liste\s+des\s+pharmacies\s+de\s+garde", re.I),
        "Ancre 'Liste des pharmacies de garde' introuvable",
    ),
]


# ---------------------------------------------------------------------------
# Exceptions dédiées au scraping
# ---------------------------------------------------------------------------
class ScrapingStructureError(Exception):
    """La structure HTML attendue n'est plus trouvée sur la page."""


# ---------------------------------------------------------------------------
# Monitoring / validation de la structure HTML
# ---------------------------------------------------------------------------
def validate_html_structure(html: str) -> List[str]:
    """Vérifie que les marqueurs structurels attendus sont présents.

    Retourne une liste de messages d'alerte (vide = tout est OK).
    """
    alerts: List[str] = []
    for marker_name, pattern, message in EXPECTED_MARKERS:
        if not pattern.search(html):
            alerts.append(f"[STRUCTURE] {marker_name}: {message}")
    # Vérifier la présence de balises h3/h4 (zones + pharmacies)
    soup = BeautifulSoup(html, "html.parser")
    h3_count = len(soup.find_all("h3"))
    h4_count = len(soup.find_all("h4"))
    if h3_count == 0:
        alerts.append("[STRUCTURE] Aucune balise <h3> trouvée (zones géographiques)")
    if h4_count == 0:
        alerts.append("[STRUCTURE] Aucune balise <h4> trouvée (noms de pharmacies)")
    return alerts


# ---------------------------------------------------------------------------
# HTTP : retry + cache
# ---------------------------------------------------------------------------
def _build_session() -> requests.Session:
    """Crée une session requests avec retry automatique (backoff exponentiel)."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,          # 0s, 1s, 2s entre les tentatives
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _cache_path(url: str) -> Path:
    """Retourne le chemin de cache pour une URL donnée (basé sur un hash)."""
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    today = date.today().isoformat()
    return CACHE_DIR / f"{today}_{url_hash}.html"


def fetch_html(url: str, *, use_cache: bool = False) -> str:
    """Récupère le HTML d'une URL avec retry automatique.

    Args:
        url: URL à récupérer.
        use_cache: Si True, utilise/crée un cache local (utile en dev/debug).
    """
    # ----- Cache lecture -----
    if use_cache:
        cached = _cache_path(url)
        if cached.exists():
            logger.info("Cache hit : %s", cached)
            return cached.read_text(encoding="utf-8")

    # ----- Requête HTTP avec retry -----
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.6",
    }
    session = _build_session()
    logger.info("GET %s (retry=3, backoff=1s)", url)
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    # Force UTF-8 (annuaireci est servi en UTF-8 ; évite le mojibake "茅" etc.)
    html = r.content.decode("utf-8", errors="replace")

    # ----- Cache écriture -----
    if use_cache:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cached = _cache_path(url)
        cached.write_text(html, encoding="utf-8")
        logger.info("Cache écrit : %s", cached)

    return html


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def parse_week_range(soup: BeautifulSoup) -> Tuple[str, str]:
    """Extrait la plage de dates 'Semaine du DD/MM/YYYY au DD/MM/YYYY'."""
    for heading in soup.find_all(["h2", "h3"]):
        txt = " ".join(heading.get_text(" ", strip=True).split())
        m = WEEK_RE.search(txt)
        if m:
            start = dtparse(m.group(1), dayfirst=True).date().isoformat()
            end = dtparse(m.group(2), dayfirst=True).date().isoformat()
            logger.debug("Période trouvée : %s → %s", start, end)
            return start, end
    raise ScrapingStructureError(
        "Impossible de trouver la période (Semaine du ... au ...) sur la page."
    )


def clean_text(s: str) -> str:
    """Normalise espaces (y compris insécables) et strip."""
    return " ".join(s.replace("\xa0", " ").split()).strip()


def extract_phones(text: str) -> List[str]:
    """Extrait et normalise les numéros de téléphone ivoiriens.

    Gère les formats 8 et 10 chiffres.
    Les séquences > 10 chiffres sont découpées en blocs de 10 puis de 8.
    """
    parts = re.split(r"[\/|,;]", text)
    phones: List[str] = []
    for p in parts:
        p = clean_text(p)
        if not p:
            continue
        digits = re.sub(r"\D+", "", p)

        if len(digits) in (8, 10):
            phones.append(digits)
        elif len(digits) > 10:
            # Tenter de découper en numéros de 10 chiffres
            _split_long_number(digits, phones)

    # Dédoublonnage en préservant l'ordre
    seen: set[str] = set()
    out: List[str] = []
    for ph in phones:
        if ph not in seen:
            seen.add(ph)
            out.append(ph)
    return out


def _split_long_number(digits: str, accumulator: List[str]) -> None:
    """Découpe une séquence de chiffres trop longue en numéros valides.

    Stratégie : blocs de 10 d'abord, puis de 8 pour le reste.
    """
    pos = 0
    while pos < len(digits):
        remaining = len(digits) - pos
        if remaining >= 10:
            accumulator.append(digits[pos : pos + 10])
            pos += 10
        elif remaining == 8:
            accumulator.append(digits[pos : pos + 8])
            pos += 8
        else:
            # Fragment trop court pour être un numéro valide → on l'ignore
            logger.debug(
                "Fragment de numéro ignoré (%d chiffres) : %s",
                remaining,
                digits[pos:],
            )
            break


# ---------------------------------------------------------------------------
# Collecte des infos d'une pharmacie (entre deux headings)
# ---------------------------------------------------------------------------
def _collect_pharmacy_details(
    start_tag: Tag,
) -> Tuple[List[str], str]:
    """Collecte adresse et téléphones entre un <h4> et le prochain heading.

    Utilise find_next_sibling() pour éviter de re-parcourir les nœuds enfants
    et restreint aux balises de contenu connues.
    """
    address_lines: List[str] = []
    phone_text = ""

    # On cherche les siblings du parent contenant le h4
    # (certaines pages imbriquent h4 dans des div)
    sibling = start_tag.find_next_sibling()
    while sibling:
        # Arrêt si on atteint un nouveau heading
        if isinstance(sibling, Tag) and sibling.name in HEADING_TAGS:
            break

        if isinstance(sibling, Tag):
            txt = clean_text(sibling.get_text(" ", strip=True))
            if txt:
                if PHONE_LIKE_RE.search(txt):
                    phone_text += " " + txt
                else:
                    address_lines.append(txt)

        sibling = sibling.find_next_sibling()

    # Fallback : si find_next_sibling n'a rien donné (structure imbriquée),
    # on utilise find_next() restreint aux balises de contenu connues.
    if not address_lines and not phone_text:
        logger.debug("Fallback find_next() pour '%s'", clean_text(start_tag.get_text()))
        seen_texts: set[str] = set()
        node = start_tag.find_next(CONTENT_TAGS | HEADING_TAGS)
        while node:
            if node.name in HEADING_TAGS:
                break
            txt = clean_text(node.get_text(" ", strip=True))
            if txt and txt not in seen_texts:
                seen_texts.add(txt)
                if PHONE_LIKE_RE.search(txt):
                    phone_text += " " + txt
                else:
                    address_lines.append(txt)
            node = node.find_next(CONTENT_TAGS | HEADING_TAGS)

    return address_lines, phone_text


# ---------------------------------------------------------------------------
# Parsing principal
# ---------------------------------------------------------------------------
def parse_annuaireci(html: str) -> Dict:
    """Parse le HTML d'annuaireci.com et retourne les données structurées."""
    soup = BeautifulSoup(html, "html.parser")

    week_start, week_end = parse_week_range(soup)

    # --- Point d'ancrage ---
    anchor: Optional[Tag] = None
    for tag in soup.find_all(["h2", "h3"]):
        if "Liste des pharmacies de garde" in tag.get_text(strip=True):
            anchor = tag
            break
    if anchor is None:
        raise ScrapingStructureError(
            "Ancre 'Liste des pharmacies de garde' introuvable."
        )

    data: Dict = {
        "source": "annuaireci",
        "source_url": URL,
        "week_start": week_start,
        "week_end": week_end,
        "areas": [],
        "scraped_at": date.today().isoformat(),
    }

    current_area_obj: Optional[Dict] = None

    # --- Parcours optimisé : on ne visite que les headings et balises de contenu ---
    target_tags = list(HEADING_TAGS | CONTENT_TAGS)
    node = anchor.find_next(target_tags)

    while node:
        tag_name = node.name if isinstance(node, Tag) else None

        # Condition d'arrêt
        if tag_name in ("h2", "h3"):
            heading_text = clean_text(node.get_text())
            if heading_text in STOP_TITLES:
                logger.debug("Stop : titre '%s' rencontré", heading_text)
                break

        # Nouvelle zone géographique
        if tag_name == "h3":
            area_name = clean_text(node.get_text())
            current_area_obj = {"area": area_name, "pharmacies": []}
            data["areas"].append(current_area_obj)
            logger.debug("Zone : %s", area_name)

        # Nouvelle pharmacie
        elif tag_name == "h4" and current_area_obj is not None:
            name = clean_text(node.get_text())
            address_lines, phone_text = _collect_pharmacy_details(node)

            phones = extract_phones(phone_text)
            address = clean_text(" ".join(address_lines)) if address_lines else ""

            current_area_obj["pharmacies"].append(
                {
                    "name_raw": name,
                    "address_raw": address,
                    "phones_raw": phones,
                }
            )
            logger.debug("  Pharmacie : %s | %s | %s", name, address, phones)

        node = node.find_next(target_tags)

    # --- Statistiques de résultat ---
    total_areas = len(data["areas"])
    total_pharmacies = sum(len(a["pharmacies"]) for a in data["areas"])
    logger.info(
        "Résultat : %d zones, %d pharmacies (période %s → %s)",
        total_areas,
        total_pharmacies,
        week_start,
        week_end,
    )
    if total_pharmacies == 0:
        logger.warning("⚠ Aucune pharmacie extraite — structure HTML peut-être modifiée")

    return data


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main() -> None:
    """Point d'entrée principal avec logging configuré."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    import argparse

    parser = argparse.ArgumentParser(description="Scrape les pharmacies de garde (annuaireci.com)")
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Utiliser le cache HTML local (utile en dev/debug)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Valider la structure HTML sans parser les pharmacies",
    )
    args = parser.parse_args()

    # --- Récupération du HTML ---
    html = fetch_html(URL, use_cache=args.cache)

    # --- Monitoring : validation de la structure ---
    alerts = validate_html_structure(html)
    if alerts:
        for alert in alerts:
            logger.warning(alert)
        if args.validate_only:
            sys.exit(1)
    else:
        logger.info("Validation structure HTML : OK")
        if args.validate_only:
            sys.exit(0)

    # --- Parsing ---
    try:
        payload = parse_annuaireci(html)
    except ScrapingStructureError as exc:
        logger.error("Erreur de structure : %s", exc)
        sys.exit(1)

    # Écriture directe en fichier UTF-8 (évite les problèmes GBK/UTF-16 de Windows)
    out_path = Path(__file__).resolve().parent / "annuaireci_week.json"
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("JSON écrit : %s", out_path)

    # Vérification rapide : pas de caractères mojibake résiduels
    bad_chars = ["茅", "猫", "脟", "掳", "鈥"]
    txt = out_path.read_text(encoding="utf-8")
    if any(c in txt for c in bad_chars):
        logger.warning("⚠ Encodage encore cassé : caractères suspects trouvés dans %s", out_path)
    else:
        logger.info("✅ Encodage OK : aucun caractère suspect")


if __name__ == "__main__":
    main()
