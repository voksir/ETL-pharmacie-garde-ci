from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("unppci_parse_pdf")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent

# Caractères accentués fréquents dans les PDF UNPPCI
_ACC = r"A-ZÉÈÊËÂÀÎÏÔÖÛÜÇ"

# --- Regex pour les en-têtes de semaine ---
# Les PDF UNPPCI utilisent 3 formats possibles :
#
# Format A (même mois) :
#   "SEMAINE DU SAMEDI 07 AU VENDREDI 13 FEVRIER 2026"
#   → jour_debut, jour_fin, mois_partagé, année_partagée
#
# Format B (mois différents, année partagée) :
#   "SEMAINE DU SAMEDI 28 FEVRIER AU VENDREDI 06 MARS 2026"
#   → jour_debut, mois_debut, jour_fin, mois_fin, année_partagée
#
# Format C (ancien, 2 mois + 2 années) :
#   "SEMAINE DU SAMEDI 02 MARS 2019 AU VENDREDI 08 MARS 2019"
#   → jour_debut, mois_debut, année_debut, jour_fin, mois_fin, année_fin

WEEK_RE_A = re.compile(
    r"SEMAINE\s+DU\s+"
    rf"(?:[{_ACC}]+\s+)?"           # jour de la semaine (optionnel)
    r"(\d{1,2})\s+"                 # jour début (groupe 1)
    r"AU\s+"
    rf"(?:[{_ACC}]+\s+)?"           # jour de la semaine (optionnel)
    r"(\d{1,2})\s+"                 # jour fin (groupe 2)
    rf"([{_ACC}]+)\s+"              # mois partagé (groupe 3)
    r"(\d{4})",                     # année partagée (groupe 4)
    re.IGNORECASE,
)

WEEK_RE_B = re.compile(
    r"SEMAINE\s+DU\s+"
    rf"(?:[{_ACC}]+\s+)?"           # jour de la semaine (optionnel)
    r"(\d{1,2})\s+"                 # jour début (groupe 1)
    rf"([{_ACC}]+)\s+"              # mois début (groupe 2)
    r"AU\s+"
    rf"(?:[{_ACC}]+\s+)?"           # jour de la semaine (optionnel)
    r"(\d{1,2})\s+"                 # jour fin (groupe 3)
    rf"([{_ACC}]+)\s+"              # mois fin (groupe 4)
    r"(\d{4})",                     # année partagée (groupe 5)
    re.IGNORECASE,
)

WEEK_RE_C = re.compile(
    r"SEMAINE\s+DU\s+"
    rf"(?:[{_ACC}]+\s+)?"           # jour de la semaine (optionnel)
    r"(\d{1,2})\s+"                 # jour début (groupe 1)
    rf"([{_ACC}]+)\s+"              # mois début (groupe 2)
    r"(\d{4})\s+"                   # année début (groupe 3)
    r"AU\s+"
    rf"(?:[{_ACC}]+\s+)?"           # jour de la semaine (optionnel)
    r"(\d{1,2})\s+"                 # jour fin (groupe 4)
    rf"([{_ACC}]+)\s+"              # mois fin (groupe 5)
    r"(\d{4})",                     # année fin (groupe 6)
    re.IGNORECASE,
)

# Détection de pharmacie en début de ligne (format Abidjan)
PHARM_RE = re.compile(r"^(PHCIE|PHARMACIE)\s+(.+)$", re.IGNORECASE)

# Détection de pharmacie avec ville en préfixe (format Intérieur)
# Ex: "ABENGOUROU PHCIE DU MARCHE / MME ..."
CITY_PHARM_RE = re.compile(
    r"^([A-Z][A-Z\s-]{2,}?)\s+(PHCIE|PHARMACIE)\s+(.+)$",
    re.IGNORECASE,
)

MONTHS: Dict[str, int] = {
    "JANVIER": 1,
    "FEVRIER": 2, "FÉVRIER": 2,
    "MARS": 3,
    "AVRIL": 4,
    "MAI": 5,
    "JUIN": 6,
    "JUILLET": 7,
    "AOUT": 8, "AOÛT": 8,
    "SEPTEMBRE": 9,
    "OCTOBRE": 10,
    "NOVEMBRE": 11,
    "DECEMBRE": 12, "DÉCEMBRE": 12,
}

PHONE_LIKE_RE = re.compile(r"\d{2}(?:[\s./-]?\d{2}){3,}")

# Mots-clés d'adresse (pour exclure des faux « noms de zone »)
ADDRESS_KEYWORDS = frozenset([
    "ROUTE", "CARREFOUR", "AVENUE", "BD", "BOULEVARD", "FACE", "PRES",
    "PRÈS", "DERRIERE", "DERRIÈRE", "ARRÊT", "ARRET", "RUE", "LOT",
    "IMMEUBLE", "VILLA", "CITÉ", "CITE", "CAMP", "MARCHE", "STATION",
    "QUARTIER", "PLACE", "ROND", "ENTRE", "APRES", "APRÈS", "DEVANT",
])

# Mots-clés d'en-tête à ignorer
HEADER_PREFIXES = ("UNION", "GARDE", "SEMAINE", "PERMANENCE", "SECTION",
                   "TOUR", "TEL", "N°", "N  ")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean(s: str) -> str:
    """Nettoie le texte extrait du PDF."""
    s = (s or "").replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    # Corrige les concaténations de saut de page (ex: "...BUS 04SEMAINE ...")
    s = re.sub(r"(\d)SEMAINE", r"\1 SEMAINE", s, flags=re.IGNORECASE)
    return s.strip()


def fr_date_to_iso(day: str, month_name: str, year: str) -> str:
    """Convertit une date française en ISO 8601."""
    m = MONTHS.get(month_name.upper())
    if not m:
        raise ValueError(f"Mois inconnu : {month_name}")
    return date(int(year), m, int(day)).isoformat()


def _try_parse_week(line: str) -> Optional[Tuple[str, str]]:
    """Tente de parser une ligne comme un en-tête de semaine.

    Retourne (week_start_iso, week_end_iso) ou None.
    Essaie les 3 formats dans l'ordre : C (le plus spécifique) → B → A.
    """
    # Format C : DD MOIS ANNEE AU DD MOIS ANNEE
    mc = WEEK_RE_C.search(line)
    if mc:
        ws = fr_date_to_iso(mc.group(1), mc.group(2), mc.group(3))
        we = fr_date_to_iso(mc.group(4), mc.group(5), mc.group(6))
        return ws, we

    # Format B : DD MOIS AU DD MOIS ANNEE
    mb = WEEK_RE_B.search(line)
    if mb:
        year = mb.group(5)
        ws = fr_date_to_iso(mb.group(1), mb.group(2), year)
        we = fr_date_to_iso(mb.group(3), mb.group(4), year)
        return ws, we

    # Format A : DD AU DD MOIS ANNEE (mois et année partagés)
    ma = WEEK_RE_A.search(line)
    if ma:
        month = ma.group(3)
        year = ma.group(4)
        ws = fr_date_to_iso(ma.group(1), month, year)
        we = fr_date_to_iso(ma.group(2), month, year)
        return ws, we

    return None


def extract_phones(text: str) -> List[str]:
    """Extrait les numéros de téléphone (8 ou 10 chiffres) depuis un texte."""
    parts = re.split(r"[\/|,;]", text)
    phones: List[str] = []
    for p in parts:
        digits = re.sub(r"\D+", "", p)
        if len(digits) in (8, 10):
            phones.append(digits)
    # Dédoublonnage stable
    seen: set[str] = set()
    out: List[str] = []
    for x in phones:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def strip_phones_from_line(line: str) -> str:
    """Retire les segments qui ressemblent à des numéros de téléphone d'une ligne.

    Retourne la partie « adresse » restante.
    """
    cleaned = PHONE_LIKE_RE.sub(" ", line)
    cleaned = re.sub(r"[\/|;,]\s*$", "", cleaned)
    cleaned = re.sub(r"^\s*[\/|;,]", "", cleaned)
    # Retirer aussi "TEL." / "TEL:" résiduel
    cleaned = re.sub(r"\bTEL\s*[.:]\s*", " ", cleaned, flags=re.IGNORECASE)
    return clean(cleaned)


def _is_pure_digits_line(line: str) -> bool:
    """Vérifie si la ligne ne contient que des chiffres/espaces/séparateurs."""
    return bool(re.fullmatch(r"[\d\s./-]+", line))


def looks_like_area(line: str) -> bool:
    """Heuristique : détecte si une ligne est un nom de zone géographique.

    Critères :
    - Non vide, entièrement en MAJUSCULES
    - Ne commence pas par un mot-clé d'en-tête ou PHCIE/PHARMACIE
    - Ne contient pas de mot-clé d'adresse
    - Longueur raisonnable (≤ 50 caractères)
    - N'est pas une ligne de chiffres purs (téléphones)
    """
    if not line:
        return False

    up = line.upper().strip()

    # Exclure les en-têtes
    if any(up.startswith(prefix) for prefix in HEADER_PREFIXES):
        return False

    # Exclure les lignes qui contiennent PHCIE/PHARMACIE
    if "PHCIE" in up or "PHARMACIE" in up:
        return False

    # Exclure les mots-clés d'adresse
    words = set(re.findall(rf"[{_ACC}]+", up))
    if words & ADDRESS_KEYWORDS:
        return False

    # Exclure les lignes trop longues
    if len(line) > 50:
        return False

    # Exclure les lignes de chiffres purs (téléphones)
    if _is_pure_digits_line(line):
        return False

    # Les zones dans les PDF UNPPCI sont en majuscules
    return line == up


def _extract_pharmacy_name(rest: str) -> str:
    """Extrait le nom de la pharmacie depuis le texte après PHCIE/PHARMACIE.

    Coupe avant le premier séparateur (/, -, –, TEL.).
    """
    # Couper avant " / ", " - ", " – " ou "TEL."
    name_raw = re.split(r"\s*[–]\s*|\s+-\s*|\s*/\s*|\bTEL\b", rest, maxsplit=1, flags=re.IGNORECASE)[0]
    return clean(name_raw.strip())


# ---------------------------------------------------------------------------
# Parsing principal
# ---------------------------------------------------------------------------
def parse_unppci_pdf(
    pdf_path: str,
    *,
    source_url: str = "",
) -> Dict[str, Any]:
    """Parse un PDF UNPPCI et retourne un payload structuré multi-semaines.

    Gère deux formats de PDF :
    - Format « Abidjan » : zones séparées sur leurs propres lignes, puis
      lignes PHCIE en dessous.
    - Format « Intérieur » : la ville est en préfixe sur la ligne pharmacie
      (ex: "BOUAKE PHCIE BEL AIR / ...").

    Args:
        pdf_path: Chemin vers le fichier PDF.
        source_url: URL d'origine du PDF (pour traçabilité).
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    pdf_path_obj = Path(pdf_path)

    logger.info("Parsing PDF : %s", pdf_path_obj.name)

    weeks: List[Dict[str, Any]] = []
    current_week: Optional[Dict[str, Any]] = None
    current_area: Optional[Dict[str, Any]] = None
    # Pour le format intérieur : cache des zones par nom dans la semaine courante
    area_by_name: Dict[str, Dict[str, Any]] = {}

    total_lines = 0
    classified = {"week": 0, "area": 0, "pharmacy": 0, "address": 0,
                  "phone": 0, "skipped": 0, "ignored_pre_week": 0}

    with pdfplumber.open(pdf_path) as pdf:
        logger.info("  Pages : %d", len(pdf.pages))

        for page_num, page in enumerate(pdf.pages, 1):
            # extract_text() sans layout : plus fiable pour le parsing ligne par ligne
            txt = page.extract_text() or ""
            lines = [clean(x) for x in txt.splitlines()]
            logger.debug("  Page %d : %d lignes", page_num, len(lines))

            for line in lines:
                if not line:
                    continue
                total_lines += 1

                # --- Détection de semaine ---
                week_dates = _try_parse_week(line)
                if week_dates:
                    ws, we = week_dates
                    # Éviter les doublons si la même semaine est répétée sur chaque page
                    if weeks and weeks[-1]["week_start"] == ws and weeks[-1]["week_end"] == we:
                        # Même semaine que la précédente, on continue dans le même contexte
                        logger.debug("    [SEMAINE-REPEAT] %s → %s (page %d)", ws, we, page_num)
                    else:
                        current_week = {"week_start": ws, "week_end": we, "areas": []}
                        weeks.append(current_week)
                        current_area = None
                        area_by_name = {}
                        classified["week"] += 1
                        logger.debug("    [SEMAINE] %s → %s", ws, we)
                    continue

                # Avant la première semaine, on ignore tout
                if current_week is None:
                    classified["ignored_pre_week"] += 1
                    continue

                # --- Lignes de section / en-tête (ignorées) ---
                up = line.upper()
                if up.startswith("SECTION") or up.startswith("PERMANENCE") or up.startswith("TOUR DE GARDE"):
                    classified["skipped"] += 1
                    continue

                # --- Détection de zone géographique (format Abidjan) ---
                if looks_like_area(line):
                    area_name = line.upper().strip()
                    if area_name in area_by_name:
                        current_area = area_by_name[area_name]
                    else:
                        current_area = {"area": line, "pharmacies": []}
                        current_week["areas"].append(current_area)
                        area_by_name[area_name] = current_area
                    classified["area"] += 1
                    logger.debug("    [ZONE] %s", line)
                    continue

                # --- Détection de pharmacie avec ville en préfixe (format Intérieur) ---
                # Ex: "ABENGOUROU PHCIE DU MARCHE / MME ..."
                # Ex: "BOUAKE PHCIE BEL AIR / M. KONE ..."
                mcity = CITY_PHARM_RE.match(line)
                if mcity and current_week is not None:
                    city_prefix = mcity.group(1).strip().upper()
                    rest = mcity.group(3).strip()

                    # Vérifier que le préfixe ressemble à un nom de ville
                    # (pas un mot-clé d'adresse ou un mot trop court)
                    city_words = set(city_prefix.split())
                    if not (city_words & ADDRESS_KEYWORDS) and len(city_prefix) >= 3:
                        # Créer ou réutiliser la zone
                        if city_prefix in area_by_name:
                            current_area = area_by_name[city_prefix]
                        else:
                            current_area = {"area": city_prefix, "pharmacies": []}
                            current_week["areas"].append(current_area)
                            area_by_name[city_prefix] = current_area
                            logger.debug("    [ZONE-AUTO] %s", city_prefix)

                        name_raw = _extract_pharmacy_name(rest)
                        phones = extract_phones(line)

                        current_area["pharmacies"].append({
                            "name_raw": name_raw,
                            "address_raw": "",
                            "phones_raw": phones,
                        })
                        classified["pharmacy"] += 1
                        logger.debug("    [PHARM-CITY] %s > %s (tél: %s)", city_prefix, name_raw, phones)
                        continue

                # --- Détection de pharmacie standard (format Abidjan) ---
                mph = PHARM_RE.match(line)
                if mph and current_area is not None:
                    rest = mph.group(2).strip()
                    name_raw = _extract_pharmacy_name(rest)
                    phones = extract_phones(line)

                    current_area["pharmacies"].append({
                        "name_raw": name_raw,
                        "address_raw": "",
                        "phones_raw": phones,
                    })
                    classified["pharmacy"] += 1
                    logger.debug("    [PHARMACIE] %s (tél: %s)", name_raw, phones)
                    continue

                # --- Ligne d'adresse / téléphone (rattachée à la dernière pharmacie) ---
                if current_area and current_area["pharmacies"]:
                    last = current_area["pharmacies"][-1]

                    has_phone = bool(PHONE_LIKE_RE.search(line))

                    if has_phone:
                        # Extraire les téléphones et les ajouter
                        new_phones = extract_phones(line)
                        for ph in new_phones:
                            if ph not in last["phones_raw"]:
                                last["phones_raw"].append(ph)
                        classified["phone"] += 1

                        # Extraire aussi la partie adresse résiduelle
                        addr_part = strip_phones_from_line(line)
                        if addr_part and not _is_pure_digits_line(addr_part):
                            last["address_raw"] = clean(
                                (last["address_raw"] + " " + addr_part).strip()
                            )
                            logger.debug("    [ADDR+TEL] addr='%s' tél=%s", addr_part, new_phones)
                        else:
                            logger.debug("    [TEL] %s", new_phones)
                    else:
                        # Ligne d'adresse pure
                        last["address_raw"] = clean(
                            (last["address_raw"] + " " + line).strip()
                        )
                        classified["address"] += 1
                        logger.debug("    [ADRESSE] %s", line)
                    continue

                # Ligne non classifiée
                classified["skipped"] += 1
                logger.debug("    [???] %s", line)

    # --- Statistiques ---
    total_areas = sum(len(w["areas"]) for w in weeks)
    total_pharmacies = sum(
        len(a["pharmacies"]) for w in weeks for a in w["areas"]
    )
    logger.info(
        "Résultat : %d semaines, %d zones, %d pharmacies",
        len(weeks), total_areas, total_pharmacies,
    )
    logger.info(
        "Classification : %d lignes — semaine=%d, zone=%d, pharmacie=%d, "
        "adresse=%d, téléphone=%d, ignoré=%d, pré-semaine=%d",
        total_lines, classified["week"], classified["area"],
        classified["pharmacy"], classified["address"], classified["phone"],
        classified["skipped"], classified["ignored_pre_week"],
    )

    if total_pharmacies == 0:
        logger.warning("Aucune pharmacie extraite — vérifier la structure du PDF")

    return {
        "source": "unppci",
        "source_url": source_url,
        "source_file": pdf_path_obj.name,
        "scraped_at": scraped_at,
        "weeks": weeks,
    }


# ---------------------------------------------------------------------------
# Point d'entrée CLI
# ---------------------------------------------------------------------------
def main() -> None:
    """Point d'entrée standalone pour tester le parsing d'un PDF."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(
        description="Parse un PDF UNPPCI de tours de garde et produit du JSON"
    )
    parser.add_argument(
        "pdf", nargs="?", default=None,
        help="Chemin vers le PDF à parser (si omis, cherche dans downloads_unppci/)",
    )
    parser.add_argument(
        "--source-url", default="",
        help="URL d'origine du PDF (pour traçabilité)",
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Fichier JSON de sortie (défaut: stdout)",
    )
    args = parser.parse_args()

    # Trouver le PDF
    if args.pdf:
        pdf_path = Path(args.pdf)
    else:
        # Chercher le premier PDF dans downloads_unppci/
        dl_dir = SCRIPT_DIR / "downloads_unppci"
        pdfs = sorted(dl_dir.glob("*.pdf")) if dl_dir.exists() else []
        if not pdfs:
            logger.error("Aucun PDF trouvé. Spécifiez un chemin ou lancez unppci_discover.py --download")
            sys.exit(1)
        pdf_path = pdfs[0]
        logger.info("PDF auto-détecté : %s", pdf_path.name)

    if not pdf_path.exists():
        logger.error("Fichier introuvable : %s", pdf_path)
        sys.exit(1)

    # Parser
    payload = parse_unppci_pdf(str(pdf_path), source_url=args.source_url)

    # Sortie JSON
    json_str = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.output:
        out_path = Path(args.output)
        out_path.write_text(json_str, encoding="utf-8")
        logger.info("JSON écrit : %s", out_path)
    else:
        sys.stdout.reconfigure(encoding="utf-8")
        print(json_str)


if __name__ == "__main__":
    main()
