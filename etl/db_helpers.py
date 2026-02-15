"""Fonctions partagées entre les scripts de chargement Supabase.

Ce module centralise la normalisation, le calcul de clés, la conversion
téléphonique et les helpers d'upsert pour éviter la duplication entre
load_annuaireci_to_supabase.py et load_unppci_to_supabase.py.
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from supabase import Client
from unidecode import unidecode

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("db_helpers")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SECTOR_RE = re.compile(r"^(.*?)\s+Secteur\s+(\d+)\s*$", re.IGNORECASE)

# Retry sur les appels Supabase
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # secondes (×1, ×2, ×4)


# ---------------------------------------------------------------------------
# Normalisation texte
# ---------------------------------------------------------------------------
def norm_text(s: str) -> str:
    """Normalise un texte : supprime accents, minuscules, alphanum seulement."""
    s = (s or "").strip()
    s = unidecode(s).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split()).strip()


def parse_area(area_raw: str) -> Tuple[str, str, int | None]:
    """Extrait (city_norm, city_raw, sector) depuis une zone.

    Exemples :
      "YOPOUGON Secteur 9" → ("yopougon", "YOPOUGON", 9)
      "ABENGOUROU"         → ("abengourou", "ABENGOUROU", None)
    """
    area_raw = (area_raw or "").strip()
    m = SECTOR_RE.match(area_raw)
    if m:
        city_raw = m.group(1).strip()
        sector = int(m.group(2))
        return norm_text(city_raw), city_raw, sector
    return norm_text(area_raw), area_raw, None


# ---------------------------------------------------------------------------
# Téléphones
# ---------------------------------------------------------------------------
def phones_to_e164_ci(phones_raw: List[str]) -> List[str]:
    """Convertit les numéros ivoiriens en format E.164 (+225...).

    Supporte :
    - 10 chiffres (format actuel) : 07XXXXXXXX → +22507XXXXXXXX
    - 8 chiffres (ancien format pré-2021) : XXXXXXXX → +22501XXXXXXXX
    """
    out: List[str] = []
    for p in phones_raw or []:
        digits = re.sub(r"\D+", "", str(p))
        if len(digits) == 10:
            out.append("+225" + digits)
        elif len(digits) == 8:
            out.append("+22501" + digits)
            logger.debug("Numéro 8 chiffres converti : %s → +22501%s", p, digits)
    # Dédoublonnage en préservant l'ordre
    seen: set[str] = set()
    res: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res


# ---------------------------------------------------------------------------
# Clés d'idempotence (SHA-1)
# ---------------------------------------------------------------------------
def compute_pharmacy_key(city_norm: str, name_norm: str) -> str:
    """Clé stable basée sur ville + nom uniquement.

    Plus robuste que city+name+address+phone : une pharmacie reste la même
    si son adresse ou téléphone change légèrement entre deux semaines.
    """
    material = f"{city_norm}|{name_norm}".encode("utf-8")
    return hashlib.sha1(material).hexdigest()


def compute_duty_key(pharmacy_key: str, start_date: str, end_date: str, source: str) -> str:
    """Clé unique pour une période de garde."""
    material = f"{pharmacy_key}|{start_date}|{end_date}|{source}".encode("utf-8")
    return hashlib.sha1(material).hexdigest()


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------
def now_utc_iso() -> str:
    """Retourne l'horodatage UTC courant au format ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


def chunks(lst: list, n: int = 200):
    """Découpe une liste en chunks de taille n."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# ---------------------------------------------------------------------------
# Supabase helpers avec retry
# ---------------------------------------------------------------------------
def upsert_with_retry(
    sb: Client,
    table: str,
    rows: List[Dict[str, Any]],
    conflict_col: str,
    chunk_size: int = 200,
) -> int:
    """Upsert par chunks avec retry automatique sur erreur réseau.

    Retourne le nombre total de lignes upsertées.
    """
    total = 0
    for i, ch in enumerate(chunks(rows, chunk_size)):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(ch, on_conflict=conflict_col).execute()
                total += len(ch)
                logger.debug(
                    "  [%s] chunk %d : %d lignes upsertées (tentative %d)",
                    table, i + 1, len(ch), attempt,
                )
                break
            except Exception as exc:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "  [%s] chunk %d erreur (tentative %d/%d) : %s — retry dans %ds",
                        table, i + 1, attempt, MAX_RETRIES, exc, wait,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "  [%s] chunk %d ÉCHEC après %d tentatives : %s",
                        table, i + 1, MAX_RETRIES, exc,
                    )
                    raise
    return total


def fetch_key_to_id(
    sb: Client,
    all_keys: List[str],
) -> Dict[str, str]:
    """Récupère le mapping pharmacy_key → id depuis Supabase, par chunks."""
    key_to_id: Dict[str, str] = {}
    for ch in chunks(all_keys, 200):
        resp = sb.table("pharmacies").select("id, pharmacy_key").in_("pharmacy_key", ch).execute()
        for row in resp.data:
            key_to_id[row["pharmacy_key"]] = row["id"]
    return key_to_id
