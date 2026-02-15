from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from supabase import create_client, Client

from db_helpers import (
    norm_text,
    parse_area,
    phones_to_e164_ci,
    compute_pharmacy_key,
    compute_duty_key,
    now_utc_iso,
    chunks,
    upsert_with_retry,
    fetch_key_to_id,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("load_annuaireci")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Main load
# ---------------------------------------------------------------------------
def main() -> None:
    """Point d'entrée principal."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    t_start = time.monotonic()

    # --- Env ---
    load_dotenv(SCRIPT_DIR.parent / ".env")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        logger.error("Variables SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY manquantes dans .env")
        sys.exit(1)

    sb = create_client(url, key)
    logger.info("Connexion Supabase OK")

    # --- Lecture du JSON (chemin relatif au script) ---
    input_path = SCRIPT_DIR / "annuaireci_week.json"
    if not input_path.exists():
        logger.error("Fichier introuvable : %s", input_path)
        sys.exit(1)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    logger.info("Fichier chargé : %s", input_path)

    source = payload["source"]
    source_url = payload["source_url"]
    week_start = payload["week_start"]
    week_end = payload["week_end"]
    scraped_at = now_utc_iso()

    logger.info("Période : %s → %s (source: %s)", week_start, week_end, source)

    # --- Phase 1 : Préparer les lignes ---
    pharmacy_rows: List[Dict[str, Any]] = []
    duty_rows_pre: List[Dict[str, Any]] = []

    for area in payload.get("areas", []):
        area_raw = area.get("area", "")
        city_norm, city_raw, sector = parse_area(area_raw)

        for ph in area.get("pharmacies", []):
            name_raw = (ph.get("name_raw") or "").strip()
            address_raw = (ph.get("address_raw") or "").strip()
            phones_raw = ph.get("phones_raw") or []

            name_norm = norm_text(name_raw)
            address_norm = norm_text(address_raw)
            phones_e164 = phones_to_e164_ci(phones_raw)

            # Clé stable : ville + nom seulement
            pharmacy_key = compute_pharmacy_key(city_norm, name_norm)

            pharmacy_rows.append({
                "pharmacy_key": pharmacy_key,
                "name_raw": name_raw,
                "name_norm": name_norm,
                "address_raw": address_raw,
                "address_norm": address_norm,
                "area_raw": area_raw,
                "city_norm": city_norm,
                "sector": sector,
                "phones_raw": phones_raw,
                "phones_e164": phones_e164,
                "source_last": source,
                "source_url_last": source_url,
                "updated_at": scraped_at,
            })

            duty_rows_pre.append({
                "duty_key": compute_duty_key(pharmacy_key, week_start, week_end, source),
                "pharmacy_key": pharmacy_key,
                "start_date": week_start,
                "end_date": week_end,
                "source": source,
                "source_url": source_url,
                "scraped_at": scraped_at,
            })

    # Dédup pharmacies par pharmacy_key (garder le dernier)
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in pharmacy_rows:
        dedup[r["pharmacy_key"]] = r
    pharmacy_rows = list(dedup.values())

    logger.info(
        "Préparé : %d pharmacies uniques, %d périodes de garde",
        len(pharmacy_rows), len(duty_rows_pre),
    )

    # --- Phase 2 : Upsert pharmacies ---
    logger.info("Upsert pharmacies...")
    n_ph = upsert_with_retry(sb, "pharmacies", pharmacy_rows, "pharmacy_key", chunk_size=200)
    logger.info("Pharmacies upsertées : %d", n_ph)

    # --- Phase 3 : Récupérer le mapping pharmacy_key → id ---
    all_keys = [r["pharmacy_key"] for r in pharmacy_rows]
    key_to_id = fetch_key_to_id(sb, all_keys)

    missing = [k for k in all_keys if k not in key_to_id]
    if missing:
        logger.error(
            "ERREUR : %d pharmacy_key sans id après upsert. Exemples : %s",
            len(missing), missing[:3],
        )
        sys.exit(1)

    logger.info("Mapping pharmacy_key → id : %d entrées", len(key_to_id))

    # --- Phase 4 : Construire et upsert les duty_periods ---
    duty_rows: List[Dict[str, Any]] = []
    for d in duty_rows_pre:
        pid = key_to_id.get(d["pharmacy_key"])
        if pid is None:
            logger.warning("pharmacy_key introuvable, duty ignorée : %s", d["pharmacy_key"])
            continue
        duty_rows.append({
            "duty_key": d["duty_key"],
            "pharmacy_id": pid,
            "start_date": d["start_date"],
            "end_date": d["end_date"],
            "source": d["source"],
            "source_url": d["source_url"],
            "scraped_at": d["scraped_at"],
        })

    # Dédup duty rows par duty_key
    duty_dedup: Dict[str, Dict[str, Any]] = {}
    for r in duty_rows:
        duty_dedup[r["duty_key"]] = r
    duty_rows = list(duty_dedup.values())

    logger.info("Upsert duty_periods...")
    n_duty = upsert_with_retry(sb, "duty_periods", duty_rows, "duty_key", chunk_size=500)
    logger.info("Duty periods upsertées : %d", n_duty)

    # --- Résumé ---
    elapsed = time.monotonic() - t_start
    logger.info(
        "✅ Terminé en %.1fs — %d pharmacies, %d duty_periods",
        elapsed, n_ph, n_duty,
    )


if __name__ == "__main__":
    main()
