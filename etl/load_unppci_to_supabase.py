"""Charge les données UNPPCI (PDF tours de garde) dans Supabase.

Pipeline complet :
  1. Découvre les articles et PDF sur unppci.org
  2. Filtre les PDF du mois courant (GARDE + GARDE INTÉRIEUR)
  3. Télécharge les PDF
  4. Parse chaque PDF avec unppci_parse_pdf
  5. Normalise et upsert dans Supabase (pharmacies + duty_periods)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
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
    upsert_with_retry,
    fetch_key_to_id,
)
from unppci_discover import (
    discover_articles,
    discover_pdfs_from_article,
    download_pdf as discover_download_pdf,
    filter_pdfs_current_month,
    PdfDoc,
)
from unppci_parse_pdf import parse_unppci_pdf

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("load_unppci")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Découverte & téléchargement
# ---------------------------------------------------------------------------
def discover_and_download(
    *,
    use_cache: bool = True,
    current_month_only: bool = True,
    max_articles: int = 5,
) -> List[Dict[str, Any]]:
    """Découvre les articles, extrait les PDF, télécharge et parse.

    Retourne une liste de dicts : {"pdf": PdfDoc, "path": Path, "payload": dict}.
    """
    logger.info("Découverte des articles UNPPCI...")
    articles = discover_articles(use_cache=use_cache, max_pages=3, garde_only=True)

    if not articles:
        logger.warning("Aucun article trouvé sur UNPPCI.")
        return []

    # Garder les N articles les plus récents (ID le plus élevé = plus récent)
    articles_sorted = sorted(articles, key=lambda a: a.id, reverse=True)[:max_articles]
    logger.info("%d articles de garde trouvés (top %d retenus)", len(articles), len(articles_sorted))

    # Extraire tous les PDF
    all_pdfs: List[PdfDoc] = []
    for art in articles_sorted:
        pdfs = discover_pdfs_from_article(art, use_cache=use_cache)
        garde_pdfs = [p for p in pdfs if p.is_garde]
        all_pdfs.extend(garde_pdfs)

    logger.info("Total PDF de garde découverts : %d", len(all_pdfs))

    # Filtrer au mois courant si demandé
    if current_month_only:
        filtered = filter_pdfs_current_month(all_pdfs)
        logger.info("PDF filtrés (mois courant) : %d / %d", len(filtered), len(all_pdfs))
        all_pdfs = filtered

    if not all_pdfs:
        logger.warning("Aucun PDF pertinent trouvé après filtrage.")
        return []

    # Télécharger et parser chaque PDF
    results: List[Dict[str, Any]] = []
    for pdf in all_pdfs:
        logger.info("Téléchargement : %s — %s", pdf.label, pdf.url)
        pdf_path = discover_download_pdf(pdf, use_cache=use_cache)
        if pdf_path is None:
            logger.error("  Échec du téléchargement : %s", pdf.url)
            continue

        logger.info("  Parsing : %s", pdf_path.name)
        payload = parse_unppci_pdf(str(pdf_path), source_url=pdf.url)
        results.append({"pdf": pdf, "path": pdf_path, "payload": payload})

    return results


# ---------------------------------------------------------------------------
# Chargement Supabase
# ---------------------------------------------------------------------------
def load_payload_to_supabase(
    sb: Client,
    payload: Dict[str, Any],
    pdf_url: str,
    scraped_at: str,
) -> Dict[str, int]:
    """Charge un payload (issu de parse_unppci_pdf) dans Supabase.

    Retourne {"pharmacies": n, "duties": n}.
    """
    source = payload.get("source", "unppci")

    pharmacy_rows: List[Dict[str, Any]] = []
    duty_rows_pre: List[Dict[str, Any]] = []

    for wk in payload.get("weeks", []):
        week_start = wk["week_start"]
        week_end = wk["week_end"]

        for area in wk.get("areas", []):
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
                    "source_url_last": pdf_url,
                    "updated_at": scraped_at,
                })

                duty_rows_pre.append({
                    "duty_key": compute_duty_key(pharmacy_key, week_start, week_end, source),
                    "pharmacy_key": pharmacy_key,
                    "start_date": week_start,
                    "end_date": week_end,
                    "source": source,
                    "source_url": pdf_url,
                    "scraped_at": scraped_at,
                })

    # --- Dédup pharmacies par pharmacy_key ---
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in pharmacy_rows:
        dedup[r["pharmacy_key"]] = r
    pharmacy_rows = list(dedup.values())

    logger.info(
        "  Préparé : %d pharmacies uniques, %d périodes de garde",
        len(pharmacy_rows), len(duty_rows_pre),
    )

    if not pharmacy_rows:
        logger.warning("  Aucune pharmacie à charger pour ce PDF.")
        return {"pharmacies": 0, "duties": 0}

    # --- Upsert pharmacies ---
    logger.info("  Upsert pharmacies...")
    n_ph = upsert_with_retry(sb, "pharmacies", pharmacy_rows, "pharmacy_key", chunk_size=200)
    logger.info("  Pharmacies upsertées : %d", n_ph)

    # --- Mapping pharmacy_key → id ---
    all_keys = [r["pharmacy_key"] for r in pharmacy_rows]
    key_to_id = fetch_key_to_id(sb, all_keys)

    missing = [k for k in all_keys if k not in key_to_id]
    if missing:
        logger.error(
            "  %d pharmacy_key sans id après upsert. Exemples : %s",
            len(missing), missing[:3],
        )

    logger.info("  Mapping pharmacy_key → id : %d entrées", len(key_to_id))

    # --- Construire et upsert les duty_periods ---
    duty_rows: List[Dict[str, Any]] = []
    for d in duty_rows_pre:
        pid = key_to_id.get(d["pharmacy_key"])
        if pid is None:
            logger.warning("  pharmacy_key introuvable, duty ignorée : %s", d["pharmacy_key"])
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

    logger.info("  Upsert duty_periods...")
    n_duty = upsert_with_retry(sb, "duty_periods", duty_rows, "duty_key", chunk_size=500)
    logger.info("  Duty periods upsertées : %d", n_duty)

    return {"pharmacies": n_ph, "duties": n_duty}


# ---------------------------------------------------------------------------
# Point d'entrée CLI
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

    parser = argparse.ArgumentParser(
        description="Charge les PDF UNPPCI de tours de garde dans Supabase"
    )
    parser.add_argument(
        "--no-cache", action="store_true",
        help="Désactiver le cache HTTP et PDF",
    )
    parser.add_argument(
        "--all-months", action="store_true",
        help="Charger tous les PDF trouvés (pas seulement le mois courant)",
    )
    parser.add_argument(
        "--max-articles", type=int, default=5,
        help="Nombre maximum d'articles récents à scanner (défaut: 5)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Forcer le rechargement même si le PDF semble déjà ingéré",
    )
    args = parser.parse_args()

    # --- Env ---
    load_dotenv(SCRIPT_DIR.parent / ".env")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        logger.error("Variables SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY manquantes dans .env")
        sys.exit(1)

    sb = create_client(url, key)
    logger.info("Connexion Supabase OK")

    # --- Découverte, téléchargement et parsing ---
    use_cache = not args.no_cache
    current_month_only = not args.all_months

    results = discover_and_download(
        use_cache=use_cache,
        current_month_only=current_month_only,
        max_articles=args.max_articles,
    )

    if not results:
        logger.warning("Aucun PDF à charger. Fin.")
        sys.exit(0)

    logger.info("PDF à charger : %d", len(results))

    # --- Chargement dans Supabase ---
    scraped_at = now_utc_iso()
    total_ph = 0
    total_duty = 0

    for item in results:
        pdf: PdfDoc = item["pdf"]
        payload: Dict[str, Any] = item["payload"]
        pdf_url = pdf.url

        # Vérification d'ingestion préalable (sauf si --force)
        if not args.force:
            try:
                resp = (
                    sb.table("duty_periods")
                    .select("id")
                    .eq("source", "unppci")
                    .eq("source_url", pdf_url)
                    .limit(1)
                    .execute()
                )
                if resp.data:
                    logger.info("SKIP (déjà ingéré) : %s", pdf_url)
                    continue
            except Exception as exc:
                logger.warning(
                    "Erreur lors de la vérification d'ingestion pour %s : %s — on continue",
                    pdf_url, exc,
                )

        logger.info("Chargement du PDF : %s — %s", pdf.label, pdf_url)
        counts = load_payload_to_supabase(sb, payload, pdf_url, scraped_at)
        total_ph += counts["pharmacies"]
        total_duty += counts["duties"]
        logger.info(
            "  → %d pharmacies, %d duty_periods",
            counts["pharmacies"], counts["duties"],
        )

    # --- Résumé ---
    elapsed = time.monotonic() - t_start
    logger.info(
        "✅ Terminé en %.1fs — %d pharmacies, %d duty_periods (total)",
        elapsed, total_ph, total_duty,
    )


if __name__ == "__main__":
    main()
