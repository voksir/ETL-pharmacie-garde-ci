from __future__ import annotations

import argparse
import hashlib
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("unppci_discover")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent

UNPPCI_BASE = "https://www.unppci.org/"
UNPPCI_ARTICLES_CAT = "https://www.unppci.org/?cat=1&rw=actualites"

CACHE_DIR = SCRIPT_DIR / ".cache_unppci"
DOWNLOAD_DIR = SCRIPT_DIR / "downloads_unppci"

# Regex pour les PDF dans /uploads/
PDF_RE = re.compile(r"/uploads/.*\.pdf", re.IGNORECASE)

# Regex pour détecter les URLs PDF dans le JavaScript embarqué
JS_PDF_RE = re.compile(r"""["']((?:https?://[^"']*|/uploads/)[^"']*\.pdf)["']""", re.IGNORECASE)

# Regex pour les liens de téléchargement via onclick (pattern UNPPCI)
# Ex: onclick="window.open('controllers/downloads.php?id=972','ipost')"
ONCLICK_DL_RE = re.compile(
    r"""window\.open\(\s*['"]([^'"]*controllers/downloads\.php\?id=\d+)['"]""",
    re.IGNORECASE,
)

# Regex pour les liens d'articles UNPPCI
ARTICLE_ID_RE = re.compile(r"[?&]id=(\d+)")

# Mots-clés pour filtrer les articles/PDF liés aux gardes
GARDE_KEYWORDS_RE = re.compile(
    r"garde|tour\b|semaine|pharmacie.*mois|mois.*pharmacie",
    re.IGNORECASE,
)

# Noms des mois en français (index 1-12)
MOIS_FR = [
    "", "JANVIER", "FEVRIER", "MARS", "AVRIL", "MAI", "JUIN",
    "JUILLET", "AOUT", "SEPTEMBRE", "OCTOBRE", "NOVEMBRE", "DECEMBRE",
]


# ---------------------------------------------------------------------------
# Détection du mois courant
# ---------------------------------------------------------------------------
def get_current_month_label() -> str:
    """Retourne le label du mois courant en français majuscules (ex: 'FEVRIER')."""
    return MOIS_FR[date.today().month]


def get_current_year() -> int:
    """Retourne l'année courante."""
    return date.today().year


def filter_pdfs_current_month(pdfs: List[PdfDoc]) -> List[PdfDoc]:
    """Filtre les PDF pour ne garder que les 2 du mois courant.

    Garde uniquement :
    - "GARDE {MOIS} {ANNEE}"           (Abidjan / communes principales)
    - "GARDE INTERIEUR {MOIS} {ANNEE}" (villes de l'intérieur)

    La correspondance se fait sur le label du PDF, insensible à la casse.
    """
    mois = get_current_month_label()
    annee = str(get_current_year())

    # Patterns attendus dans le label (insensible à la casse)
    # "GARDE FEVRIER 2026" et "GARDE INTERIEUR FEVRIER 2026"
    pattern_principal = re.compile(
        rf"^\s*GARDE\s+{mois}\s+{annee}\s*$", re.IGNORECASE
    )
    pattern_interieur = re.compile(
        rf"^\s*GARDE\s+INTERIEUR\s+{mois}\s+{annee}\s*$", re.IGNORECASE
    )

    matched: List[PdfDoc] = []
    for pdf in pdfs:
        label = pdf.label.strip()
        if pattern_principal.match(label) or pattern_interieur.match(label):
            matched.append(pdf)

    logger.info(
        "Filtre mois courant (%s %s) : %d/%d PDF retenus",
        mois, annee, len(matched), len(pdfs),
    )
    if len(matched) < 2:
        logger.warning(
            "⚠ Seulement %d PDF trouvé(s) pour %s %s (attendu 2). "
            "L'article du mois n'est peut-être pas encore publié.",
            len(matched), mois, annee,
        )

    return matched


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class Article:
    """Un article découvert sur le site UNPPCI."""
    id: int
    url: str
    title: str
    is_garde: bool = False


@dataclass
class PdfDoc:
    """Un document PDF découvert."""
    url: str
    label: str
    article_id: Optional[int] = None
    article_title: Optional[str] = None
    is_garde: bool = False


# ---------------------------------------------------------------------------
# Session HTTP avec retry
# ---------------------------------------------------------------------------
def _build_session(*, verify_ssl: bool = True) -> requests.Session:
    """Crée une session requests avec retry automatique."""
    session = requests.Session()
    session.verify = verify_ssl
    retries = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Retourne une session partagée (singleton)."""
    global _session
    if _session is None:
        _session = _build_session()
    return _session


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
def _cache_path(url: str, ext: str = "html") -> Path:
    """Chemin de cache pour une URL donnée, par jour."""
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    today = date.today().isoformat()
    return CACHE_DIR / f"{today}_{url_hash}.{ext}"


# ---------------------------------------------------------------------------
# Fetch HTML
# ---------------------------------------------------------------------------
def fetch_html(url: str, *, use_cache: bool = False) -> str:
    """Récupère le HTML avec retry et cache optionnel."""
    if use_cache:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        p = _cache_path(url)
        if p.exists():
            logger.debug("Cache hit : %s", p.name)
            return p.read_text(encoding="utf-8")

    session = _get_session()
    logger.debug("GET %s", url)
    r = session.get(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            ),
            "Accept-Language": "fr-FR,fr;q=0.9",
        },
        timeout=30,
    )
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    if use_cache:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _cache_path(url).write_text(html, encoding="utf-8")

    return html


# ---------------------------------------------------------------------------
# Phase 1 : Découvrir les articles depuis la page catégorie (avec pagination)
# ---------------------------------------------------------------------------
def _extract_article_id(href: str) -> Optional[int]:
    """Extrait l'id d'article depuis une URL ?p=articles&id=XXX."""
    m = ARTICLE_ID_RE.search(href)
    return int(m.group(1)) if m else None


def discover_articles(
    *,
    use_cache: bool = False,
    max_pages: int = 3,
    garde_only: bool = True,
) -> List[Article]:
    """Scrape la page catégorie Actualités pour lister les articles.

    Gère la pagination (lien 'Plus d'articles' ou paramètre page).
    Filtre optionnellement les articles liés aux tours de garde.
    """
    articles: Dict[int, Article] = {}
    current_url: Optional[str] = UNPPCI_ARTICLES_CAT
    page = 0

    while current_url and page < max_pages:
        page += 1
        logger.info("Scan articles page %d : %s", page, current_url)
        html = fetch_html(current_url, use_cache=use_cache)
        soup = BeautifulSoup(html, "html.parser")

        # Extraire les liens d'articles
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            art_id = _extract_article_id(href)
            if art_id is None:
                continue

            title = " ".join(a_tag.get_text(" ", strip=True).split())
            if not title:
                continue

            is_garde = bool(GARDE_KEYWORDS_RE.search(title))

            if art_id not in articles:
                full_url = urljoin(UNPPCI_BASE, href)
                articles[art_id] = Article(
                    id=art_id,
                    url=full_url,
                    title=title,
                    is_garde=is_garde,
                )

        # Chercher le lien de pagination "Plus d'articles"
        next_url = None
        for link in soup.find_all("a", href=True):
            link_text = link.get_text(strip=True).lower()
            if "plus d" in link_text and "article" in link_text:
                next_url = urljoin(UNPPCI_BASE, link["href"].strip())
                break
        current_url = next_url

    # Trier par id décroissant (plus récent en premier)
    result = sorted(articles.values(), key=lambda a: a.id, reverse=True)

    if garde_only:
        result = [a for a in result if a.is_garde]

    logger.info(
        "Articles découverts : %d total, %d liés aux gardes",
        len(articles),
        sum(1 for a in articles.values() if a.is_garde),
    )
    return result


# ---------------------------------------------------------------------------
# Phase 2 : Extraire les PDF d'un article (href + JavaScript)
# ---------------------------------------------------------------------------
def _extract_pdfs_from_html(
    html: str,
    page_url: str,
    article: Optional[Article] = None,
) -> List[PdfDoc]:
    """Extrait les liens PDF depuis le HTML d'un article.

    Cherche dans (par ordre de priorité) :
    1. Les attributs onclick contenant controllers/downloads.php?id=XXX
    2. Les attributs href des balises <a> pointant vers /uploads/*.pdf
    3. Les URLs PDF dans le JavaScript embarqué
    4. Les data-attributes contenant des URLs PDF
    """
    soup = BeautifulSoup(html, "html.parser")
    found: Dict[str, PdfDoc] = {}

    # Identifier les PDF de la bannière (marquee) pour les exclure —
    # ils apparaissent sur toutes les pages et ne sont pas liés à l'article.
    banner_urls: set[str] = set()
    for marquee in soup.find_all("marquee"):
        for a_tag in marquee.find_all("a", href=True):
            href = a_tag["href"].strip()
            if PDF_RE.search(href):
                banner_urls.add(urljoin(page_url, href))

    def _add(url: str, label: str) -> None:
        if url not in found and url not in banner_urls:
            found[url] = PdfDoc(
                url=url,
                label=label,
                article_id=article.id if article else None,
                article_title=article.title if article else None,
            )

    # --- Méthode 1 (prioritaire) : onclick="window.open('controllers/downloads.php?id=972')" ---
    for a_tag in soup.find_all("a", onclick=True):
        onclick = a_tag["onclick"]
        m = ONCLICK_DL_RE.search(onclick)
        if m:
            dl_path = m.group(1)
            full = urljoin(page_url, dl_path)
            label = " ".join(a_tag.get_text(" ", strip=True).split()) or "pdf (download)"
            _add(full, label)
            logger.debug("    PDF via onclick : %s → %s", label, full)

    # --- Méthode 2 : liens <a href="...pdf"> (hors bannière) ---
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if PDF_RE.search(href):
            full = urljoin(page_url, href)
            label = " ".join(a_tag.get_text(" ", strip=True).split()) or "pdf"
            _add(full, label)

    # --- Méthode 3 : URLs PDF dans les blocs <script> ---
    for script in soup.find_all("script"):
        if script.string:
            for m_js in JS_PDF_RE.finditer(script.string):
                pdf_url = urljoin(page_url, m_js.group(1))
                _add(pdf_url, "pdf (via JS)")

    # --- Méthode 4 : data-attributes contenant des URLs PDF ---
    for tag in soup.find_all(True):
        for attr_name, attr_val in (tag.attrs or {}).items():
            if attr_name in ("href", "onclick"):
                continue  # déjà traité
            if isinstance(attr_val, str) and PDF_RE.search(attr_val):
                pdf_url = urljoin(page_url, attr_val)
                label = " ".join(tag.get_text(" ", strip=True).split()) or f"pdf ({attr_name})"
                _add(pdf_url, label)

    if banner_urls:
        logger.debug("    %d PDF de bannière exclus", len(banner_urls))

    return list(found.values())


def discover_pdfs_from_article(
    article: Article,
    *,
    use_cache: bool = False,
) -> List[PdfDoc]:
    """Récupère et extrait les PDF d'un article donné."""
    logger.info("  Scan article #%d : %s", article.id, article.title)
    html = fetch_html(article.url, use_cache=use_cache)
    pdfs = _extract_pdfs_from_html(html, article.url, article)

    # Filtrer les PDF liés aux gardes (par label ou par contexte article)
    if article.is_garde:
        # Si l'article est un "tour de garde", tous ses PDFs sont pertinents
        for pdf in pdfs:
            pdf.is_garde = True
    else:
        # Sinon, filtrer par label
        for pdf in pdfs:
            pdf.is_garde = bool(GARDE_KEYWORDS_RE.search(pdf.label))

    garde_count = sum(1 for p in pdfs if p.is_garde)
    logger.info("    → %d PDF trouvés (%d liés aux gardes)", len(pdfs), garde_count)
    return pdfs


# ---------------------------------------------------------------------------
# Phase 3 : Téléchargement des PDF
# ---------------------------------------------------------------------------
def download_pdf(pdf: PdfDoc, *, use_cache: bool = False) -> Optional[Path]:
    """Télécharge un PDF et le sauvegarde localement.

    Retourne le chemin du fichier ou None en cas d'erreur.
    """
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Nom de fichier basé sur l'URL
    url_path = urlparse(pdf.url).path
    filename = Path(url_path).name or "unknown.pdf"

    # Pour les downloads.php?id=XXX, utiliser l'id comme nom
    if "downloads.php" in pdf.url:
        qs = parse_qs(urlparse(pdf.url).query)
        dl_id = qs.get("id", ["unknown"])[0]
        # Utiliser le label nettoyé comme nom de fichier
        safe_label = re.sub(r"[^\w\s-]", "", pdf.label).strip().replace(" ", "_")
        filename = f"dl{dl_id}_{safe_label}.pdf" if safe_label else f"dl{dl_id}.pdf"

    # Préfixer par l'id d'article si disponible
    if pdf.article_id:
        dest = DOWNLOAD_DIR / f"art{pdf.article_id}_{filename}"
    else:
        dest = DOWNLOAD_DIR / filename

    # Cache : ne pas re-télécharger si le fichier existe déjà
    if use_cache and dest.exists() and dest.stat().st_size > 0:
        logger.debug("  PDF déjà téléchargé : %s", dest.name)
        return dest

    # Session dédiée au téléchargement : SSL désactivé car le serveur UNPPCI
    # coupe fréquemment la connexion SSL lors du téléchargement de fichiers.
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    dl_session = _build_session(verify_ssl=False)

    try:
        logger.info("  Téléchargement : %s", pdf.url)
        r = dl_session.get(
            pdf.url,
            timeout=120,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
                ),
            },
        )
        r.raise_for_status()

        # Vérifier que c'est bien un PDF (ou au minimum du contenu binaire)
        content_type = r.headers.get("Content-Type", "")
        is_pdf = "pdf" in content_type.lower() or r.content[:5] == b"%PDF-"
        is_binary = "octet-stream" in content_type.lower() or len(r.content) > 1000

        if not is_pdf and not is_binary:
            logger.warning("  ⚠ Pas un PDF (%s, %d octets) : %s", content_type, len(r.content), pdf.url)
            return None

        dest.write_bytes(r.content)
        logger.info("  ✅ Sauvegardé : %s (%.1f Ko)", dest.name, len(r.content) / 1024)
        return dest

    except Exception as exc:
        logger.error("  Erreur téléchargement %s : %s", pdf.url, exc)
        return None


# ---------------------------------------------------------------------------
# Pipeline complet
# ---------------------------------------------------------------------------
def run_discovery(
    *,
    use_cache: bool = False,
    download: bool = False,
    current_month_only: bool = False,
    max_articles: int = 3,
    max_pages: int = 3,
    garde_only: bool = True,
) -> List[PdfDoc]:
    """Pipeline complet : découverte articles → extraction PDF → téléchargement.

    Args:
        use_cache: Utiliser le cache HTML/PDF local.
        download: Télécharger les PDF trouvés.
        current_month_only: Ne garder que les 2 PDF du mois courant
            (GARDE + GARDE INTERIEUR).
        max_articles: Nombre max d'articles à scanner.
        max_pages: Nombre max de pages de listing à parcourir.
        garde_only: Ne garder que les PDF liés aux tours de garde.

    Returns:
        Liste des PdfDoc découverts.
    """
    t_start = time.monotonic()

    # 1) Découvrir les articles
    articles = discover_articles(
        use_cache=use_cache,
        max_pages=max_pages,
        garde_only=garde_only,
    )
    if not articles:
        logger.warning("Aucun article de garde trouvé")
        return []

    # Limiter le nombre d'articles à scanner
    articles = articles[:max_articles]
    logger.info("Articles à scanner : %d", len(articles))
    for a in articles:
        logger.info("  #%d : %s", a.id, a.title)

    # 2) Extraire les PDF de chaque article
    all_pdfs: List[PdfDoc] = []
    seen_urls: set[str] = set()

    for article in articles:
        pdfs = discover_pdfs_from_article(article, use_cache=use_cache)
        for pdf in pdfs:
            if garde_only and not pdf.is_garde:
                continue
            if pdf.url not in seen_urls:
                seen_urls.add(pdf.url)
                all_pdfs.append(pdf)

    logger.info("Total PDF uniques : %d", len(all_pdfs))

    # 3) Filtre mois courant (si activé)
    if current_month_only:
        all_pdfs = filter_pdfs_current_month(all_pdfs)

    # 4) Télécharger si demandé
    if download and all_pdfs:
        logger.info("Téléchargement des PDF...")
        for pdf in all_pdfs:
            download_pdf(pdf, use_cache=use_cache)

    elapsed = time.monotonic() - t_start
    logger.info("Terminé en %.1fs — %d PDF découverts", elapsed, len(all_pdfs))
    return all_pdfs


# ---------------------------------------------------------------------------
# Point d'entrée CLI
# ---------------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(
        description="Découvre et télécharge les PDF de garde depuis unppci.org"
    )
    parser.add_argument(
        "--cache", action="store_true",
        help="Utiliser le cache local (HTML + PDF)",
    )
    parser.add_argument(
        "--download", action="store_true",
        help="Télécharger les PDF trouvés dans etl/downloads_unppci/",
    )
    parser.add_argument(
        "--max-articles", type=int, default=3,
        help="Nombre max d'articles à scanner (défaut: 3)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=3,
        help="Nombre max de pages de listing (défaut: 3)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Inclure tous les PDF (pas seulement les gardes)",
    )
    parser.add_argument(
        "--current-month", action="store_true",
        help=(
            "Ne télécharger que les 2 PDF du mois courant : "
            "GARDE {MOIS} et GARDE INTERIEUR {MOIS}"
        ),
    )
    args = parser.parse_args()

    if args.current_month:
        mois = get_current_month_label()
        annee = get_current_year()
        logger.info("Mode mois courant : %s %d", mois, annee)

    pdfs = run_discovery(
        use_cache=args.cache,
        download=args.download,
        current_month_only=args.current_month,
        max_articles=args.max_articles,
        max_pages=args.max_pages,
        garde_only=not args.all,
    )

    # Affichage résumé sur stdout
    sys.stdout.reconfigure(encoding="utf-8")
    print()
    for p in pdfs:
        tag = "[GARDE]" if p.is_garde else "[AUTRE]"
        art = f"(art #{p.article_id})" if p.article_id else ""
        print(f"  {tag} {p.label} {art}")
        print(f"        {p.url}")
    print(f"\n  Total : {len(pdfs)} PDF(s)")


if __name__ == "__main__":
    main()
