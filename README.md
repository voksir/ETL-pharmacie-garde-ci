# Pharmacie de Garde CI

Pipeline ETL (Extract, Transform, Load) pour collecter, normaliser et centraliser les données des **pharmacies de garde en Cote d'Ivoire** dans une base Supabase.

---

## Sommaire

- [Apercu du projet](#apercu-du-projet)
- [Architecture](#architecture)
- [Sources de donnees](#sources-de-donnees)
- [Structure du projet](#structure-du-projet)
- [Installation](#installation)
- [Configuration](#configuration)
- [Utilisation](#utilisation)
  - [Source 1 : AnnuaireCI](#source-1--annuaireci)
  - [Source 2 : UNPPCI](#source-2--unppci)
- [Schema de la base de donnees](#schema-de-la-base-de-donnees)
- [Details techniques](#details-techniques)
- [Options CLI](#options-cli)
- [Gestion des erreurs et robustesse](#gestion-des-erreurs-et-robustesse)
- [Developpement](#developpement)

---

## Apercu du projet

Ce projet automatise la collecte des tours de garde des pharmacies ivoiriennes a partir de deux sources officielles :

1. **annuaireci.com** - Annuaire en ligne (scraping HTML)
2. **unppci.org** - Union Nationale des Pharmaciens Prive de Cote d'Ivoire (parsing de PDF)

Les donnees sont normalisees, dedupliquees et chargees dans une base **Supabase** (PostgreSQL) pour alimenter une application SaaS.

### Fonctionnalites principales

- Extraction automatisee depuis 2 sources distinctes
- Normalisation des noms, adresses et numeros de telephone (format E.164)
- Cles d'idempotence SHA-1 pour eviter les doublons (UPSERT)
- Cache local pour reduire les requetes HTTP repetees
- Retry automatique avec backoff exponentiel (HTTP + Supabase)
- Logging structure pour le monitoring et le debug
- Interface CLI flexible avec arguments configurables

---

## Architecture

```
                    +-------------------+
                    |  annuaireci.com   |
                    |   (page HTML)     |
                    +--------+----------+
                             |
                    annuaireci_scrape.py
                             |
                    annuaireci_week.json
                             |
                load_annuaireci_to_supabase.py
                             |
                             v
                    +-------------------+
                    |     SUPABASE      |
                    |  +-----------+    |
                    |  | pharmacies|    |
                    |  +-----------+    |
                    |  |duty_periods|   |
                    |  +-----------+    |
                    +--------+----------+
                             ^
                             |
                 load_unppci_to_supabase.py
                             |
              +--------------+--------------+
              |                             |
     unppci_discover.py          unppci_parse_pdf.py
              |                             |
     +--------+----------+        (parsing des PDF)
     | unppci.org         |
     | (articles + PDF)   |
     +--------------------+
```

---

## Sources de donnees

### 1. AnnuaireCI (`annuaireci.com`)

| Caracteristique | Detail |
|-----------------|--------|
| URL | `https://annuaireci.com/pharmacies-de-garde/` |
| Format | Page HTML avec balises `<h3>` (zones) et `<h4>` (pharmacies) |
| Frequence | Hebdomadaire (Semaine du DD/MM/YYYY au DD/MM/YYYY) |
| Couverture | Tout le territoire ivoirien |
| Donnees extraites | Nom, adresse, telephones, zone geographique |

### 2. UNPPCI (`unppci.org`)

| Caracteristique | Detail |
|-----------------|--------|
| URL | `https://www.unppci.org/?cat=1&rw=actualites` |
| Format | Articles avec PDF joints (via `onclick` JavaScript) |
| Frequence | Mensuelle (PDF par mois avec 4-5 semaines) |
| Couverture | Abidjan + Interieur du pays |
| Documents | 2 PDF/mois : `GARDE {MOIS}` (Abidjan) + `GARDE INTERIEUR {MOIS}` |

---

## Structure du projet

```
pharmacie-garde-ci/
|-- .env                              # Variables d'environnement (Supabase)
|-- .gitignore                        # Fichiers exclus du versionnage
|-- README.md                         # Documentation (ce fichier)
|
|-- etl/
    |-- annuaireci_scrape.py          # [E] Scraping HTML annuaireci.com
    |-- annuaireci_week.json          # Sortie JSON du scraper (intermediaire)
    |-- load_annuaireci_to_supabase.py # [L] Chargement AnnuaireCI -> Supabase
    |
    |-- unppci_discover.py            # [E] Decouverte et telechargement des PDF UNPPCI
    |-- unppci_parse_pdf.py           # [T] Parsing des PDF en donnees structurees
    |-- load_unppci_to_supabase.py    # [L] Pipeline complet UNPPCI -> Supabase
    |
    |-- db_helpers.py                 # Module partage : normalisation, cles, upsert
    |
    |-- .cache/                       # Cache HTML annuaireci (genere, ignore par git)
    |-- .cache_unppci/                # Cache HTML unppci (genere, ignore par git)
    |-- downloads_unppci/             # PDF telecharges (genere, ignore par git)
```

### Description des modules (2 293 lignes de code Python)

| Module | Lignes | Role |
|--------|--------|------|
| `annuaireci_scrape.py` | 441 | Scraping HTML, extraction pharmacies/zones/telephones |
| `load_annuaireci_to_supabase.py` | 191 | Normalisation JSON -> upsert Supabase |
| `unppci_discover.py` | 610 | Decouverte articles, extraction liens PDF, telechargement |
| `unppci_parse_pdf.py` | 527 | Parsing PDF multi-formats (Abidjan + Interieur) |
| `load_unppci_to_supabase.py` | 349 | Pipeline complet decouverte -> parsing -> chargement |
| `db_helpers.py` | 175 | Fonctions partagees (normalisation, cles SHA-1, retry) |

---

## Installation

### Prerequis

- Python 3.10+
- Un compte [Supabase](https://supabase.com/) avec les tables configurees

### Installation des dependances

```bash
pip install requests beautifulsoup4 python-dateutil pdfplumber python-dotenv supabase unidecode
```

### Liste des packages Python

| Package | Usage |
|---------|-------|
| `requests` | Requetes HTTP avec retry |
| `beautifulsoup4` | Parsing HTML (annuaireci + unppci) |
| `python-dateutil` | Parsing de dates flexibles |
| `pdfplumber` | Extraction de texte depuis les PDF |
| `python-dotenv` | Chargement des variables `.env` |
| `supabase` | Client Python pour Supabase (upsert, select) |
| `unidecode` | Translitteration Unicode -> ASCII |
| `urllib3` | Gestion des retries HTTP |

---

## Configuration

### Variables d'environnement

Creer un fichier `.env` a la racine du projet :

```env
SUPABASE_URL=https://votre-projet.supabase.co
SUPABASE_SERVICE_ROLE_KEY=votre_cle_service_role
```

> **Securite** : Le fichier `.env` est exclu du versionnage via `.gitignore`. Ne committez jamais vos cles.

---

## Utilisation

### Source 1 : AnnuaireCI

#### Etape 1 - Scraping

```bash
cd etl
python annuaireci_scrape.py
```

Genere le fichier `annuaireci_week.json` contenant les pharmacies de garde de la semaine en cours.

**Options :**
```bash
python annuaireci_scrape.py --cache           # Utiliser le cache HTML local
python annuaireci_scrape.py --validate-only   # Verifier la structure HTML sans parser
```

**Exemple de sortie JSON :**
```json
{
  "source": "annuaireci",
  "source_url": "https://annuaireci.com/pharmacies-de-garde/",
  "week_start": "2026-02-14",
  "week_end": "2026-02-20",
  "areas": [
    {
      "area": "ABENGOUROU",
      "pharmacies": [
        {
          "name_raw": "Pharmacie Belle Fontaine (Grde)",
          "address_raw": "QUARTIER HKB",
          "phones_raw": ["0769353909", "0708629483"]
        }
      ]
    }
  ],
  "scraped_at": "2026-02-15"
}
```

#### Etape 2 - Chargement Supabase

```bash
python load_annuaireci_to_supabase.py
```

Lit `annuaireci_week.json`, normalise les donnees et les upsert dans Supabase.

---

### Source 2 : UNPPCI

#### Option A - Pipeline complet (recommande)

```bash
cd etl
python load_unppci_to_supabase.py
```

Execute automatiquement toute la chaine :
1. Decouverte des articles sur unppci.org
2. Filtrage des PDF du mois courant
3. Telechargement des PDF
4. Parsing (extraction pharmacies, zones, telephones)
5. Chargement dans Supabase

**Options :**
```bash
python load_unppci_to_supabase.py --no-cache       # Desactiver le cache
python load_unppci_to_supabase.py --all-months      # Tous les mois (pas seulement le courant)
python load_unppci_to_supabase.py --max-articles 10 # Scanner plus d'articles
python load_unppci_to_supabase.py --force            # Re-ingerer meme si deja charge
```

#### Option B - Etapes individuelles

```bash
# 1. Decouvrir et telecharger les PDF
python unppci_discover.py --download --current-month

# 2. Parser un PDF specifique
python unppci_parse_pdf.py chemin/vers/le.pdf

# 3. Parser avec sortie JSON
python unppci_parse_pdf.py --output resultat.json
```

---

## Schema de la base de donnees

### Table `pharmacies`

| Colonne | Type | Description |
|---------|------|-------------|
| `id` | UUID | Identifiant unique (genere par Supabase) |
| `pharmacy_key` | TEXT (unique) | Cle SHA-1 d'idempotence (ville + nom) |
| `name_raw` | TEXT | Nom brut extrait de la source |
| `name_norm` | TEXT | Nom normalise (sans accents, minuscules) |
| `address_raw` | TEXT | Adresse brute |
| `address_norm` | TEXT | Adresse normalisee |
| `area_raw` | TEXT | Zone geographique brute |
| `city_norm` | TEXT | Ville normalisee |
| `sector` | INTEGER | Numero de secteur (Yopougon, Abobo, etc.) |
| `phones_raw` | JSONB | Telephones bruts (tableau) |
| `phones_e164` | JSONB | Telephones au format E.164 (tableau) |
| `source_last` | TEXT | Derniere source (`annuaireci` ou `unppci`) |
| `source_url_last` | TEXT | URL de la derniere source |
| `updated_at` | TIMESTAMPTZ | Date de derniere mise a jour |

### Table `duty_periods`

| Colonne | Type | Description |
|---------|------|-------------|
| `id` | UUID | Identifiant unique |
| `duty_key` | TEXT (unique) | Cle SHA-1 (pharmacy_key + dates + source) |
| `pharmacy_id` | UUID (FK) | Reference vers `pharmacies.id` |
| `start_date` | DATE | Debut de la periode de garde |
| `end_date` | DATE | Fin de la periode de garde |
| `source` | TEXT | Source des donnees |
| `source_url` | TEXT | URL d'origine |
| `scraped_at` | TIMESTAMPTZ | Horodatage de l'extraction |

---

## Details techniques

### Normalisation des donnees

#### Texte (`norm_text`)
```
"Pharmacie de l'Esperance" -> "pharmacie de l esperance"
```
- Suppression des accents (`unidecode`)
- Mise en minuscules
- Conservation uniquement des caracteres alphanumeriques

#### Zones geographiques (`parse_area`)
```
"YOPOUGON Secteur 9" -> city_norm="yopougon", sector=9
"ABENGOUROU"         -> city_norm="abengourou", sector=None
```

#### Telephones (`phones_to_e164_ci`)
```
"0769353909"  -> "+2250769353909"   (10 chiffres : format actuel)
"27123456"    -> "+22501271234 56"  (8 chiffres : ancien format pre-2021)
```
- Format E.164 international (`+225...`)
- Support des numeros 8 et 10 chiffres
- Deduplication automatique

### Cles d'idempotence (SHA-1)

Les cles garantissent l'idempotence des operations UPSERT :

- **`pharmacy_key`** = SHA-1(`city_norm | name_norm`)
  - Stable meme si l'adresse ou le telephone change entre deux extractions
- **`duty_key`** = SHA-1(`pharmacy_key | start_date | end_date | source`)
  - Unique par pharmacie, periode et source

### Parsing des PDF UNPPCI

Le parser gere deux formats distincts de PDF :

#### Format Abidjan
```
YOPOUGON SECTEUR 1          <- Zone geographique (ligne seule, majuscules)
PHCIE DE L'ESPERANCE         <- Pharmacie
Av. de la Paix               <- Adresse
07 69 35 39 09               <- Telephone
```

#### Format Interieur
```
ABENGOUROU PHCIE DU MARCHE / MME KONAN  <- Ville + pharmacie sur la meme ligne
```

#### Formats de dates geres
```
Format A : "SEMAINE DU SAMEDI 07 AU VENDREDI 13 FEVRIER 2026"
Format B : "SEMAINE DU SAMEDI 28 FEVRIER AU VENDREDI 06 MARS 2026"
Format C : "SEMAINE DU SAMEDI 02 MARS 2019 AU VENDREDI 08 MARS 2019"
```

### Detection automatique du mois courant

Le module `unppci_discover.py` detecte automatiquement le mois en cours et filtre les PDF pour ne telecharger que les 2 pertinents :
- `GARDE {MOIS} {ANNEE}` (Abidjan et communes principales)
- `GARDE INTERIEUR {MOIS} {ANNEE}` (villes de l'interieur)

---

## Options CLI

### `annuaireci_scrape.py`

| Option | Description |
|--------|-------------|
| `--cache` | Utiliser le cache HTML local (dev/debug) |
| `--validate-only` | Valider la structure HTML sans extraction |

### `unppci_discover.py`

| Option | Description |
|--------|-------------|
| `--cache` | Utiliser le cache local (HTML + PDF) |
| `--download` | Telecharger les PDF trouves |
| `--max-articles N` | Nombre max d'articles a scanner (defaut: 3) |
| `--max-pages N` | Nombre max de pages de listing (defaut: 3) |
| `--all` | Inclure tous les PDF (pas seulement les gardes) |
| `--current-month` | Ne telecharger que les 2 PDF du mois courant |

### `unppci_parse_pdf.py`

| Option | Description |
|--------|-------------|
| `pdf` | Chemin vers le PDF (auto-detecte si omis) |
| `--source-url URL` | URL d'origine pour tracabilite |
| `--output / -o FILE` | Fichier JSON de sortie (defaut: stdout) |

### `load_unppci_to_supabase.py`

| Option | Description |
|--------|-------------|
| `--no-cache` | Desactiver le cache HTTP et PDF |
| `--all-months` | Charger tous les mois (pas seulement le courant) |
| `--max-articles N` | Nombre max d'articles a scanner (defaut: 5) |
| `--force` | Re-ingerer meme si le PDF semble deja charge |

---

## Gestion des erreurs et robustesse

### Retry HTTP
- **3-4 tentatives** avec backoff exponentiel (0s, 1s, 2s, 4s)
- Codes HTTP reessayes : 429, 500, 502, 503, 504
- Session dediee avec SSL desactive pour UNPPCI (serveur instable)

### Retry Supabase
- **3 tentatives** avec backoff exponentiel (2s, 4s, 8s)
- Upsert par chunks de 200 (pharmacies) ou 500 (duty_periods)
- Erreurs loguees avec detail du chunk concerne

### Cache local
- Cache HTML par URL et par jour (hash MD5 tronque)
- Cache PDF par nom de fichier
- Repertoires : `etl/.cache/`, `etl/.cache_unppci/`, `etl/downloads_unppci/`

### Validation de structure HTML
- Verification des marqueurs attendus sur annuaireci.com
- Alertes si la structure change (nombre de balises `<h3>`, `<h4>`)
- Mode `--validate-only` pour le monitoring

### Encodage
- Decodage force en UTF-8 des reponses HTTP (`errors="replace"`)
- Ecriture directe des fichiers JSON en UTF-8 (contourne les problemes Windows GBK)
- Verification post-ecriture des caracteres mojibake residuels

### Logging
- Logs structures sur `stderr` avec horodatage et niveau
- Format : `YYYY-MM-DD HH:MM:SS [LEVEL] module: message`
- Classification des lignes parsees dans les PDF (debug)

---

## Developpement

### Lancer en mode debug

```bash
# Scraping avec cache (evite de solliciter le site)
python annuaireci_scrape.py --cache

# Decouverte UNPPCI avec cache
python unppci_discover.py --cache --download --current-month

# Parser un PDF local
python unppci_parse_pdf.py downloads_unppci/mon_fichier.pdf -o resultat.json
```

### Executer le pipeline complet

```bash
cd etl

# Source 1 : AnnuaireCI
python annuaireci_scrape.py
python load_annuaireci_to_supabase.py

# Source 2 : UNPPCI
python load_unppci_to_supabase.py
```

### Environnement virtuel recommande

```bash
python -m venv .venv
.venv\Scripts\activate       # Windows
source .venv/bin/activate    # macOS/Linux

pip install requests beautifulsoup4 python-dateutil pdfplumber python-dotenv supabase unidecode
```

---

## Licence

Projet prive - Tous droits reserves.
