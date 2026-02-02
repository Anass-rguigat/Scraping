# Project Extractor — CRI Projects (Fès‑Meknès + Béni Mellal‑Khénifra)

Ce dossier contient deux scripts Python qui extraient des projets d’investissement depuis des PDFs CRI et les normalisent dans un fichier unique `output_projects.csv`.

## Vue d’ensemble

- **`cri_fes_mekness.py`**
  - Traite des **PDFs multi‑pages** (plusieurs projets par PDF) dans `fes-mekness/`.
  - Extrait par règles **déterministes** (regex + calculs simples).
  - Génère `output_projects.csv` (écriture complète du fichier).

- **`cri_benimallal.py`**
  - Scrape `coeurdumaroc.ma` pour récupérer **tous les PDFs** “Banque de projets BK”.
  - Télécharge les PDFs dans `banque_projets_bk/`.
  - Extrait **1 projet par PDF** (texte + chiffres).
  - Met à jour `output_projects.csv` en **remplaçant uniquement** les lignes de source Béni Mellal, **sans supprimer** les lignes générées par `cri_fes_mekness.py`.

## Prérequis

- Python 3.10+ recommandé
- Dépendances Python:
  - `pandas`
  - `pdfplumber`
  - `requests`
  - `beautifulsoup4`

Installation (exemple):

```bash
pip install pandas pdfplumber requests beautifulsoup4
```

## Fichiers et dossiers

- **`output_projects.csv`**: sortie consolidée (toutes les régions/sources)
- **`fes-mekness/`**: PDFs d’entrée pour Fès‑Meknès (multi‑pages)
- **`banque_projets_bk/`**: PDFs téléchargés pour Béni Mellal‑Khénifra

## Format du CSV (colonnes)

Les scripts remplissent le CSV avec les colonnes suivantes:

- `project_id`
- `project_reference`
- `project_title`
- `project_description`
- `sector`
- `sub_sector`
- `project_bank_category`
- `is_project_bank`
- `region`
- `province`
- `industrial_zone`
- `estimated_investment_mad`
- `min_investment_mad`
- `investment_range`
- `payback_period_years`
- `roi_estimated`
- `required_land_area_m2`
- `required_building_area_m2`
- `has_pdf`
- `pdf_url`
- `pdf_page_number`
- `publication_date`
- `last_update`
- `language`
- `currency`
- `source_type`

## Dictionnaire des champs (explication complète)

Cette section documente **chaque attribut** (colonne CSV): définition, unité, origine (PDF/web), règles de génération et calculs.

### Identifiants & traçabilité

- **`project_id`**
  - **Définition**: identifiant numérique unique dans le CSV.
  - **Origine**:
    - Fès‑Meknès: compteur interne (`global_index`) lors de l’extraction.
    - Béni Mellal: généré à partir de `max(project_id)` déjà présent + 1, pour éviter les collisions avec d’autres sources.
  - **Règle**: doit être **unique**.

- **`project_reference`**
  - **Définition**: identifiant texte stable (utilisable comme “slug”).
  - **Génération**: concaténation normalisée:
    - Fès‑Meknès: `REGION-NORMALIZED(TITLE)-project_id`
    - Béni Mellal: `BENI-MELLAL-KHENIFRA-NORMALIZED(TITLE)-project_id`
  - **Normalisation**:
    - suppression des accents
    - majuscules
    - espaces → `-`
    - suppression ponctuation non alphanumérique (hors `_` et `-`)
  - **Règle**: doit être **unique**.

- **`has_pdf`**
  - **Définition**: indique que l’enregistrement provient d’un PDF.
  - **Valeur**: `True` (toujours vrai dans ces pipelines).

- **`pdf_url`**
  - **Définition**: chemin absolu du PDF sur la machine.
  - **Valeur**:
    - Fès‑Meknès: chemin du PDF source (dans `fes-mekness/`).
    - Béni Mellal: chemin du PDF téléchargé (dans `banque_projets_bk/`).
  - **Remarque**: ce n’est pas une URL web, c’est un chemin local.

- **`pdf_page_number`**
  - **Définition**: page d’origine de l’extraction.
  - **Valeur**:
    - Fès‑Meknès: page de début du projet (1‑based).
    - Béni Mellal: `1` (un projet par PDF).

- **`source_type`**
  - **Définition**: libellé de provenance (source régionale).
  - **Valeurs typiques**:
    - `CRI Fès-Meknès`
    - `CRI Béni Mellal-Khénifra`
  - **Usage**: clé de “refresh” côté Béni Mellal (remplacer uniquement cette source).

### Contenu projet (texte)

- **`project_title`**
  - **Définition**: titre du projet.
  - **Origine**:
    - Fès‑Meknès: extrait après `PROJET N° ...`.
    - Béni Mellal: extrait après `Projet N° ...` (ou fallback sur nom de fichier).
  - **Nettoyage**: suppression des doublons (certains PDFs répètent le titre deux fois).

- **`project_description`**
  - **Définition**: résumé textuel (description du projet).
  - **Origine**:
    - Fès‑Meknès: extraction “layout” (colonne gauche) quand disponible, sinon bloc `DESCRIPTION...INDICATEURS`.
    - Béni Mellal: extraction depuis `DESCRIPTION DU PROJET` ou un paragraphe narratif.
  - **Règle d’arrêt** (Béni Mellal): stop dès qu’on atteint des sections techniques (ex: `PROGRAMME`, `SUPERFICIE`, `PRODUIT`, `CAPACITÉ`, `EMPLOIS`, `TRI`, `PBP`, etc.) pour éviter d’embarquer tout le PDF.

### Classification (secteur / filière)

- **`sector`**
  - **Définition**: secteur économique principal.
  - **Origine**:
    - Fès‑Meknès: champ `FILIÈRE` du PDF (ou fallback via nom de fichier).
    - Béni Mellal: champ `Secteur économique :`.
  - **Nettoyage**: suppression du préfixe (`Secteur économique:`).

- **`sub_sector`**
  - **Définition**: sous-secteur / filière de production.
  - **Origine**:
    - Fès‑Meknès: `SOUS-FILIÈRE` du PDF.
    - Béni Mellal: `Filières de production :`.
  - **Nettoyage**: suppression du préfixe (`Filières de production:`).

- **`project_bank_category`**
  - **Définition**: catégorie utilisée pour regrouper dans la “banque de projets”.
  - **Règle**: égal à `sector` (dans les deux scripts).

- **`is_project_bank`**
  - **Définition**: drapeau “ce projet appartient à la banque”.
  - **Valeur**: `True`.

### Géographie

- **`region`**
  - **Définition**: région administrative.
  - **Valeur**:
    - Fès‑Meknès: détectée dans le PDF (ou fixée à `Fès-Meknès`).
    - Béni Mellal: fixée à `Béni Mellal-Khénifra`.

- **`province`**
  - **Définition**: province (ou localisation courte si province non explicitement listée).
  - **Béni Mellal (règle)**:
    - si une province connue est trouvée dans le texte → on la retourne (`Béni Mellal`, `Azilal`, `Fquih Ben Salah`, `Khénifra`, `Khouribga`)
    - sinon extraction de `Lieu:` (si le contenu est court et non bruité)

- **`industrial_zone`**
  - **Définition**: zones industrielles / sites d’implantation listés (ZI/ZAE/Agropole…).
  - **Béni Mellal (règle)**:
    - extraction de motifs: `Agro‑pôle/Agropole`, `Future ZI ...`, `ZI ...`, `ZAE ...`
    - nettoyage “anti-bruit” (ne pas inclure les intrants/équipements)
    - format final: liste jointe par `; `

### Finances & surfaces (numérique)

- **`currency`**
  - **Définition**: devise des montants financiers.
  - **Valeur**: `MAD`.

- **`estimated_investment_mad`**
  - **Définition**: investissement estimé converti en **MAD**.
  - **Béni Mellal (sources possibles)**:
    - `MDH/MDHS` (millions de dirhams): `X MDH` ou `X-Y MDHS`
    - `Mn MDHS` / `Mns MDHS` (variantes typographiques): traité comme `MDHS`
    - `DHS/DH` (dirhams): montant direct en MAD
    - `KDHS` (milliers de dirhams): converti en MAD via `* 1000`
  - **Règle intervalle**: si `A-B` → moyenne `(A+B)/2`.

- **`min_investment_mad`**
  - **Définition**: estimation “minimum” utilisée pour filtrage/reco.
  - **Calcul**: `estimated_investment_mad * 0.8`.

- **`investment_range`**
  - **Définition**: classe d’investissement.
  - **Calcul** (seuils dans le script):
    - `Low` si `< 5,000,000`
    - `Medium` si `<= 20,000,000`
    - `High` si `> 20,000,000`

- **`payback_period_years`**
  - **Définition**: période de retour (années).
  - **Sources possibles**:
    - `PBP : A-B ans` ou `PBP : A ans` → moyenne si intervalle
    - certains PDFs BK utilisent `ROI : 4-5 ans` (ici “ROI” = payback en années) → pris comme payback
  - **Normalisations**: corrections sur erreurs OCR fréquentes (`PB0P` → `PBP`).

- **`roi_estimated`**
  - **Définition**: rendement estimé en pourcentage (quand c’est un TRI) ou estimé depuis le payback.
  - **Sources possibles**:
    - `TRI : x-y%` ou `TRI : x%` → moyenne si intervalle
  - **Complétion déterministe** (utile pour système de recommandation):
    - si TRI absent mais `payback_period_years` présent → `roi_estimated = 100 / payback_period_years`
    - si payback absent mais TRI présent → `payback_period_years = 100 / roi_estimated`

- **`required_land_area_m2`**
  - **Définition**: surface foncière nécessaire (m²).
  - **Sources possibles**:
    - `Sup : A m²`, `Sup : A m2`
    - `Sup : A à B m²` (ou `A m² à B m²`) → moyenne
    - `Sup : A-B m2` → moyenne
    - `Sup : A-B Ha` → converti en m² via `Ha * 10,000`
    - `Sup : A Ha` → converti en m²

- **`required_building_area_m2`**
  - **Définition**: surface bâtie requise (m²).
  - **État actuel**:
    - Fès‑Meknès: extrait si présent.
    - Béni Mellal: `NULL` (non disponible de façon fiable dans les PDFs BK).

### Dates & métadonnées

- **`publication_date`**
  - **Définition**: date de publication du document.
  - **Valeur**:
    - Fès‑Meknès: détectée depuis la couverture ou le nom du fichier.
    - Béni Mellal: `NULL` (non stable/fiable dans les PDFs BK actuels).

- **`last_update`**
  - **Définition**: date de dernière mise à jour.
  - **Valeur**:
    - Fès‑Meknès: égal à `publication_date`.
    - Béni Mellal: `NULL` (non stable/fiable dans les PDFs BK actuels).

- **`language`**
  - **Définition**: langue du contenu.
  - **Valeur**: `FR`.

## Règles & conditions (ce qui est “calculé” ou “conditionnel”)

### Calculs financiers

- **Range**: `investment_range` est dérivé de `estimated_investment_mad` via des seuils fixes.
- **Min**: `min_investment_mad = 0.8 * estimated_investment_mad`.
- **ROI vs PBP**:
  - si **TRI** est présent → `roi_estimated` vient du PDF.
  - si **TRI** absent mais **PBP** présent → `roi_estimated = 100/PBP` (complétion déterministe).
  - si **PBP** absent mais **TRI** présent → `payback_period_years = 100/TRI` (complétion déterministe).

### Conversions d’unités (Béni Mellal)

- `MDH/MDHS` → MAD via `* 1_000_000`
- `KDHS` → MAD via `* 1_000`
- `DHS/DH` → déjà en MAD
- `Ha` → m² via `* 10_000`

### Conditions de “refresh” CSV (Béni Mellal)

- On supprime uniquement les lignes existantes dont `source_type == "CRI Béni Mellal-Khénifra"`.
- On conserve toutes les autres sources (ex. `CRI Fès-Meknès`).
- On ré-écrit le fichier final (kept + new).

## Exécution

### 1) Fès‑Meknès

Depuis le dossier `project_extractor/`:

```bash
python cri_fes_mekness.py
```

Ce script **ré-écrit** `output_projects.csv` avec les projets Fès‑Meknès issus des PDFs listés dans `PDF_FILENAMES`.

### 2) Béni Mellal‑Khénifra

```bash
python cri_benimallal.py
```

Comportement:

- Si `banque_projets_bk/` est absent ou vide, le script télécharge les PDFs depuis `https://coeurdumaroc.ma/fr/projects`.
- Il extrait les attributs depuis chaque PDF (jusqu’à 2 pages par PDF).
- Il met à jour `output_projects.csv` en conservant les autres sources.

## Règles importantes (cohabitation des deux scripts)

### `cri_fes_mekness.py` (overwrite)

- Produit un CSV complet basé sur ses PDFs d’entrée.
- Si tu veux **conserver** aussi Béni Mellal, exécute ensuite `cri_benimallal.py` pour ré‑injecter les lignes BK.

### `cri_benimallal.py` (refresh “par source_type”)

- Lit `output_projects.csv`
- Garde toutes les lignes dont `source_type != "CRI Béni Mellal-Khénifra"`
- Recalcule les 200 lignes BK
- Ré-écrit le CSV final (lignes autres sources + nouvelles lignes BK)

## Notes sur l’extraction (logique)

### `cri_fes_mekness.py`

- Extraction “project-based”:
  - lecture page par page
  - détection des pages début de projet
  - construction d’un bloc texte par projet (page début + page suivante)
- Règles déterministes:
  - regex pour `FILIÈRE`, `SOUS-FILIÈRE`, investissement, superficie, ROI/payback
  - calculs simples:
    - `min_investment_mad = estimated_investment_mad * 0.8`
    - `roi_estimated = 100/payback` (ou `100/6` si payback absent)

### `cri_benimallal.py`

- Téléchargement:
  - crawl `/fr/projects` (liste + pagination + pages détail) pour collecter tous les liens `.pdf`
  - téléchargement dans `banque_projets_bk/`
- Extraction PDF:
  - `pdfplumber` + découpe “colonne gauche / colonne droite”
  - lecture des **2 premières pages** (certains chiffres sont sur la 2e page)
  - normalisation robuste du texte (ex: `PB0P` → `PBP`, `KDHS`, `Mn MDHS`, `m2/m²`, etc.)
- Complétion déterministe utile pour recommandation:
  - si `TRI` absent mais `PBP` présent → `roi_estimated = 100 / PBP`
  - si `PBP` absent mais `TRI` présent → `payback_period_years = 100 / TRI`
  - support investissement en `MDH/MDHS`, `DHS/DH`, `KDHS`
  - support surface en `m²`, `m2`, et `Ha` (converti en m²)

## Troubleshooting

- **`output_projects.csv` est ouvert dans Excel**:
  - Windows peut bloquer l’écriture. Ferme Excel puis relance.
  - Certains scripts écrivent un fichier de secours `output_projects_new.csv` si le fichier est verrouillé.

- **Caractères “bizarres” dans l’affichage terminal (Windows)**:
  - Les PDFs contiennent parfois des symboles non supportés par la console.  
    Le CSV est écrit en `utf-8-sig` pour une meilleure compatibilité (Excel).

- **Téléchargement incomplet / pas de PDFs**:
  - Vérifie ta connexion.
  - Vérifie que `https://coeurdumaroc.ma/fr/projects` est accessible.

