#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LDLC smartphone tracker (Excel history)

Objectif
--------
Ce script surveille les prix de smartphones sur LDLC en conservant un historique dans un fichier Excel.

Principe
--------
1) Le script parcourt plusieurs pages de la catégorie "mobile-smartphone" (pagination incluse).
2) Il ne conserve que certaines marques/modèles (iPhone, Samsung/Galaxy, Xiaomi/Redmi/POCO/MIX).
3) Pour chaque produit, il tente d'extraire le prix depuis la page de listing.
   - Si le prix est absent ou ambigu, il peut faire un "fallback" sur la fiche produit (plus coûteux).
4) Il met à jour un fichier Excel :
   - lignes = produits (référence LDLC de type PBxxxx)
   - colonnes = exécutions (timestamp) avec le prix en euros
   - l'historique est conservé : à chaque run, une nouvelle colonne timestamp est ajoutée.

Robustesse (monitoring)
-----------------------
- Le script maintient un compteur de "runs vides" dans state.json.
- Il ne plante qu'après MAX_EMPTY_RUNS exécutions consécutives sans aucun produit récupéré.
  (ex : blocage LDLC, changement HTML, problème réseau).
- Pour éviter l'erreur "artifact introuvable" côté GitHub Actions, un fichier Excel minimal
  est créé s'il n'existe pas encore (même en cas de run vide).

Usage
-----
python scrape.py
"""

import os
import re
import time
import html
import json
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Configuration générale
# =========================

BASE_URL = "https://www.ldlc.com"

PAGES_LISTE = [
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/",
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/page2/",
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/page3/",
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/page4/",
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/page5/",
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/page6/",
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/page7/",
    "https://www.ldlc.com/telephonie/telephonie-portable/mobile-smartphone/c4416/page8/",
]

HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
}

EXCEL_FILE = "ldlc_suivi_smartphones.xlsx"
SHEET_NAME = "Suivi"

REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_PAGES_SEC = 1.0
SLEEP_BETWEEN_PRODUCTS_SEC = 0.15  # délai avant de charger une fiche produit (fallback)

# Pour limiter le temps d'exécution : on borne le nombre de fallbacks "fiche produit".
# Au-delà, on garde prix_eur=None (et l'historique Excel contiendra une valeur manquante).
MAX_FALLBACK_PRODUCT_PAGES = 80

# =========================
# Monitoring (advanced)
# =========================

STATE_FILE = "state.json"
MAX_EMPTY_RUNS = 3  # échec après 3 runs consécutifs sans aucun produit récupéré


def load_state() -> dict:
    """Charge l'état de monitoring (compteur de runs vides)."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"empty_runs": 0}


def save_state(state: dict) -> None:
    """Sauvegarde l'état de monitoring dans state.json."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def ensure_excel_exists(excel_file: str = EXCEL_FILE, sheet_name: str = SHEET_NAME) -> None:
    """
    Crée un fichier Excel minimal s'il n'existe pas.
    Objectif : éviter que GitHub Actions échoue sur l'upload d'artifact si un run est vide.
    """
    if os.path.exists(excel_file):
        return

    df_empty = pd.DataFrame(columns=["reference", "nom", "url_produit"])
    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="w") as writer:
        df_empty.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Fichier Excel initialisé (vide) : {excel_file}")


# =========================
# Session HTTP avec retries
# =========================

def build_session() -> requests.Session:
    """
    Construit une session HTTP avec stratégie de retry.
    Intérêt :
    - amortir les erreurs transitoires (429/5xx, timeouts),
    - réduire le risque de run vide dû à des soucis réseau ponctuels.
    """
    session = requests.Session()
    session.headers.update(HEADERS_HTTP)

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()


# =========================
# Filtre marques : iPhone / Samsung / Xiaomi
# =========================

def is_target_brand(product_name: str) -> bool:
    """
    Filtre simple sur le nom produit :
    - Apple : uniquement iPhone
    - Samsung : "samsung" ou "galaxy"
    - Xiaomi : "xiaomi" ou "redmi" ou "poco" ou "mix"
    """
    n = (product_name or "").lower()

    if "iphone" in n:
        return True
    if "samsung" in n or "galaxy" in n:
        return True
    if "xiaomi" in n or "redmi" in n or "poco" in n or "mix" in n:
        return True
    return False


# =========================
# Utilitaires parsing / extraction prix
# =========================

def get_soup(url: str) -> BeautifulSoup:
    """
    Télécharge une page et retourne un objet BeautifulSoup.
    """
    r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(html.unescape(r.text), "lxml")


def _normalize_price_text(t: str) -> str:
    """
    Normalise un texte contenant un prix : espaces, NBSP, etc.
    """
    t = (t or "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_main_price_from_text(text: str):
    """
    Extrait un prix "principal" depuis un texte susceptible de contenir :
    - ancien prix + nouveau prix,
    - éco-participation (ex: 3€07),
    - paiements en plusieurs fois (ex: 9 x 46€39).

    Méthode :
    - on extrait toutes les occurrences "xxx€yy" ou "xxx€",
    - on filtre sur une plage plausible (50 à 10000 euros),
    - si deux candidats sont présents (souvent old/new), on prend le second.
    """
    if not text:
        return None

    t = _normalize_price_text(text)

    pattern = re.compile(r"(\d[\d\s]*)\s*€\s*(\d{2})?")
    matches = []
    for m in pattern.finditer(t):
        euros_raw = (m.group(1) or "").replace(" ", "")
        cents_raw = m.group(2)
        if not euros_raw.isdigit():
            continue
        euros = int(euros_raw)
        cents = int(cents_raw) if (cents_raw and cents_raw.isdigit()) else 0
        val = euros + cents / 100.0
        matches.append(val)

    if not matches:
        return None

    candidates = [v for v in matches if 50 <= v <= 10000]
    if not candidates:
        return matches[0]

    if len(candidates) >= 2:
        return candidates[1]
    return candidates[0]


def find_product_container(a_tag):
    """
    Remonte dans le DOM depuis le lien produit pour trouver un bloc parent contenant ".price".
    L'objectif est d'associer un lien produit à sa zone prix sur la page listing.
    """
    node = a_tag
    for _ in range(7):
        if node is None:
            break
        if node.select_one(".price"):
            return node
        node = node.parent
    return None


# =========================
# Fallback prix : fiche produit
# =========================

def extract_price_jsonld(soup: BeautifulSoup):
    """
    Cherche un prix dans les scripts JSON-LD (données structurées).
    """
    for s in soup.select('script[type="application/ld+json"]'):
        txt = s.string or s.text
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except json.JSONDecodeError:
            continue

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                offers = node.get("offers")
                if isinstance(offers, dict) and "price" in offers:
                    try:
                        return float(str(offers["price"]).replace(",", "."))
                    except Exception:
                        pass
                if isinstance(offers, list):
                    for of in offers:
                        if isinstance(of, dict) and "price" in of:
                            try:
                                return float(str(of["price"]).replace(",", "."))
                            except Exception:
                                pass
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
    return None


def extract_price_meta(soup: BeautifulSoup):
    """
    Cherche un prix dans des balises meta (itemprop / OpenGraph).
    """
    selectors = [
        ('meta[itemprop="price"]', "content"),
        ('meta[property="product:price:amount"]', "content"),
    ]
    for sel, attr in selectors:
        el = soup.select_one(sel)
        if el and el.get(attr):
            try:
                return float(str(el.get(attr)).replace(",", "."))
            except Exception:
                pass
    return None


def extract_price_dom(soup: BeautifulSoup):
    """
    Cherche un prix dans le DOM via des sélecteurs probables, puis parse le texte.
    """
    selectors = [
        ".price",
        ".sale-price",
        ".prod-cta .price",
        ".product-price",
        "[itemprop='price']",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if not el:
            continue
        txt = " ".join(el.stripped_strings)
        p = extract_main_price_from_text(txt)
        if p is not None:
            return p

    page_txt = soup.get_text(" ", strip=True)
    return extract_main_price_from_text(page_txt)


def get_price_from_product_page(product_url: str):
    """
    Extrait le prix depuis la fiche produit (plus lent qu'un listing).
    """
    soup = get_soup(product_url)
    return extract_price_jsonld(soup) or extract_price_meta(soup) or extract_price_dom(soup)


# =========================
# Scraping pages listing
# =========================

def scrape_listing_page(url: str, fallback_budget: dict):
    """
    Scrape une page listing :
    - récupère les liens produits /fiche/PBxxxx.html
    - filtre marques
    - extrait le prix depuis le listing
    - fallback fiche produit si nécessaire et si budget fallback non épuisé
    """
    soup = get_soup(url)
    links = soup.select('a[href^="/fiche/"]')

    rows = []
    seen_refs = set()

    for a in links:
        href = a.get("href") or ""
        m = re.search(r"^/fiche/(PB[0-9A-Z]+)\.html$", href, re.I)
        if not m:
            continue

        ref = m.group(1)
        if ref in seen_refs:
            continue

        # Nom produit
        name = a.get_text(strip=True) or ""
        if not name:
            parent = a.find_parent()
            if parent:
                title = parent.select_one(".title, .title-3, h3, .txt span")
                if title:
                    name = title.get_text(strip=True)

        if not name:
            continue

        if not is_target_brand(name):
            continue

        container = find_product_container(a)
        price_eur = None

        # Extraction prix depuis le listing
        if container:
            el_price = container.select_one(".price")
            if el_price:
                txt_price = " ".join(el_price.stripped_strings)
                price_eur = extract_main_price_from_text(txt_price)

        abs_url = urljoin(BASE_URL, href)

        # Fallback fiche produit (limité par MAX_FALLBACK_PRODUCT_PAGES)
        if price_eur is None and fallback_budget["remaining"] > 0:
            time.sleep(SLEEP_BETWEEN_PRODUCTS_SEC)
            try:
                price_eur = get_price_from_product_page(abs_url)
            except Exception:
                price_eur = None
            finally:
                fallback_budget["remaining"] -= 1

        rows.append(
            {
                "reference": ref,
                "nom": name,
                "url_produit": abs_url,
                "prix_eur": price_eur,
            }
        )
        seen_refs.add(ref)

    return rows


def scrape_all_pages():
    """
    Scrape l'ensemble des pages configurées et déduplique par référence produit.
    """
    all_rows = []
    seen = set()

    fallback_budget = {"remaining": MAX_FALLBACK_PRODUCT_PAGES}

    for url in PAGES_LISTE:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Page: {url}")
        try:
            page_rows = scrape_listing_page(url, fallback_budget=fallback_budget)
        except Exception as e:
            print(f"Erreur lors du scraping de la page ({url}) : {e}")
            page_rows = []

        print(f"  Produits conservés (iPhone/Samsung/Xiaomi) : {len(page_rows)}")
        for r in page_rows:
            if r["reference"] not in seen:
                all_rows.append(r)
                seen.add(r["reference"])

        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    # Tri : prix croissant, puis nom. Les prix manquants vont à la fin.
    all_rows.sort(key=lambda x: (x["prix_eur"] if x["prix_eur"] is not None else 1e18, x["nom"]))
    print(f"Budget fallback restant (fiches produit) : {fallback_budget['remaining']}")
    return all_rows


# =========================
# Historisation Excel
# =========================

def update_excel_history(rows, excel_file=EXCEL_FILE, sheet_name=SHEET_NAME):
    """
    Met à jour l'historique Excel.

    - Chaque run ajoute une nouvelle colonne horodatée contenant le prix (prix_eur).
    - Les colonnes "nom" et "url_produit" sont maintenues (et mises à jour si changement).
    - On conserve toutes les colonnes existantes (historique complet).
    """
    ensure_excel_exists(excel_file=excel_file, sheet_name=sheet_name)

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    df_run = pd.DataFrame(rows).copy()
    if df_run.empty:
        print("Aucun produit trouvé : aucun ajout de colonne timestamp.")
        return

    df_run[timestamp] = df_run["prix_eur"]
    df_run = df_run.set_index("reference")[["nom", "url_produit", timestamp]]

    # Chargement de l'historique existant
    try:
        df_hist = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl")
        if not df_hist.empty and "reference" in df_hist.columns:
            df_hist = df_hist.set_index("reference")
        else:
            df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))
    except Exception as e:
        print(f"Impossible de lire l'Excel existant ({e}). Recréation d'un historique vide.")
        df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))

    # On travaille sur l'union des références (anciennes + nouvelles)
    idx = df_hist.index.union(df_run.index)
    df_merged = df_hist.reindex(idx)

    # Assure l'existence de colonnes structurelles
    for col in ("nom", "url_produit"):
        if col not in df_merged.columns:
            df_merged[col] = pd.NA

    # Mise à jour nom/url : priorité aux valeurs du run courant quand elles existent
    df_merged["nom"] = df_run["nom"].reindex(idx).combine_first(df_merged["nom"])
    df_merged["url_produit"] = df_run["url_produit"].reindex(idx).combine_first(df_merged["url_produit"])

    # Ajout de la colonne timestamp pour ce run (prix)
    df_merged[timestamp] = df_run[timestamp].reindex(idx)

    df_out = df_merged.reset_index()

    # Tri : on privilégie la colonne du dernier run (timestamp), puis le nom
    df_out = df_out.sort_values(by=[timestamp, "nom"], na_position="last")

    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="w") as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Excel mis à jour : {excel_file} | run = {timestamp} | nb_produits_run = {len(df_run)}")


# =========================
# Exécution (un run par lancement)
# =========================

def run_once():
    """
    Exécute un cycle complet :
    - scraping
    - monitoring runs vides
    - mise à jour Excel si données présentes
    """
    ensure_excel_exists()

    state = load_state()

    rows = scrape_all_pages()
    print(f"Total produits uniques conservés : {len(rows)}")

    # Monitoring : gestion des runs vides
    if not rows:
        state["empty_runs"] = int(state.get("empty_runs", 0)) + 1
        save_state(state)

        print(f"Aucun produit récupéré. empty_runs={state['empty_runs']} (seuil={MAX_EMPTY_RUNS})")

        if state["empty_runs"] >= MAX_EMPTY_RUNS:
            raise RuntimeError(
                f"Aucun produit récupéré pendant {state['empty_runs']} runs consécutifs. "
                "Cause probable : blocage, changement HTML ou problème réseau."
            )
        return

    # Reset compteur si run non vide
    state["empty_runs"] = 0
    save_state(state)

    # Affichage d'un échantillon (utile dans les logs GitHub Actions)
    df_preview = pd.DataFrame(rows)
    if not df_preview.empty:
        print(df_preview.head(25).to_string(index=False))

    update_excel_history(rows)


if __name__ == "__main__":
    run_once()
