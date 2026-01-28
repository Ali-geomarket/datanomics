#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LDLC smartphone tracker (Excel history)

Objectif
--------
- Parcourir les pages "listing" LDLC de smartphones
- Garder uniquement Apple iPhone / Samsung / Xiaomi
- Enregistrer un historique des prix dans un fichier Excel :
    lignes  = produits (référence LDLC de type PBxxxx)
    colonnes = exécutions (horodatage), avec le prix en euros

Points d’attention
------------------
- LDLC affiche parfois des montants type "€/mois" ou "3 x ..." : ce ne sont pas les prix comptants.
  Le script filtre ces montants pour garder le prix final (ex: 169,95€).
- Pour éviter les échecs après un certain nombre de requêtes :
  utilisation d'une session HTTP + retries + temporisations raisonnables.
- Si une exécution ne remonte aucun produit, on n’échoue pas immédiatement :
  on échoue seulement après N runs consécutifs vides (monitoring).
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
# Configuration
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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
        "Gecko/20100101 Firefox/123.0"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

EXCEL_FILE = "ldlc_suivi_smartphones.xlsx"
SHEET_NAME = "Suivi"

REQUEST_TIMEOUT = 30

# Si tu vois des 403/429 après ~100 produits, augmente un peu les sleeps
SLEEP_BETWEEN_PAGES_SEC = 1.2
SLEEP_BETWEEN_PRODUCTS_SEC = 0.35

# Limite le nombre de "fallback" vers fiche produit par run (réduit fortement le risque de blocage)
MAX_FALLBACK_PRODUCT_PAGES = 60

# =========================
# Monitoring (advanced)
# =========================

STATE_FILE = "state.json"
MAX_EMPTY_RUNS = 3  # échec uniquement après 3 runs consécutifs vides


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"empty_runs": 0}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# =========================
# Filtre marque
# =========================

def is_target_brand(product_name: str) -> bool:
    n = (product_name or "").lower()
    if "iphone" in n:
        return True
    if "samsung" in n or "galaxy" in n:
        return True
    if "xiaomi" in n or "redmi" in n or "poco" in n:
        return True
    return False


# =========================
# Session HTTP + retries
# =========================

def build_session() -> requests.Session:
    """
    Session requests avec retries sur erreurs réseau et codes fréquents de limitation (429) / serveur.
    """
    session = requests.Session()
    session.headers.update(HEADERS_HTTP)

    retry = Retry(
        total=4,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()


def get_soup(url: str) -> BeautifulSoup:
    r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    # Log minimal utile si LDLC limite
    if r.status_code >= 400:
        print(f"  !! HTTP {r.status_code} sur {url}")
    r.raise_for_status()
    return BeautifulSoup(html.unescape(r.text), "lxml")


# =========================
# Extraction prix robuste
# =========================

def _normalize_spaces(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_installment_context(text: str, span_start: int, span_end: int) -> bool:
    """
    Détecte si un montant est dans un contexte de mensualité / paiement en plusieurs fois.
    On regarde un voisinage autour du montant.
    """
    left = max(0, span_start - 18)
    right = min(len(text), span_end + 18)
    ctx = text[left:right].lower()

    # Signaux typiques de mensualités / paiement fractionné
    # (ex: "3 x 58€58", "x 46€39", "€/mois", "par mois", "mensuel", "à partir de")
    if re.search(r"(\b\d+\s*[x×]\s*$)", text[left:span_start].lower()):
        return True
    if "€/mois" in ctx or "par mois" in ctx or "mensuel" in ctx:
        return True
    if re.search(r"\b\d+\s*[x×]\s*\d", ctx):
        return True
    if "payer mensuellement" in ctx or "paiement" in ctx:
        return True
    if "a partir de" in ctx or "à partir de" in ctx:
        return True

    return False


def extract_cash_price(text: str):
    """
    Extrait le prix comptant le plus probable à partir d'un bloc texte.

    Méthode :
    - on récupère toutes les occurrences "xxx€yy"
    - on supprime les montants associés à une mensualité (x fois, €/mois, etc.)
    - on supprime les montants trop faibles (éco-part typiquement 3€05)
    - on garde un prix plausible smartphone et on prend le meilleur candidat
    """
    if not text:
        return None

    t = _normalize_spaces(text)

    pattern = re.compile(r"(\d[\d\s]*)\s*€\s*(\d{2})?")
    found = []

    for m in pattern.finditer(t):
        euros_raw = (m.group(1) or "").replace(" ", "")
        cents_raw = m.group(2)

        if not euros_raw.isdigit():
            continue

        euros = int(euros_raw)
        cents = int(cents_raw) if (cents_raw and cents_raw.isdigit()) else 0
        val = euros + cents / 100.0

        found.append((m.start(), m.end(), val))

    if not found:
        return None

    # 1) écarte les contextes "mensualités"
    candidates = []
    for start, end, val in found:
        if _is_installment_context(t, start, end):
            continue
        candidates.append(val)

    # 2) écarte les montants trop faibles (éco-part, frais…)
    candidates = [v for v in candidates if v >= 30]

    if not candidates:
        # En dernier recours, prendre le maximum trouvé (ça évite souvent de garder la mensualité)
        return max(v for (_s, _e, v) in found)

    # 3) règle simple : le prix comptant est généralement le plus élevé parmi les candidats filtrés
    return max(candidates)


def find_product_container(a_tag):
    """
    Remonte dans le DOM pour trouver un bloc parent contenant une zone de prix.
    """
    node = a_tag
    for _ in range(10):
        if node is None:
            break
        if node.select_one(".price"):
            return node
        node = node.parent
    return None


# =========================
# Fallback fiche produit
# =========================

def extract_price_jsonld(soup: BeautifulSoup):
    """
    Les fiches produit contiennent souvent du JSON-LD avec offers.price.
    C’est généralement la source la plus fiable (prix comptant).
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
    Alternative : meta itemprop=price ou OpenGraph.
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
    Dernier recours : extraction dans le DOM via classes usuelles + filtre mensualités.
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
        p = extract_cash_price(txt)
        if p is not None:
            return p

    page_txt = soup.get_text(" ", strip=True)
    return extract_cash_price(page_txt)


def get_price_from_product_page(product_url: str):
    soup = get_soup(product_url)
    return extract_price_jsonld(soup) or extract_price_meta(soup) or extract_price_dom(soup)


# =========================
# Scraping listing pages
# =========================

def scrape_listing_page(url: str, fallback_budget: dict):
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

        if not name or not is_target_brand(name):
            continue

        abs_url = urljoin(BASE_URL, href)

        # Prix depuis le listing si possible
        price_eur = None
        container = find_product_container(a)
        if container:
            el_price = container.select_one(".price")
            if el_price:
                txt_price = " ".join(el_price.stripped_strings)
                price_eur = extract_cash_price(txt_price)

        # Fallback fiche produit uniquement si nécessaire + budget limité
        if price_eur is None and fallback_budget["used"] < fallback_budget["max"]:
            fallback_budget["used"] += 1
            time.sleep(SLEEP_BETWEEN_PRODUCTS_SEC)
            try:
                price_eur = get_price_from_product_page(abs_url)
            except Exception as e:
                print(f"  !! Fallback KO {ref} ({abs_url}) : {e}")
                price_eur = None

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
    all_rows = []
    seen = set()

    fallback_budget = {"used": 0, "max": MAX_FALLBACK_PRODUCT_PAGES}

    for url in PAGES_LISTE:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Listing: {url}")
        try:
            page_rows = scrape_listing_page(url, fallback_budget)
        except Exception as e:
            print(f"  !! Erreur listing ({url}) : {e}")
            page_rows = []

        print(f"  -> {len(page_rows)} produits retenus | fallback_used={fallback_budget['used']}/{fallback_budget['max']}")

        for r in page_rows:
            if r["reference"] not in seen:
                all_rows.append(r)
                seen.add(r["reference"])

        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    all_rows.sort(key=lambda x: (x["prix_eur"] if x["prix_eur"] is not None else 1e18, x["nom"]))
    return all_rows


# =========================
# Excel history
# =========================

def update_excel_history(rows, excel_file=EXCEL_FILE, sheet_name=SHEET_NAME):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    df_run = pd.DataFrame(rows).copy()
    if df_run.empty:
        print("Aucun produit -> Excel non modifié.")
        return

    df_run[timestamp] = df_run["prix_eur"]
    df_run = df_run.set_index("reference")
    df_run = df_run[["nom", "url_produit", timestamp]]

    # Chargement historique
    if os.path.exists(excel_file):
        try:
            df_hist = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl")
            if not df_hist.empty and "reference" in df_hist.columns:
                df_hist = df_hist.set_index("reference")
            else:
                df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))
        except Exception as e:
            print(f"Lecture Excel impossible ({e}). Recréation.")
            df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))
    else:
        df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))

    # Merge
    df_merged = df_hist.combine_first(df_run)
    df_merged[timestamp] = df_run[timestamp].reindex(df_merged.index)

    # Mise à jour nom / url si changent
    if "nom" in df_merged.columns:
        df_merged["nom"] = df_run["nom"].combine_first(df_merged["nom"])
    else:
        df_merged["nom"] = df_run["nom"]

    if "url_produit" in df_merged.columns:
        df_merged["url_produit"] = df_run["url_produit"].combine_first(df_merged["url_produit"])
    else:
        df_merged["url_produit"] = df_run["url_produit"]

    df_out = df_merged.reset_index()
    df_out = df_out.sort_values(by=[timestamp, "nom"], na_position="last")

    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="w") as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Excel mis à jour : {excel_file} | run={timestamp} | produits={len(df_run)}")


# =========================
# Main
# =========================

def run_once():
    state = load_state()

    rows = scrape_all_pages()
    print(f"Total produits uniques : {len(rows)}")

    # Monitoring runs vides
    if not rows:
        state["empty_runs"] = int(state.get("empty_runs", 0)) + 1
        save_state(state)

        print(f"Aucun produit récupéré. empty_runs={state['empty_runs']} (seuil={MAX_EMPTY_RUNS})")
        if state["empty_runs"] >= MAX_EMPTY_RUNS:
            raise RuntimeError(
                f"Aucun produit récupéré pendant {state['empty_runs']} runs consécutifs. "
                "Changement LDLC / blocage / réseau probable."
            )
        return

    state["empty_runs"] = 0
    save_state(state)

    # Aperçu console
    df = pd.DataFrame(rows)
    if not df.empty:
        missing = df["prix_eur"].isna().sum()
        print(df.head(20).to_string(index=False))
        print(f"Produits sans prix (None) : {missing}/{len(df)}")

    update_excel_history(rows)


if __name__ == "__main__":
    run_once()
