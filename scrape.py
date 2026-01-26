#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LDLC smartphone tracker (Excel history)
- Scrapes LDLC listing pages (smartphones category)
- Keeps only: iPhone (Apple), Samsung, Xiaomi
- Writes/updates an Excel file with:
    rows   = products (LDLC reference PBxxxx)
    cols   = runs (timestamp) containing price_eur
- Keeps history (adds a new timestamp column each run)
- Designed to be GitHub-friendly (single file, requirements.txt ready)

Usage:
    python scrape.py

Notes:
- Be mindful of LDLC terms and reasonable request rate (sleep included).
- This version includes "advanced monitoring":
  it fails only after N consecutive empty runs (possible temporary block or HTML change).
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
}

EXCEL_FILE = "ldlc_suivi_smartphones.xlsx"
SHEET_NAME = "Suivi"

REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_PAGES_SEC = 1.0
SLEEP_BETWEEN_PRODUCTS_SEC = 0.2  # when opening product pages for fallback price

# Prix "raisonnable" max pour éviter les parsing foireux (Excel en E+11)
MAX_REASONABLE_PRICE = 10000.0

# =========================
# Monitoring (advanced)
# =========================
STATE_FILE = "state.json"
MAX_EMPTY_RUNS = 3  # fail only after 3 consecutive empty runs


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
# Brand filter (keep only iPhone/Samsung/Xiaomi)
# =========================

def normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def is_allowed_brand(product_name: str) -> bool:
    """
    Keep only:
    - iPhone (Apple iPhone / iPhone)
    - Samsung (Samsung / Galaxy)
    - Xiaomi (Xiaomi)
    """
    n = normalize_text(product_name)
    if not n:
        return False

    # iPhone
    if "iphone" in n:
        return True

    # Samsung
    if "samsung" in n or "galaxy" in n:
        return True

    # Xiaomi
    if "xiaomi" in n:
        return True

    return False


# =========================
# HTTP / parsing utilities
# =========================

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS_HTTP, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(html.unescape(r.text), "lxml")


def text_to_price(text: str):
    """Convert LDLC price text to float EUR."""
    if not text:
        return None

    t = text.replace("\xa0", "").replace(" ", "").strip()

    # Common formats: "1299€00" or "1299€"
    m = re.match(r"^(\d+)(?:€(\d{2}))?$", t)
    if m:
        euros = int(m.group(1))
        cents = int(m.group(2)) if m.group(2) else 0
        return euros + cents / 100

    # Fallback: keep digits and separators
    t = re.sub(r"[^\d,\.]", "", t).replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None


def find_product_container(a_tag):
    """Climb up DOM to find a block that contains a '.price'."""
    node = a_tag
    for _ in range(7):
        if node is None:
            break
        if node.select_one(".price"):
            return node
        node = node.parent
    return None


def format_euro(price: float) -> str:
    if price is None:
        return ""
    euros = int(price)
    cents = int(round((price - euros) * 100))
    s = f"{euros:,}".replace(",", " ").replace(" ", "\xa0")
    return f"{s}€{cents:02d}"


def price_is_sane(price: float) -> bool:
    return price is not None and 0 < price <= MAX_REASONABLE_PRICE


# =========================
# Fallback: read price from product page
# =========================

def extract_price_jsonld(soup: BeautifulSoup):
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
                    return text_to_price(str(offers["price"]))
                if isinstance(offers, list):
                    for of in offers:
                        if isinstance(of, dict) and "price" in of:
                            return text_to_price(str(of["price"]))
                if "price" in node:
                    return text_to_price(str(node["price"]))
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
    return None


def extract_price_meta(soup: BeautifulSoup):
    selectors = [
        ('meta[itemprop="price"]', "content"),
        ('meta[property="product:price:amount"]', "content"),
    ]
    for sel, attr in selectors:
        el = soup.select_one(sel)
        if el and el.get(attr):
            p = text_to_price(el.get(attr))
            if p is not None:
                return p
    return None


def extract_price_dom(soup: BeautifulSoup):
    el = soup.select_one(".price, .sale-price, .prod-cta .price, .product-price")
    if el:
        return text_to_price("".join(el.stripped_strings))
    return None


def get_price_from_product_page(product_url: str):
    soup = get_soup(product_url)
    price = extract_price_jsonld(soup) or extract_price_meta(soup) or extract_price_dom(soup)
    if not price_is_sane(price):
        return None, ""
    return price, format_euro(price)


# =========================
# Scraping listing pages
# =========================

def scrape_listing_page(url: str):
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

        # Product name
        name = a.get_text(strip=True) or ""
        if not name:
            parent = a.find_parent()
            if parent:
                title = parent.select_one(".title, .title-3, h3, .txt span")
                if title:
                    name = title.get_text(strip=True)

        if not name:
            continue

        # Keep only iPhone / Samsung / Xiaomi
        if not is_allowed_brand(name):
            continue

        container = find_product_container(a)
        raw_price = None
        price_eur = None
        if container:
            el_price = container.select_one(".price")
            if el_price:
                raw_price = "".join(el_price.stripped_strings)
                price_eur = text_to_price(raw_price)

        # Sanity check: si prix absurde => fallback produit
        if not price_is_sane(price_eur):
            price_eur = None
            raw_price = None

        abs_url = urljoin(BASE_URL, href)

        # Fallback to product page if listing price missing or insane
        if price_eur is None:
            time.sleep(SLEEP_BETWEEN_PRODUCTS_SEC)
            price_eur, formatted = get_price_from_product_page(abs_url)
            raw_price = formatted if formatted else None

        rows.append(
            {
                "reference": ref,
                "nom": name,
                "url_produit": abs_url,
                "prix_eur": price_eur,
                "prix_brut": raw_price,
            }
        )
        seen_refs.add(ref)

    return rows


def scrape_all_pages():
    all_rows = []
    seen = set()

    for url in PAGES_LISTE:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Page: {url}")
        try:
            page_rows = scrape_listing_page(url)
        except Exception as e:
            print(f"  !! Erreur page ({url}) : {e}")
            page_rows = []

        print(f"  -> {len(page_rows)} produits gardés (iPhone/Samsung/Xiaomi)")
        for r in page_rows:
            if r["reference"] not in seen:
                all_rows.append(r)
                seen.add(r["reference"])

        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    # sort by price then name (None last)
    all_rows.sort(key=lambda x: (x["prix_eur"] if x["prix_eur"] is not None else 1e18, x["nom"]))
    return all_rows


# =========================
# Excel history: columns = runs
# =========================

def update_excel_history(rows, excel_file=EXCEL_FILE, sheet_name=SHEET_NAME):
    """
    Rows = products (PB ref)
    Columns = timestamps (each run) with price_eur
    Keeps past runs, adds new products.
    """
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    df_run = pd.DataFrame(rows).copy()
    if df_run.empty:
        print("Aucun produit trouvé -> Excel non modifié.")
        return

    df_run[timestamp] = df_run["prix_eur"]
    df_run = df_run.set_index("reference")
    df_run = df_run[["nom", "url_produit", timestamp]]

    # Load existing history
    if os.path.exists(excel_file):
        try:
            df_hist = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl")
            if not df_hist.empty and "reference" in df_hist.columns:
                df_hist = df_hist.set_index("reference")
            else:
                df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))
        except Exception as e:
            print(f"Impossible de lire l'excel existant ({e}). On repart de zéro.")
            df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))
    else:
        df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))

    # Merge: keep all existing columns, then add/overwrite only the current run column
    df_merged = df_hist.combine_first(df_run)

    # Add the new run column aligned on index (no overlap)
    df_merged[timestamp] = df_run[timestamp].reindex(df_merged.index)

    # Update name/url if changed (prefer current run)
    if "nom" in df_merged.columns:
        df_merged["nom"] = df_run["nom"].combine_first(df_merged["nom"])
    else:
        df_merged["nom"] = df_run["nom"]

    if "url_produit" in df_merged.columns:
        df_merged["url_produit"] = df_run["url_produit"].combine_first(df_merged["url_produit"])
    else:
        df_merged["url_produit"] = df_run["url_produit"]

    df_out = df_merged.reset_index()

    # Sort by latest run column then name (None last)
    df_out = df_out.sort_values(by=[timestamp, "nom"], na_position="last")

    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="w") as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Excel mis à jour : {excel_file} | run = {timestamp} | produits = {len(df_run)}")


# =========================
# Main (one run per execution)
# =========================

def run_once():
    state = load_state()

    rows = scrape_all_pages()
    print(f"Total produits uniques (après filtre marque) : {len(rows)}")

    # Monitoring: empty run counter (fail only after N consecutive empty runs)
    if not rows:
        state["empty_runs"] = int(state.get("empty_runs", 0)) + 1
        save_state(state)

        print(f"⚠️ Aucun produit récupéré. empty_runs={state['empty_runs']} (seuil={MAX_EMPTY_RUNS})")
        if state["empty_runs"] >= MAX_EMPTY_RUNS:
            raise RuntimeError(
                f"Aucun produit récupéré pendant {state['empty_runs']} runs consécutifs. "
                "Blocage LDLC, changement HTML ou problème réseau probable."
            )
        return

    # Reset counter on success
    state["empty_runs"] = 0
    save_state(state)

    # Optional: quick preview
    df = pd.DataFrame(rows)
    if not df.empty:
        print(df.head(20).to_string(index=False))

    update_excel_history(rows)


if __name__ == "__main__":
    run_once()
