#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LDLC smartphone tracker (Excel history)
- Scrapes LDLC listing pages for Apple iPhone / Samsung / Xiaomi
- Writes/updates an Excel file with:
    rows   = products (LDLC reference PBxxxx)
    cols   = runs (timestamp) containing price_eur
- Keeps history (adds a new timestamp column each run)

Usage:
    python scrape.py

Notes:
- Be mindful of LDLC terms and reasonable request rate (sleep included).
- "Advanced monitoring": fails only after N consecutive empty runs.
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
SLEEP_BETWEEN_PRODUCTS_SEC = 0.15  # fallback product pages only if price missing

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
# Brand filter: keep all iPhone / Samsung / Xiaomi
# =========================

def is_target_brand(product_name: str) -> bool:
    n = (product_name or "").lower()
    # Apple: focus on iPhone only (avoids random Apple accessories if any)
    if "iphone" in n:
        return True
    # Samsung phones
    if "samsung" in n or "galaxy" in n:
        return True
    # Xiaomi phones (include Redmi/MIX/POCO if LDLC uses those labels)
    if "xiaomi" in n or "redmi" in n or "poco" in n or "mix" in n:
        return True
    return False


# =========================
# HTTP / parsing utilities
# =========================

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS_HTTP, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(html.unescape(r.text), "lxml")


def _normalize_price_text(t: str) -> str:
    # Keep NBSP as space, remove weird spacing
    t = (t or "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_main_price_from_text(text: str):
    """
    Extract a "real" product price from a blob of text that may contain:
    - old price + new price
    - eco-part (3€07)
    - installments (9 x 46€39)
    Strategy:
    - find all occurrences of "xxx€yy" (or "xxx€") in order
    - keep candidates between 50 and 10000 euros (avoid eco-part & installments)
    - if there are 2 candidates early (often old+new), pick the LAST of the first 2
      -> handles promo blocks where old price appears before new price
    """
    if not text:
        return None

    t = _normalize_price_text(text)

    # Match formats like:
    # "1 329€00", "1329€00", "1329 € 00", "659€", "659€00"
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
        matches.append((m.start(), val))

    if not matches:
        return None

    # Filter out eco-part / monthly payments etc.
    candidates = [val for (_pos, val) in matches if 50 <= val <= 10000]

    if not candidates:
        # As a last resort, return the first parsed number
        return matches[0][1]

    # If promo: old then new; pick last among first two candidates
    if len(candidates) >= 2:
        return candidates[1]
    return candidates[0]


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
    # Try a few likely price containers; then parse with robust extractor.
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
    # Fallback: search in whole page text (more expensive but robust)
    page_txt = soup.get_text(" ", strip=True)
    return extract_main_price_from_text(page_txt)


def get_price_from_product_page(product_url: str):
    soup = get_soup(product_url)
    return extract_price_jsonld(soup) or extract_price_meta(soup) or extract_price_dom(soup)


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

        # Keep only iPhone/Samsung/Xiaomi
        if not is_target_brand(name):
            continue

        container = find_product_container(a)
        price_eur = None

        if container:
            el_price = container.select_one(".price")
            if el_price:
                # IMPORTANT: this block can contain old/new/eco-part/installments
                txt_price = " ".join(el_price.stripped_strings)
                price_eur = extract_main_price_from_text(txt_price)

        abs_url = urljoin(BASE_URL, href)

        # Fallback to product page if listing price missing
        if price_eur is None:
            time.sleep(SLEEP_BETWEEN_PRODUCTS_SEC)
            try:
                price_eur = get_price_from_product_page(abs_url)
            except Exception:
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

    # Add the new run column aligned on index
    df_merged[timestamp] = df_run[timestamp].reindex(df_merged.index)

    # Update name/url if changed
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

    # This overwrites the XLSX file, but keeps ALL history columns inside it.
    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="w") as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Excel mis à jour : {excel_file} | run = {timestamp} | produits = {len(df_run)}")


# =========================
# Main (one run per execution)
# =========================

def run_once():
    state = load_state()

    rows = scrape_all_pages()
    print(f"Total produits uniques gardés : {len(rows)}")

    # Monitoring: empty run counter
    if not rows:
        state["empty_runs"] = int(state.get("empty_runs", 0)) + 1
        save_state(state)

        print(f"Aucun produit récupéré. empty_runs={state['empty_runs']} (seuil={MAX_EMPTY_RUNS})")
        if state["empty_runs"] >= MAX_EMPTY_RUNS:
            raise RuntimeError(
                f"Aucun produit récupéré pendant {state['empty_runs']} runs consécutifs. "
                "Blocage LDLC, changement HTML ou problème réseau probable."
            )
        return

    # Reset counter on success
    state["empty_runs"] = 0
    save_state(state)

    # Optional preview
    df = pd.DataFrame(rows)
    if not df.empty:
        print(df.head(25).to_string(index=False))

    update_excel_history(rows)


if __name__ == "__main__":
    run_once()
