"""
Scrape natural wine producer lists from:
  - vinnatur.se/bonder/
  - gladvin.dk
  - louisdressner.com/producers
  - lescaves.co.uk/producers (if accessible)
  - winetrade.se (if accessible)

Usage:
  python3 scripts/scrape.py              # live scrape
  python3 scripts/scrape.py --offline    # use cached HTML files

Output: data/producers.json
"""

import json, re, sys, requests
from bs4 import BeautifulSoup
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
OFFLINE = "--offline" in sys.argv

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"


def get_html(url, filename):
    cache = DATA / filename
    if OFFLINE:
        if not cache.exists():
            sys.exit(f"ERROR: {cache} not found. Run without --offline first.")
        return cache.read_text(encoding="utf-8")
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    cache.write_text(r.text, encoding="utf-8")
    return r.text


def scrape_vinnatur():
    print("Scraping vinnatur.se...")
    soup = BeautifulSoup(get_html("https://vinnatur.se/bonder/", "vinnatur.html"), "html.parser")
    producers = []

    for h5 in soup.find_all("h5"):
        a = h5.find("a")
        if not a or "/bonde/" not in a.get("href", ""):
            continue
        name = a.get_text(strip=True)
        href = a["href"]
        url = href if href.startswith("http") else "https://vinnatur.se" + href
        region = country = None
        for el in h5.find_all_previous():
            if el.name == "h4" and region is None:
                region = el.get_text(strip=True)
            if el.name == "h2" and country is None:
                country = el.get_text(strip=True)
            if region and country:
                break
        producers.append({"name": name, "country": country, "region": region,
                          "source": "vinnatur.se", "url": url})

    print(f"  Found {len(producers)} producers")
    return producers


SKIP_SLUGS = {
    "smagekasser-6-forskellige-vine-bragt-til-din-doer", "nin-ortiz-kasser",
    "gavekort-til-gladvin", "champagne", "crmant-pt-nat-andre-mousserende-vine",
    "hvidvin", "orangevin", "ros", "roedvin", "cider-oel-kombucha-etc", "sake",
    "vin-till-sverige", "tres-hombres", "ved-bestilling", "loire",
    "resten-af-frankrig", "catalonien", "resten-af-spanien", "portugal",
    "italien", "georgien", "tyskland-og-oestrig", "graekenland", "australien",
    "usa", "canada", "mexico", "chile", "argentina", "anjou", "bourgueil-chinon",
    "cheverny", "muscadet", "saumur", "savennires", "touraine", "alsace",
    "bourgogne", "beaujolais", "jura-savoie-bugey", "rhne-provence", "ardche",
    "syd-og-vestfrankrig", "bourgogne-beaujolais", "stille-vine", "hvide-bobler",
    "roede-bobler", "mousserende", "fragt-til-bornholm", "fiefs-vendeens",
    "pouilly-fum", "coteaux-dancenis", "saumur-champigny", "touraine-og-cheverny",
    "champagne-ctx-champenois", "nrias-wines", "samegrelo-imereti-vest-georgien",
    "kartli-kakheti-oest-georgien", "tysk-og-oestrigsk-orangevin",
    "fransk-orangevin", "spansk-orangevin", "italiensk-orangevin",
    "georgisk-orangevin", "nordamerikansk-orangevin", "sydamerikansk-orangevin",
    "australsk-orangevin", "andre-mousserende-vine", "part-i-getaria-knippelsbro",
    "part-ii-la-rochelle-knippelsbro", "for-kent", "aeblecider", "paerecider",
    "calvados", "mousserende-sake", "luyt-frisach",
}

NON_WINE_WORDS = {"brewery", "brasserie", "cider", "sake", "øl", "beer", "kombucha"}
NON_WINE_SLUG_WORDS = {"sake", "shuz", "brasserie", "brewery", "cidre", "kombucha",
                       "biden", "daigo", "dokan", "gonin", "hanatomoe", "ine-mankai",
                       "kame", "katori", "kido", "kirei", "kizan", "nabeshima",
                       "soma", "yuzu", "ryorizake", "akishika"}


def scrape_gladvin():
    print("Scraping gladvin.dk...")
    soup = BeautifulSoup(get_html("https://gladvin.dk/", "gladvin.html"), "html.parser")
    producers = []
    seen = set()

    all_uls = [ul for ul in soup.find_all("ul")
               if "dropdown-menu" in (ul.get("class") or [])]

    for ul in all_uls:
        for a in ul.find_all("a"):
            href = a.get("href", "")
            label = a.get_text(strip=True)
            if not href or not label or len(label) < 3:
                continue
            m = re.match(r"^/([^/]+)-(\d+)/$", href)
            if not m:
                continue
            slug_base = re.sub(r"-\d+$", "", m.group(1))
            if slug_base in SKIP_SLUGS:
                continue
            if any(w in slug_base for w in NON_WINE_SLUG_WORDS):
                continue
            parts = [p.strip() for p in label.split(",")]
            name = parts[0]
            region = parts[1] if len(parts) > 1 else None
            country = parts[2] if len(parts) > 2 else None
            if any(w in name.lower() for w in NON_WINE_WORDS):
                continue
            norm = name.lower()
            if norm not in seen and len(name) > 2:
                seen.add(norm)
                producers.append({
                    "name": name, "country": country, "region": region,
                    "source": "gladvin.dk",
                    "url": "https://gladvin.dk" + href,
                })

    print(f"  Found {len(producers)} producers")
    return producers


def scrape_louisdressner():
    print("Scraping louisdressner.com...")
    soup = BeautifulSoup(get_html("https://louisdressner.com/producers", "louisdressner.html"), "html.parser")
    producers = []
    seen = set()

    # Structure: h3 = country, h4 = region, li > a = producer
    current_country = None
    current_region = None

    for el in soup.find_all(["h3", "h4", "li"]):
        if el.name == "h3":
            current_country = el.get_text(strip=True).title()
            current_region = None
        elif el.name == "h4":
            current_region = el.get_text(strip=True)
        elif el.name == "li":
            a = el.find("a")
            if not a:
                continue
            href = a.get("href", "")
            if "/producers/" not in href:
                continue
            name = a.get_text(strip=True)
            # Skip non-wine and non-European (we include all for corpus breadth)
            if not name or len(name) < 2:
                continue
            norm = name.lower()
            if norm not in seen:
                seen.add(norm)
                producers.append({
                    "name": name,
                    "country": current_country,
                    "region": current_region,
                    "source": "louisdressner.com",
                    "url": "https://louisdressner.com" + href if href.startswith("/") else href,
                })

    # Deduplicate (the page renders the list twice)
    deduped = []
    seen2 = set()
    for p in producers:
        k = p["name"].lower()
        if k not in seen2:
            seen2.add(k)
            deduped.append(p)

    print(f"  Found {len(deduped)} producers")
    return deduped


def normalize(name):
    name = re.sub(r"\s*\(.*?\)", "", name)
    return name.split(",")[0].strip().lower()


def scrape_winetrade():
    print("Scraping winetrade.se...")
    soup = BeautifulSoup(
        get_html("https://winetrade.se/en/pages/vinbonder", "winetrade.html"),
        "html.parser"
    )
    producers = []
    seen = set()
    current_country = None
    current_region = None

    for el in soup.find_all(["h2", "h3", "li"]):
        text = el.get_text(strip=True)
        if not text:
            continue
        if el.name == "h2":
            current_country = text
            current_region = None
        elif el.name == "h3":
            current_region = text
        elif el.name == "li":
            a = el.find("a")
            if not a:
                continue
            name = a.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            if any(w in name.lower() for w in {"webshop", "systembolaget", "restaurang",
                                                "kontakt", "log in", "search", "cart"}):
                continue
            norm = name.lower()
            if norm not in seen:
                seen.add(norm)
                producers.append({
                    "name": name, "country": current_country,
                    "region": current_region, "source": "winetrade.se",
                    "url": a.get("href", ""),
                })

    SKIP = {"webshop", "systembolaget", "restaurang", "kontakt", "om oss",
            "allmänna", "subscribe", "facebook", "instagram", "loading", "hemleverans"}
    filtered = [p for p in producers
                if not any(w in p["name"].lower() for w in SKIP)]
    print(f"  Found {len(filtered)} producers")
    return filtered


def main():
    vinnatur      = scrape_vinnatur()
    gladvin       = scrape_gladvin()
    louisdressner = scrape_louisdressner()
    winetrade     = scrape_winetrade()

    all_producers = vinnatur + gladvin + louisdressner + winetrade

    seen = {}
    merged = []
    for p in all_producers:
        key = normalize(p["name"])
        if key not in seen:
            seen[key] = True
            merged.append(p)

    merged.sort(key=lambda p: p["name"].lower())

    by_source = {}
    for p in merged:
        by_source[p["source"]] = by_source.get(p["source"], 0) + 1

    print(f"\n{'─'*60}")
    for s, n in by_source.items():
        print(f"  {s:<25} {n:>4} producers")
    print(f"  {'unique total':<25} {len(merged):>4} producers")
    print(f"{'─'*60}")

    DATA.mkdir(exist_ok=True)
    (DATA / "producers.json").write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\nSaved → data/producers.json")


if __name__ == "__main__":
    main()

