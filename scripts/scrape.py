"""
Scrape natural wine producer lists from:
  - vinnatur.se/bonder/
  - gladvin.dk

Usage:
  python3 scrape.py              # live scrape
  python3 scrape.py --offline    # parse from cached vinnatur.html / gladvin.html

Output: producers.json
"""

import json, re, sys, requests
from bs4 import BeautifulSoup
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
OFFLINE = "--offline" in sys.argv


def get_html(url, filename):
    if OFFLINE:
        p = Path(filename)
        if not p.exists():
            sys.exit(f"ERROR: {filename} not found. Run without --offline first.")
        return p.read_text(encoding="utf-8")
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    Path(filename).write_text(r.text, encoding="utf-8")
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
    print(f"  dropdown-menu uls: {len(all_uls)}")

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

            # Strip region suffix: "Producer, Region, Country" → "Producer"
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
                    "name": name,
                    "country": country,
                    "region": region,
                    "source": "gladvin.dk",
                    "url": "https://gladvin.dk" + href,
                })

    print(f"  Found {len(producers)} producers")
    return producers


def normalize(name):
    """Normalize for cross-source deduplication."""
    name = re.sub(r"\s*\(.*?\)", "", name)
    return name.split(",")[0].strip().lower()


def main():
    vinnatur = scrape_vinnatur()
    gladvin = scrape_gladvin()

    seen = {}
    merged = []
    for p in vinnatur + gladvin:
        key = normalize(p["name"])
        if key not in seen:
            seen[key] = True
            merged.append(p)

    # Sort by name only, regardless of source
    merged.sort(key=lambda p: p["name"].lower())

    print(f"\n{'─'*60}")
    print(f"  vinnatur.se:  {len(vinnatur):>4} producers")
    print(f"  gladvin.dk:   {len(gladvin):>4} producers")
    print(f"  unique total: {len(merged):>4} producers")
    print(f"{'─'*60}")

    Path("producers.json").write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("\nSaved → producers.json")

    print("\nSample (first 20 alphabetically):")
    for p in merged[:20]:
        country = (p["country"] or p["region"] or "?")[:20]
        print(f"  {p['name']:<42} {country:<20} [{p['source']}]")

if __name__ == "__main__":
    main()
