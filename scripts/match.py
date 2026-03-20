"""
Match Systembolaget wines against natural wine producer corpus.
Supports caching so only new producers are sent to Claude.

Usage:
  python3 scripts/match.py
  python3 scripts/match.py --skip-claude
"""

import json, re, sys, os, time, requests
from pathlib import Path

SKIP_CLAUDE = "--skip-claude" in sys.argv
FUZZY_HIGH  = 88
FUZZY_LOW   = 72

STORE_ID    = "0114"  # PK-Huset, Stockholm

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

try:
    from rapidfuzz import fuzz, process
except ImportError:
    sys.exit("Install rapidfuzz: pip install rapidfuzz")

STRIP_WORDS = r"""\b(
    domaine|château|chateau|cave|caves|clos|mas|maison|bodega|bodegas|
    weingut|azienda|agricola|vitivinicola|az|agr|cantina|cantine|
    dominio|domaines|tenuta|fattoria|podere|cascina|
    champagne|établissements|etablissements|
    srl|sarl|s\.a\.|s\.l\.|lda|gmbh|bv|ab|inc|llc|ltd|sas|spa|nv|sa|
    di|du|de|den|des|le|la|les|el|los|las|et|und|and
)\b"""

def normalize(name):
    name = name.lower()
    for a, b in [('é','e'),('è','e'),('ê','e'),('à','a'),('â','a'),('ä','a'),
                 ('ö','o'),('ô','o'),('ü','u'),('î','i'),('ï','i'),('ç','c'),
                 ('ã','a'),('õ','o'),('ñ','n')]:
        name = name.replace(a, b)
    name = re.sub(r"\(.*?\)", "", name)
    name = name.split(",")[0]
    name = re.sub(STRIP_WORDS, " ", name, flags=re.VERBOSE)
    name = re.sub(r"['\"\-\.]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def load_corpus():
    # Use docs/data/producers.json if available (updated corpus), else data/
    corpus_path = DATA.parent / "docs" / "data" / "producers.json"
    if not corpus_path.exists():
        corpus_path = DATA / "producers.json"
    data = json.loads(corpus_path.read_text(encoding="utf-8"))
    return [{"name": p["name"], "normalized": normalize(p["name"]),
             "country": p.get("country"), "source": p["source"]} for p in data]


def combined_score(a, b):
    set_score  = fuzz.token_set_ratio(a, b)
    sort_score = fuzz.token_sort_ratio(a, b)
    tokens_a, tokens_b = len(a.split()), len(b.split())
    if tokens_a < tokens_b:
        ratio = tokens_a / max(tokens_b, 1)
        set_score = set_score * (0.5 + 0.5 * ratio)
    base = 0.55 * set_score + 0.45 * sort_score
    # Require meaningful exact token overlap
    overlap = {t for t in a.split() if len(t) > 3} & {t for t in b.split() if len(t) > 3}
    if not overlap:
        base = min(base, 79.0)
    return base


def fuzzy_match(producer_name, corpus):
    query = normalize(producer_name)
    if not query or len(query) < 3:
        return None, 0
    best_score, best_idx = 0, None
    for i, p in enumerate(corpus):
        score = combined_score(query, p["normalized"])
        if score > best_score:
            best_score, best_idx = score, i
    return (corpus[best_idx], best_score) if best_idx is not None else (None, 0)


def classify_producers(producers_batch):
    api_key = os.environ.get("REPLICATE_API_TOKEN", "")
    if not api_key:
        print("  WARNING: REPLICATE_API_TOKEN not set")
        return {}

    # Clean names that could break JSON generation
    clean_batch = [n.replace("S.L.", "SL").replace("S.A.", "SA").replace("S.S.", "SS").replace("S.r.l.", "Srl") for n in producers_batch]
    names_list = "\n".join(f"- {n}" for n in clean_batch)
    name_map = dict(zip(clean_batch, producers_batch))  # map back to originals
    prompt = f"""You are a natural wine expert. For each producer below, determine if they make natural wine.
Natural wine means: wild/ambient fermentation, minimal or no added sulfites, organic or biodynamic farming, no fining/filtering or minimal, no added yeasts or enzymes.
Be strict - organic alone is NOT enough. The producer must specifically be known for natural winemaking practices.

Respond ONLY with a JSON object. Keys = exact producer names as given. Values = objects with:
- "isNatural": true or false
- "confidence": 0.0 to 1.0 (be conservative, use <0.8 if uncertain)
Producers:
{names_list}

JSON only, no markdown:"""

    r = requests.post(
        "https://api.replicate.com/v1/models/google/gemini-3.1-pro/predictions",
        headers={"Authorization": f"Bearer {api_key}", "content-type": "application/json"},
        json={"input": {
            "prompt": prompt,
            "thinking_level": "low",
            "temperature": 0.1,
            "max_output_tokens": 4000,
        }},
        timeout=60,
    )
    r.raise_for_status()
    prediction_id = r.json().get("id")
    if not prediction_id:
        return {}

    for _ in range(120):
        time.sleep(5)
        poll = requests.get(
            f"https://api.replicate.com/v1/predictions/{prediction_id}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15,
        )
        result = poll.json()
        status = result.get("status")
        if status == "succeeded":
            output = result.get("output", "")
            text = "".join(output) if isinstance(output, list) else str(output)
            break
        elif status in ("failed", "canceled"):
            print(f"  WARNING: Replicate prediction {status}")
            return {}
    else:
        print("  WARNING: Replicate timed out")
        return {}

    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"```\s*$", "", text).strip()
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        text = m.group(0)
    try:
        result = json.loads(text)
        # Map cleaned names back to original names
        return {name_map.get(k, k): v for k, v in result.items()}
    except json.JSONDecodeError:
        print(f"  WARNING: invalid JSON: {text[:80]}")
        return {}


def add_store_stock(wines, api_key, store_id=STORE_ID):
    print(f"\nFetching store stock for {store_id}...")
    base = f"https://api-extern.systembolaget.se/sb-api-ecommerce/v1/stockbalance/store/{store_id}"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    hits = 0
    for i, wine in enumerate(wines):
        try:
            resp = requests.get(f"{base}/{wine['id']}/", headers=headers, timeout=10)
            if resp.status_code == 200:
                stock = resp.json().get("stock", 0)
                wine["inStore"] = stock > 0
                wine["storeStock"] = stock
                if stock > 0:
                    hits += 1
            else:
                wine["inStore"] = False
                wine["storeStock"] = 0
        except Exception as e:
            wine["inStore"] = False
            wine["storeStock"] = 0
        if i % 20 == 19:
            print(f"  [{i+1}/{len(wines)}]...")
        time.sleep(0.15)
    print(f"In store {store_id}: {hits}/{len(wines)}")
    return wines


def main():
    api_key = os.environ.get("SYSTEMBOLAGET_API_KEY", "8d39a7340ee7439f8b4c1e995c8f3e4a")

    corpus = load_corpus()
    wines  = json.loads((DATA / "systembolaget_wines.json").read_text(encoding="utf-8"))

    # Load cache of previously classified producers
    cache_path = DATA / "producer_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}

    # Filter to in-stock only
    in_stock = [w for w in wines
                if not w.get("isCompletelyOutOfStock")
                and not w.get("isTemporaryOutOfStock")
                and not w.get("isDiscontinued")
                and not w.get("isSupplierTemporaryNotAvailable")]

    print(f"Corpus:   {len(corpus)} producers")
    print(f"In stock: {len(in_stock)} wines")

    producers = {}
    for w in in_stock:
        p = w["producer"]
        if p and p not in producers:
            producers[p] = w["country"]
    print(f"Unique producers: {len(producers)}")

    # Step 1: fuzzy + cache
    results = {}
    for_claude = []

    for producer in producers:
        if producer in cache:
            results[producer] = cache[producer]
            continue
        match, score = fuzzy_match(producer, corpus)
        if score >= FUZZY_HIGH:
            results[producer] = {
                "isNatural": True, "confidence": round(score/100, 2),
                "method": "fuzzy", "matchedTo": match["name"],
                "reason": f"Matched '{match['name']}' (score {score:.0f})",
            }
        else:
            for_claude.append(producer)

    cached_count = sum(1 for p in producers if p in cache)
    print(f"\nFrom cache:    {cached_count}")
    print(f"Fuzzy matched: {sum(1 for p, v in results.items() if v['method'] == 'fuzzy')}")
    print(f"For Claude:    {len(for_claude)}")

    # Step 2: Claude for unknowns
    if not SKIP_CLAUDE and for_claude:
        print(f"\nClaude Haiku ({len(for_claude)} producers)...")
        BATCH = 25
        for i in range(0, len(for_claude), BATCH):
            batch = for_claude[i:i+BATCH]
            print(f"  Batch {i//BATCH+1}/{(len(for_claude)-1)//BATCH+1}...")
            verdicts = classify_producers(batch)
            for producer, v in verdicts.items():
                results[producer] = {
                    "isNatural": v.get("isNatural", False),
                    "confidence": v.get("confidence", 0),
                    "method": "claude",
                    "matchedTo": None,
                    "reason": v.get("reason", ""),
                }
            time.sleep(4)  # free tier: max 15 req/min

    for producer in for_claude:
        if producer not in results:
            results[producer] = {"isNatural": False, "confidence": 0,
                                 "method": "unclassified", "matchedTo": None, "reason": ""}

    # Update cache with new results
    cache.update(results)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

    # Load blacklist
    blacklist_path = DATA / "blacklist.json"
    blacklist = set()
    if blacklist_path.exists():
        bl = json.loads(blacklist_path.read_text(encoding="utf-8"))
        blacklist = {p.lower() for p in bl.get("producers", [])}
        print(f"Blacklist: {len(blacklist)} producers")

    # Annotate and save
    EU_COUNTRIES = {
        'Frankrike','Italien','Spanien','Österrike','Tyskland','Portugal',
        'Slovenien','Ungern','Kroatien','Georgien','Grekland','Schweiz',
        'Tjeckien','Slovakien','Moldavien','Bulgarien','Rumänien','Serbien',
        'Armenien','Cypern','Malta','Luxemburg',
    }

    annotated = []
    for w in in_stock:
        v = results.get(w["producer"], {"isNatural": False, "confidence": 0,
                                         "method": "unknown", "matchedTo": None, "reason": ""})
        if (v["isNatural"]
                and v["confidence"] >= 0.70
                and w.get("country") in EU_COUNTRIES
                and w.get("volume", 0) <= 1500
                and w.get("producer", "").lower() not in blacklist):
            annotated.append({
                "id":         w["productId"],
                "num":        w["productNumber"],
                "name":       w["name"],
                "sub":        w.get("subname", ""),
                "producer":   w["producer"],
                "country":    w["country"],
                "cat":        w.get("categoryLevel2", ""),
                "price":      w["price"],
                "vintage":    w.get("vintage"),
                "grapes":     w.get("grapes", []),
                "organic":    w.get("isOrganic", False),
                "conf":       round(v["confidence"], 2),
                "method":     v["method"],
                "assortment": w.get("assortment", ""),
                "inStore":    False,
                "storeStock": 0,
            })

    annotated.sort(key=lambda w: (-w["conf"], w["name"].lower()))

    # Add store stock
    annotated = add_store_stock(annotated, api_key)

    out = DATA.parent / "docs" / "data" / "results.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(annotated, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    in_store_count = sum(1 for w in annotated if w["inStore"])
    print(f"\n{'─'*50}")
    print(f"  Natural wines (EU, ≥70%, ≤1.5L): {len(annotated)}")
    print(f"  In store {STORE_ID}:               {in_store_count}")
    print(f"{'─'*50}")
    print(f"Saved → docs/data/results.json")
    print(f"Saved → data/producer_cache.json ({len(cache)} producers cached)")


if __name__ == "__main__":
    main()
