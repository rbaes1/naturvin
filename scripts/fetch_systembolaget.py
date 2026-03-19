"""
Fetch wines from Systembolaget (fast + tillfälligt sortiment).
Reads API key from SYSTEMBOLAGET_API_KEY env var.
"""

import json, time, requests, os, sys
from pathlib import Path

API_KEY  = os.environ.get("SYSTEMBOLAGET_API_KEY", "8d39a7340ee7439f8b4c1e995c8f3e4a")
BASE_URL = "https://api-extern.systembolaget.se/sb-api-ecommerce/v1/productsearch/search"
HEADERS  = {"Ocp-Apim-Subscription-Key": API_KEY, "User-Agent": "Mozilla/5.0"}

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

ASSORTMENTS = ["Fast sortiment", "Tillfälligt sortiment"]


def fetch_assortment(assortment_text):
    wines, page, total = [], 1, None
    while True:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=15, params={
            "categoryLevel1": "Vin", "assortmentText": assortment_text,
            "size": 30, "page": page,
        })
        r.raise_for_status()
        data = r.json()
        if total is None:
            total = data.get("metadata", {}).get("docCount", "?")
            print(f"    Total: {total}")
        products = data.get("products", [])
        if not products:
            break
        wines.extend(products)
        if len(products) < 30 or (isinstance(total, int) and len(wines) >= total):
            break
        page += 1
        time.sleep(0.2)
    return wines


def main():
    all_wines = {}
    for assortment in ASSORTMENTS:
        print(f"Fetching '{assortment}'...")
        for w in fetch_assortment(assortment):
            pid = w.get("productId")
            if pid and pid not in all_wines:
                all_wines[pid] = w
        print(f"  Running total: {len(all_wines)}")

    slim = []
    for w in all_wines.values():
        slim.append({
            "productId":   w.get("productId"),
            "productNumber": w.get("productNumberShort"),
            "name":        w.get("productNameBold", ""),
            "subname":     w.get("productNameThin", ""),
            "producer":    w.get("producerName", ""),
            "supplier":    w.get("supplierName", ""),
            "country":     w.get("country", ""),
            "categoryLevel1": w.get("categoryLevel1", ""),
            "categoryLevel2": w.get("categoryLevel2", ""),
            "categoryLevel3": w.get("categoryLevel3", ""),
            "assortment":  w.get("assortmentText", ""),
            "price":       w.get("price"),
            "volume":      w.get("volume"),
            "vintage":     w.get("vintage"),
            "grapes":      w.get("grapes", []),
            "isOrganic":   w.get("isOrganic", False),
            "isCompletelyOutOfStock":       w.get("isCompletelyOutOfStock", False),
            "isTemporaryOutOfStock":        w.get("isTemporaryOutOfStock", False),
            "isDiscontinued":               w.get("isDiscontinued", False),
            "isSupplierTemporaryNotAvailable": w.get("isSupplierTemporaryNotAvailable", False),
            "productLaunchDate": w.get("productLaunchDate", ""),
        })

    slim.sort(key=lambda w: w["name"].lower())
    DATA.mkdir(exist_ok=True)
    (DATA / "systembolaget_wines.json").write_text(
        json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\nSaved {len(slim)} wines → data/systembolaget_wines.json")


if __name__ == "__main__":
    main()
