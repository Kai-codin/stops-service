# ukbuses.py
print("[uk.py] Module loading...", flush=True)

import asyncio
from typing import List, Optional, Dict, Any
import httpx

print("[uk.py] Imports done", flush=True)


UKBUSES_BASE = "https://ukbuses.org/api/stops/"


async def fetch_uk(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetch stops from ukbuses.org and return a normalized list.

    Parameters
    ----------
    min_lat, max_lat, min_lon, max_lon : optional
        If provided, these will be added as bbox query params. NOTE:
        ukbuses expects:
            ymin = min_lat
            ymax = max_lat
            xmin = min_lon
            xmax = max_lon
    client : optional httpx.AsyncClient
        If omitted a temporary client will be used (and closed).
    timeout : request timeout seconds

    Returns
    -------
    list of dicts with keys: id, name, lat, lon, bearing, source
    """
    print("[uk.py] fetch_uk: Starting fetch from ukbuses.org...", flush=True)
    close_client = False
    if client is None:
        print("[uk.py] fetch_uk: Creating temporary client...", flush=True)
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        params = {}
        if None not in (min_lat, max_lat, min_lon, max_lon):
            # mapping to ukbuses param names (based on observed working example)
            params = {
                "ymin": str(min_lat),
                "ymax": str(max_lat),
                "xmin": str(min_lon),
                "xmax": str(max_lon),
            }
            print(f"[uk.py] fetch_uk: Using bbox params: {params}", flush=True)

        url = UKBUSES_BASE
        results: List[Dict[str, Any]] = []

        while url:
            print(f"[uk.py] fetch_uk: Fetching from {url}...", flush=True)
            resp = await client.get(url, params=params if url == UKBUSES_BASE else None)
            resp.raise_for_status()
            data = resp.json()

            # Expecting paginated object with "results" list and "next" link
            page_results = data.get("results", [])
            print(f"[uk.py] fetch_uk: Got {len(page_results)} results from this page", flush=True)
            for item in page_results:
                # Normalise: ukbuses uses "location": [lon, lat]
                loc = item.get("location") or []
                lon = None
                lat = None
                if isinstance(loc, (list, tuple)) and len(loc) >= 2:
                    lon = float(loc[0])
                    lat = float(loc[1])

                normalized = {
                    "id": item.get("atco_code") or item.get("naptan_code") or None,
                    "name": item.get("name") or item.get("common_name") or item.get("long_name") or "",
                    "lat": lat,
                    "lon": lon,
                    "bearing": item.get("bearing", "") or "",
                    "source": "ukbuses",
                    # you can copy other fields here if needed
                }
                results.append(normalized)

            # pagination: server returns "next" (full url) or None
            next_url = data.get("next")
            if not next_url:
                print(f"[uk.py] fetch_uk: No more pages", flush=True)
                break

            # Clear params for subsequent pages; 'next' already contains querystring
            url = next_url
            params = None

        print(f"[uk.py] fetch_uk: Fetched {len(results)} UK stops from ukbuses.org", flush=True)
        return results

    finally:
        if close_client:
            print("[uk.py] fetch_uk: Closing client...", flush=True)
            await client.aclose()
