print("[switzerland.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import json

print("[switzerland.py] Imports done", flush=True)

# Single JSON endpoint
SWITZERLAND_JSON_URL = (
    "https://data.oev-info.ch/api/explore/v2.1/catalog/datasets/stop-points-today/exports/json?lang=en&timezone=Europe%2FZurich"
)


async def fetch_switzerland(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Swiss stops from the national stop-points JSON feed.

    Returns list of dicts with:
      id, name, lat, lon, bearing, source
    """
    print("[switzerland.py] fetch_switzerland: Starting JSON fetch...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        print(f"[switzerland.py] fetch_switzerland: GET {SWITZERLAND_JSON_URL}", flush=True)
        resp = await client.get(SWITZERLAND_JSON_URL)
        resp.raise_for_status()

        try:
            raw_data = resp.json()
        except Exception as e:
            print(f"[switzerland.py] ERROR: Could not decode JSON: {e}", flush=True)
            return []

        print(f"[switzerland.py] JSON received, {len(raw_data)} stop records", flush=True)

        results: List[Dict[str, Any]] = []

        # Bounding box helper
        def in_bbox(lat: float, lon: float) -> bool:
            if None in (min_lat, max_lat, min_lon, max_lon):
                return True
            return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

        for stop in raw_data:
            # Required fields
            name = stop.get("designationofficial")
            coords = stop.get("hyperlink_geographie") or {}
            lat = coords.get("lat")
            lon = coords.get("lon")

            if name is None or lat is None or lon is None:
                continue

            # Convert to float
            try:
                lat = float(lat)
                lon = float(lon)
            except (ValueError, TypeError):
                continue

            if not in_bbox(lat, lon):
                continue

            stop_id = stop.get("sloid") or stop.get("number") or stop.get("numbershort")

            normalized = {
                "id": stop_id,
                "name": name,
                "lat": lat,
                "lon": lon,
                "bearing": "",       # no bearing info in JSON
                "source": "switzerland-json",
            }

            results.append(normalized)

        print(f"[switzerland.py] fetch_switzerland: Returning {len(results)} stops", flush=True)
        return results

    finally:
        if close_client:
            await client.aclose()
            print("[switzerland.py] fetch_switzerland: Client closed", flush=True)
