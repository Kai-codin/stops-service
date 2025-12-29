print("[guernsey.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx

print("[guernsey.py] Imports done", flush=True)

GUERNSEY_ENDPOINT = (
    "https://ticketless-app.api.urbanthings.cloud/api/2/transit/stops?densityLevels=high&maxLatitude=49.5021&maxLongitude=-2.4686&minLatitude=49.4123&minLongitude=-2.6062"
)


async def fetch_guernsey(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Guernsey bus stops from JSON feed.

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[guernsey.py] fetch_guernsey: Starting fetch from guernsey...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        try:
            resp = await client.get(JERSEY_ENDPOINT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[guernsey.py] ❌ Failed to fetch Guernsey data: {e}", flush=True)
            return []

        stops = data.get("items", [])
        print(f"[guernsey.py] fetch_guernsey: Got {len(stops)} raw stops", flush=True)

        results: List[Dict[str, Any]] = []

        def in_bbox(lat: float, lon: float) -> bool:
            if None in (min_lat, max_lat, min_lon, max_lon):
                return True
            return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

        for s in stops:
            stop_id = s.get("primaryCode")
            location = s.get("location")
            lat = location.get("latitude")
            lon = location.get("longitude")

            if stop_id is None or lat is None or lon is None:
                continue

            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except (ValueError, TypeError):
                continue

            if not in_bbox(lat_f, lon_f):
                continue

            normalized = {
                "id": f"guernsey:{stop_id}",  # namespace-safe
                "name": s.get("name", ""),
                "lat": lat_f,
                "lon": lon_f,
                "bearing": "",
                "source": "guernsey",
            }

            results.append(normalized)

        print(
            f"[guernsey.py] fetch_guernsey: Fetched {len(results)} guernsey stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[jersey.py] fetch_jersey: Client closed", flush=True)
