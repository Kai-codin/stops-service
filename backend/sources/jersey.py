print("[jersey.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx

print("[jersey.py] Imports done", flush=True)

JERSEY_ENDPOINT = (
    "https://raw.githubusercontent.com/jclgoodwin/bustimes.org/refs/heads/main/busstops/jersey-bus-stops.json"
)


async def fetch_jersey(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Jersey bus stops from JSON feed.

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[jersey.py] fetch_jersey: Starting fetch from jersey...", flush=True)

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
            print(f"[jersey.py] ❌ Failed to fetch Jersey data: {e}", flush=True)
            return []

        stops = data.get("stops", [])
        print(f"[jersey.py] fetch_jersey: Got {len(stops)} raw stops", flush=True)

        results: List[Dict[str, Any]] = []

        def in_bbox(lat: float, lon: float) -> bool:
            if None in (min_lat, max_lat, min_lon, max_lon):
                return True
            return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

        for s in stops:
            stop_id = s.get("StopNumber")
            lat = s.get("Latitude")
            lon = s.get("Longitude")

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
                "id": f"jersey:{stop_id}",  # namespace-safe
                "name": s.get("StopName", ""),
                "lat": lat_f,
                "lon": lon_f,
                "bearing": "",
                "source": "jersey",
            }

            results.append(normalized)

        print(
            f"[jersey.py] fetch_jersey: Fetched {len(results)} jersey stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[jersey.py] fetch_jersey: Client closed", flush=True)
