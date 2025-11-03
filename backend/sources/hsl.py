# hsl.py
print("[hsl.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import math

print("[hsl.py] Imports done", flush=True)

HSL_ENDPOINT = "https://api.digitransit.fi/routing/v2/hsl/gtfs/v1?digitransit-subscription-key=a1e437f79628464c9ea8d542db6f6e94"


async def fetch_hsl(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetch stops via the Digitransit HSL GraphQL endpoint.

    Note: this uses the simple `stops` query and will filter client-side if bbox provided.
    (If you know a GraphQL argument for bbox on the endpoint, you can adapt the query to pass it.)

    Returns list of dicts with keys: id, name, lat, lon, bearing, source
    """
    print("[hsl.py] fetch_hsl: Starting fetch from HSL...", flush=True)
    close_client = False
    if client is None:
        print("[hsl.py] fetch_hsl: Creating temporary client...", flush=True)
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        query = """
        {
          stops {
            gtfsId
            name
            lat
            lon
            zoneId
          }
        }
        """

        print(f"[hsl.py] fetch_hsl: Posting to {HSL_ENDPOINT}...", flush=True)
        resp = await client.post(HSL_ENDPOINT, json={"query": query})
        resp.raise_for_status()
        payload = resp.json()

        stops = payload.get("data", {}).get("stops", [])
        print(f"[hsl.py] fetch_hsl: Got {len(stops)} stops from GraphQL", flush=True)
        results: List[Dict[str, Any]] = []

        # Helper: bbox check
        def in_bbox(lat: float, lon: float) -> bool:
            if None in (min_lat, max_lat, min_lon, max_lon):
                return True
            return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

        for s in stops:
            # Some GraphQL stops may have None/invalid coords
            lat = s.get("lat")
            lon = s.get("lon")
            if lat is None or lon is None:
                continue

            try:
                lat = float(lat)
                lon = float(lon)
            except (ValueError, TypeError):
                continue

            if not in_bbox(lat, lon):
                continue

            normalized = {
                "id": s.get("gtfsId"),
                "name": s.get("name") or "",
                "lat": lat,
                "lon": lon,
                "bearing": "",  # GraphQL stops don't include bearing in example
                "source": "hsl",
                # you can attach zoneId etc if useful
            }
            results.append(normalized)

        print(f"[hsl.py] fetch_hsl: Fetched {len(results)} HSL stops", flush=True)
        return results

    finally:
        if close_client:
            print("[hsl.py] fetch_hsl: Closing client...", flush=True)
            await client.aclose()
