print("[guernsey.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx

print("[guernsey.py] Imports done", flush=True)

GUERNSEY_ENDPOINT = (
    "https://ticketless-app.api.urbanthings.cloud/api/2/transit/stops?densityLevels=high&maxLatitude=49.52455&maxLongitude=-2.4341999999999997&minLatitude=49.38985&minLongitude=-2.6406"
)

"""
Headers for Guernsey API requests:

-H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0' \
-H 'Accept: application/vnd.ticketless.arrivalsList+json; version=3' \
-H 'Accept-Language: en-GB,en;q=0.5' \
-H 'Accept-Encoding: gzip, deflate, br, zstd' \
-H 'x-ut-app: travel.ticketless.app.guernsey;platform=web' \
-H 'x-api-key: TIzVfvPTlb5bjo69rsOPbabDVhwwgSiLaV5MCiME' \
-H 'Origin: https://buses.gg' \
-H 'Sec-GPC: 1' \
-H 'Connection: keep-alive' \
-H 'Referer: https://buses.gg/' \
-H 'Sec-Fetch-Dest: empty' \
-H 'Sec-Fetch-Mode: cors' \
-H 'Sec-Fetch-Site: cross-site' \
-H 'TE: trailers'
"""

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
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0",
                "Accept": "application/vnd.ticketless.arrivalsList+json; version=3",
                "Accept-Language": "en-GB,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "x-ut-app": "travel.ticketless.app.guernsey;platform=web",
                "x-api-key": "TIzVfvPTlb5bjo69rsOPbabDVhwwgSiLaV5MCiME",
                "Origin": "https://buses.gg",
                "Sec-GPC": "1",
                "Connection": "keep-alive",
                "Referer": "https://buses.gg/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "cross-site",
                "TE": "trailers",
            }
            resp = await client.get(GUERNSEY_ENDPOINT, headers=headers)

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
