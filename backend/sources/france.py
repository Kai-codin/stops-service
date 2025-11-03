# france.py
print("[france.py] Module loading...", flush=True)

from typing import List, Dict, Any, Optional
import httpx
import asyncio

print("[france.py] Imports done", flush=True)

FRANCE_ENDPOINT = "https://transport.data.gouv.fr/api/gtfs-stops"


async def fetch_france(
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 60,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    print("[france.py] fetch_france: Starting fetch from transport.data.gouv.fr...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    # Use smaller tiles (1°×1° grid)
    overlap = 0.05
    lat_ranges = [(lat - overlap, lat + 0.5 + overlap) for lat in range(41, 52)]
    lon_ranges = [(lon - overlap, lon + 0.5 + overlap) for lon in range(-6, 10)]

    results: List[Dict[str, Any]] = []

    try:
        for south, north in lat_ranges:
            for west, east in lon_ranges:
                params = {"south": south, "north": north, "west": west, "east": east}

                if debug:
                    print(f"[france.py] Fetching bbox {west},{south},{east},{north}", flush=True)

                try:
                    resp = await client.get(
                        FRANCE_ENDPOINT,
                        params=params,
                        headers={"accept": "application/json"},
                    )

                    if resp.status_code == 422:
                        print(f"[france.py] ⚠️ Invalid bbox {west},{south},{east},{north}", flush=True)
                        continue
                    if resp.status_code != 200:
                        print(f"[france.py] Skipping bbox {west},{south},{east},{north} - HTTP {resp.status_code}", flush=True)
                        continue

                    data = resp.json()
                    features = data.get("features", [])
                    for f in features:
                        props = f.get("properties", {})
                        coords = f.get("geometry", {}).get("coordinates", [None, None])
                        lon, lat = coords or (None, None)
                        if lat is None or lon is None:
                            continue

                        stop_id = props.get("stop_id") or props.get("id") or ""
                        name = props.get("stop_name") or ""
                        results.append({
                            "id": f"fr:{stop_id}",
                            "name": name,
                            "lat": lat,
                            "lon": lon,
                            "bearing": "",
                            "source": "france",
                        })

                    print(f"[france.py] ✅ {len(features)} stops from {west},{south},{east},{north}", flush=True)

                except Exception as e:
                    print(f"[france.py] ⚠️ Error fetching {west},{south},{east},{north}: {e}", flush=True)

        print(f"[france.py] Total collected {len(results)} stops across {len(lat_ranges)*len(lon_ranges)} tiles", flush=True)
        return results

    finally:
        if close_client:
            await client.aclose()
