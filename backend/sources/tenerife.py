# tenerife.py
print("[tenerife.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import math
import json
import zipfile

print("[tenerife.py] Imports done", flush=True)

tenerife_ENDPOINTS = {
    "https://datos.tenerife.es/ckan/dataset/36c2e26f-0d18-4b5a-b214-1636168e0765/resource/9f291323-8b78-453a-9008-4f0e3bfb3ce3/download/fichero-zip-de-google-transit.zip",

}


async def fetch_tenerife(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch stops via the Digitransit tenerife GraphQL endpoint.

    Note: this uses the simple `stops` query and will filter client-side if bbox provided.
    (If you know a GraphQL argument for bbox on the endpoint, you can adapt the query to pass it.)

    Returns list of dicts with keys: id, name, lat, lon, bearing, source
    """
    print("[tenerife.py] fetch_tenerife: Starting fetch from tenerife...", flush=True)
    close_client = False
    if client is None:
        print("[tenerife.py] fetch_tenerife: Creating temporary client...", flush=True)
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        payload = {
            "data": {
                "stops": {}
            }
        }
        for endpoint in tenerife_ENDPOINTS:
            print(f"[tenerife.py] fetch_tenerife: Posting to {endpoint}...", flush=True)
            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
                filename = endpoint.split('/')[-1]
                with open(filename,'wb') as output_file:
                    output_file.write(resp.content)
                print(f'[tenerife.py] Downloading {filename} completed', flush=True)
                
                try:
                    with zipfile.ZipFile(filename, 'r') as z:
                        # Check if stops.txt exists in archive
                        if 'stops.txt' not in z.namelist():
                            print(f"[tenerife.py] ⚠️ No stops.txt in {filename}, skipping", flush=True)
                            continue
                            
                        with z.open('stops.txt') as f:
                            for line in f:
                                decoded_line = line.decode('utf-8-sig').strip('\r\n')
                                if not decoded_line or decoded_line.startswith("stop_id"):
                                    continue
                                parts = decoded_line.split(',')
                                if len(parts) < 4:
                                    continue
                                stop_id = parts[0]
                                stop_name = parts[1]
                                stop_lat = parts[2]
                                stop_lon = parts[3]
                                payload["data"]["stops"][stop_id] = {
                                    "name": stop_name,
                                    "lat": stop_lat,
                                    "lon": stop_lon
                                }
                except zipfile.BadZipFile as e:
                    print(f"[tenerife.py] ⚠️ Bad zip file {filename}: {e}", flush=True)
                    continue
                except Exception as e:
                    print(f"[tenerife.py] ⚠️ Error processing {filename}: {e}", flush=True)
                    continue
            except Exception as e:
                print(f"[tenerife.py] ⚠️ Error fetching {endpoint}: {e}", flush=True)
                continue

        stops = payload.get("data", {}).get("stops", {})
        print(f"[tenerife.py] fetch_tenerife: Got {len(stops)} stops from GTFS", flush=True)

        stops = json.loads(json.dumps(stops)).values()  # Convert dict_values to list of dicts

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
                "source": "tenerife",
                # you can attach zoneId etc if useful
            }
            results.append(normalized)

        print(f"[tenerife.py] fetch_tenerife: Fetched {len(results)} tenerife stops", flush=True)
        return results      
    finally:
        if close_client:
            print("[tenerife.py] fetch_tenerife: Closing client...", flush=True)
            await client.aclose()
