# slovakia.py
print("[slovakia.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import math
import json
import zipfile

print("[slovakia.py] Imports done", flush=True)

slovakia_ENDPOINTS = {
    "https://s3.transitpdf.com/files/uran/improved-gtfs-bratislava-sk.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-zsr.zip"
}


async def fetch_slovakia(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch stops via the Digitransit slovakia GraphQL endpoint.

    Note: this uses the simple `stops` query and will filter client-side if bbox provided.
    (If you know a GraphQL argument for bbox on the endpoint, you can adapt the query to pass it.)

    Returns list of dicts with keys: id, name, lat, lon, bearing, source
    """
    print("[slovakia.py] fetch_slovakia: Starting fetch from slovakia...", flush=True)
    close_client = False
    if client is None:
        print("[slovakia.py] fetch_slovakia: Creating temporary client...", flush=True)
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        payload = {
            "data": {
                "stops": {}
            }
        }
        for endpoint in slovakia_ENDPOINTS:
            print(f"[slovakia.py] fetch_slovakia: Posting to {endpoint}...", flush=True)
            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
                filename = endpoint.split('/')[-1]
                with open(filename,'wb') as output_file:
                    output_file.write(resp.content)
                print(f'[slovakia.py] Downloading {filename} completed', flush=True)
                
                try:
                    with zipfile.ZipFile(filename, 'r') as z:
                        # Check if stops.txt exists in archive
                        if 'stops.txt' not in z.namelist():
                            print(f"[slovakia.py] ⚠️ No stops.txt in {filename}, skipping", flush=True)
                            continue
                            
                        with z.open('stops.txt') as f:
                            for line in f:
                                # stops.txt is a CSV file
                                # format "stop_id","stop_code","stop_name","stop_lat","stop_lon","zone_id","stop_url","location_type","parent_station","stop_desc","stop_timezone","wheelchair_boarding","level_id","platform_code"
                                # get stop_name, stop_lat, stop_lon

                                decoded_line = line.decode('utf-8').strip()
                                if decoded_line.startswith("stop_id"):
                                    continue
                                parts = decoded_line.split('","')
                                if len(parts) < 5:
                                    continue
                                stop_id = parts[0].replace('"','')
                                stop_name = parts[2]
                                stop_lat = parts[3]
                                stop_lon = parts[4]
                                payload["data"]["stops"][stop_id] = {
                                    "name": stop_name,
                                    "lat": stop_lat,
                                    "lon": stop_lon
                                }
                except zipfile.BadZipFile as e:
                    print(f"[slovakia.py] ⚠️ Bad zip file {filename}: {e}", flush=True)
                    continue
                except Exception as e:
                    print(f"[slovakia.py] ⚠️ Error processing {filename}: {e}", flush=True)
                    continue
            except Exception as e:
                print(f"[slovakia.py] ⚠️ Error fetching {endpoint}: {e}", flush=True)
                continue

        stops = payload.get("data", {}).get("stops", {})
        print(f"[slovakia.py] fetch_slovakia: Got {len(stops)} stops from GTFS", flush=True)

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
                "source": "slovakia",
                # you can attach zoneId etc if useful
            }
            results.append(normalized)

        print(f"[slovakia.py] fetch_slovakia: Fetched {len(results)} slovakia stops", flush=True)
        return results      
    finally:
        if close_client:
            print("[slovakia.py] fetch_slovakia: Closing client...", flush=True)
            await client.aclose()
