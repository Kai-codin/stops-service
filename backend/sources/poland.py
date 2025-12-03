# poland.py
print("[poland.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import math
import json
import zipfile

print("[poland.py] Imports done", flush=True)

poland_ENDPOINTS = {
    "https://s3.transitpdf.com/files/uran/improved-gtfs-bialostocka.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gdynia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-zakroczym.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-kolej-lka.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-koleje-autobusowy.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-koleje-kml.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-koleje.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-elblag.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gzk-bystry.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ka-swinoujscie.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-koleje-dolno.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-koleje-mazowieckie.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mpk-lomza.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mzdik-radom.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mzk-elk.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mzk-wejherowo.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-pkp-intercity.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-um-torun.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-wkd.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-zbiorkom.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-zdmikp.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ztm-kielce.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ztm-lublin.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ztm-rzesz.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ztm-warszawa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-torun-pl.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mpk-wroclaw.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-zarz-d-transportu-miejskiego-w-gda-sku.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-kombus.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-lodz.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mi-kpd.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-bus-krk.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-koleje-wielkopolskie.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mobilis-krk.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-tram-krk.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ztm-zarzad-transportu-metropolitalnego.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-zditm-szczecin.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-poznan.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ztm-poznan.zip"
}


async def fetch_poland(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch stops via the Digitransit poland GraphQL endpoint.

    Note: this uses the simple `stops` query and will filter client-side if bbox provided.
    (If you know a GraphQL argument for bbox on the endpoint, you can adapt the query to pass it.)

    Returns list of dicts with keys: id, name, lat, lon, bearing, source
    """
    print("[poland.py] fetch_poland: Starting fetch from poland...", flush=True)
    close_client = False
    if client is None:
        print("[poland.py] fetch_poland: Creating temporary client...", flush=True)
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        payload = {
            "data": {
                "stops": {}
            }
        }
        for endpoint in poland_ENDPOINTS:
            print(f"[poland.py] fetch_poland: Posting to {endpoint}...", flush=True)
            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
                filename = endpoint.split('/')[-1]
                with open(filename,'wb') as output_file:
                    output_file.write(resp.content)
                print(f'[poland.py] Downloading {filename} completed', flush=True)
                
                try:
                    with zipfile.ZipFile(filename, 'r') as z:
                        # Check if stops.txt exists in archive
                        if 'stops.txt' not in z.namelist():
                            print(f"[poland.py] ⚠️ No stops.txt in {filename}, skipping", flush=True)
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
                    print(f"[poland.py] ⚠️ Bad zip file {filename}: {e}", flush=True)
                    continue
                except Exception as e:
                    print(f"[poland.py] ⚠️ Error processing {filename}: {e}", flush=True)
                    continue
            except Exception as e:
                print(f"[poland.py] ⚠️ Error fetching {endpoint}: {e}", flush=True)
                continue

        stops = payload.get("data", {}).get("stops", {})
        print(f"[poland.py] fetch_poland: Got {len(stops)} stops from GTFS", flush=True)

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
                "source": "poland",
                # you can attach zoneId etc if useful
            }
            results.append(normalized)

        print(f"[poland.py] fetch_poland: Fetched {len(results)} poland stops", flush=True)
        return results      
    finally:
        if close_client:
            print("[poland.py] fetch_poland: Closing client...", flush=True)
            await client.aclose()
