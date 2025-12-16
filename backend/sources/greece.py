print("[greece.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import zipfile
import csv
import io

print("[greece.py] Imports done", flush=True)

GREECE_ENDPOINTS = [
    "https://s3.transitpdf.com/files/uran/improved-gtfs-athens-urban-transport-organisation.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-swb-athensurbantransportorga.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-trainose.zip",
]


async def fetch_greece(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Greece stops from GTFS ZIP feeds.

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[greece.py] fetch_greece: Starting fetch from greece...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        stops_by_id: Dict[str, Dict[str, Any]] = {}

        for endpoint in GREECE_ENDPOINTS:
            print(f"[greece.py] fetch_greece: Downloading {endpoint}", flush=True)

            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
            except Exception as e:
                print(f"[greece.py] ⚠️ Failed to download {endpoint}: {e}", flush=True)
                continue

            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    if "stops.txt" not in z.namelist():
                        print("[greece.py] ⚠️ stops.txt not found in archive", flush=True)
                        continue

                    with z.open("stops.txt") as f:
                        reader = csv.DictReader(
                            io.TextIOWrapper(f, encoding="utf-8")
                        )

                        for row in reader:
                            stop_id = row.get("stop_id")
                            lat = row.get("stop_lat")
                            lon = row.get("stop_lon")

                            if not stop_id or not lat or not lon:
                                continue

                            try:
                                lat_f = float(lat)
                                lon_f = float(lon)
                            except ValueError:
                                continue

                            # Optional bbox filter
                            if (
                                min_lat is not None
                                and max_lat is not None
                                and min_lon is not None
                                and max_lon is not None
                            ):
                                if not (
                                    min_lat <= lat_f <= max_lat
                                    and min_lon <= lon_f <= max_lon
                                ):
                                    continue

                            stops_by_id[stop_id] = {
                                "id": stop_id,
                                "name": row.get("stop_name", ""),
                                "lat": lat_f,
                                "lon": lon_f,
                                "bearing": "",
                                "source": "greece",
                            }

            except zipfile.BadZipFile as e:
                print(f"[greece.py] ⚠️ Bad ZIP file: {e}", flush=True)
            except Exception as e:
                print(f"[greece.py] ⚠️ Error parsing GTFS: {e}", flush=True)

        results = list(stops_by_id.values())

        print(
            f"[greece.py] fetch_greece: Fetched {len(results)} greece stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[greece.py] fetch_greece: Client closed", flush=True)
