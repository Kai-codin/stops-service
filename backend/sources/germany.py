print("[germany.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import zipfile
import csv
import io

print("[germany.py] Imports done", flush=True)

GERMANY_GTFS_ZIP = "https://download.gtfs.de/germany/free/latest.zip"


async def fetch_germany(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 60,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Germany stops from the GTFS feed at download.gtfs.de.

    Returns list of dicts with keys: id, name, lat, lon, bearing, source
    """

    print("[germany.py] fetch_germany: Starting fetch from GERMANY GTFS...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        # Download the GTFS ZIP
        try:
            resp = await client.get(GERMANY_GTFS_ZIP)
            resp.raise_for_status()
            zip_bytes = io.BytesIO(resp.content)
        except Exception as e:
            print(f"[germany.py] ❌ Failed to download Germany GTFS: {e}", flush=True)
            return []

        stops_by_id: Dict[str, Dict[str, Any]] = {}

        try:
            with zipfile.ZipFile(zip_bytes) as z:
                if "stops.txt" not in z.namelist():
                    print("[germany.py] ⚠️ stops.txt not found in Germany GTFS", flush=True)
                else:
                    with z.open("stops.txt") as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))

                        for row in reader:
                            stop_id = row.get("stop_id")
                            lat = row.get("stop_lat")
                            lon = row.get("stop_lon")

                            if not stop_id or not lat or not lon:
                                continue

                            try:
                                lat_f = float(lat)
                                lon_f = float(lon)
                            except (ValueError, TypeError):
                                continue

                            # BBOX filter (optional)
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
                                "source": "germany",
                            }

        except zipfile.BadZipFile as e:
            print(f"[germany.py] ⚠️ Bad ZIP file: {e}", flush=True)
        except Exception as e:
            print(f"[germany.py] ⚠️ Error parsing Germany GTFS: {e}", flush=True)

        results = list(stops_by_id.values())

        print(
            f"[germany.py] fetch_germany: Fetched {len(results)} Germany stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[germany.py] fetch_germany: Client closed", flush=True)
