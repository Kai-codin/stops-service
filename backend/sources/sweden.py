print("[sweden.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import zipfile
import csv
import io
import os

print("[sweden.py] Imports done", flush=True)

SWEDEN_GTFS_URL = "https://api.resrobot.se/v2.1/gtfs/sweden.zip"


async def fetch_sweden(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 120,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Sweden stops from ResRobot GTFS feed.

    Requires env var:
        SWEDEN_KEY

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[sweden.py] fetch_sweden: Starting fetch from Sweden...", flush=True)

    api_key = os.getenv("SWEDEN_KEY")
    if not api_key:
        print("[sweden.py] ❌ SWEDEN_KEY env var not set", flush=True)
        return []

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        params = {
            "accessId": api_key
        }

        # Download GTFS ZIP
        try:
            resp = await client.get(SWEDEN_GTFS_URL, params=params)
            resp.raise_for_status()
            zip_bytes = io.BytesIO(resp.content)
        except Exception as e:
            print(f"[sweden.py] ❌ Failed to download Sweden GTFS: {e}", flush=True)
            return []

        stops_by_id: Dict[str, Dict[str, Any]] = {}

        try:
            with zipfile.ZipFile(zip_bytes) as z:
                if "stops.txt" not in z.namelist():
                    print("[sweden.py] ⚠️ stops.txt not found in GTFS", flush=True)
                    return []

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
                        except (ValueError, TypeError):
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
                            "source": "sweden",
                        }

        except zipfile.BadZipFile as e:
            print(f"[sweden.py] ⚠️ Bad ZIP file: {e}", flush=True)
        except Exception as e:
            print(f"[sweden.py] ⚠️ Error parsing Sweden GTFS: {e}", flush=True)

        results = list(stops_by_id.values())

        print(
            f"[sweden.py] fetch_sweden: Fetched {len(results)} Sweden stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[sweden.py] fetch_sweden: Client closed", flush=True)
