# poland.py
print("[poland.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import csv
import io
import zipfile
import traceback

print("[poland.py] Imports done", flush=True)

poland_ENDPOINTS = {
    # TODO: Replace with actual Poland GTFS feed URL.
    # Currently set to Auckland, NZ as a placeholder.
    "https://gtfs.at.govt.nz/gtfs.zip"
}


async def fetch_poland(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 60,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Poland stops from GTFS ZIP feeds.

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[poland.py] fetch_poland: Starting fetch from Poland...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        stops_by_id: Dict[str, Dict[str, Any]] = {}

        for endpoint in poland_ENDPOINTS:
            print(f"[poland.py] fetch_poland: Downloading {endpoint}", flush=True)

            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
            except Exception as e:
                print(
                    f"[poland.py] ⚠️ Failed to download {endpoint}: "
                    f"{type(e).__name__}: {e}",
                    flush=True,
                )
                if debug:
                    traceback.print_exc()
                continue

            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    if "stops.txt" not in z.namelist():
                        print("[poland.py] ⚠️ stops.txt not found in archive", flush=True)
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
                                "source": "poland",
                            }

            except zipfile.BadZipFile as e:
                print(f"[poland.py] ⚠️ Bad ZIP file: {e}", flush=True)
            except Exception as e:
                print(f"[poland.py] ⚠️ Error parsing GTFS: {type(e).__name__}: {e}", flush=True)
                if debug:
                    traceback.print_exc()

        results = list(stops_by_id.values())

        print(
            f"[poland.py] fetch_poland: Fetched {len(results)} Poland stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[poland.py] fetch_poland: Client closed", flush=True)
