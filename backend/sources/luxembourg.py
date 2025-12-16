print("[luxembourg.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import zipfile
import csv
import io
import re

print("[luxembourg.py] Imports done", flush=True)

LUX_DATASET_PAGE = (
    "https://data.public.lu/en/datasets/"
    "horaires-et-arrets-des-transport-publics-gtfs/"
)

GTFS_URL_REGEX = re.compile(
    r"https://download\.data\.public\.lu/resources/"
    r"horaires-et-arrets-des-transport-publics-gtfs/[^\"']+\.zip"
)


async def _get_latest_lux_gtfs_url(client: httpx.AsyncClient) -> Optional[str]:
    """
    Scrape data.public.lu dataset page and return the latest GTFS ZIP URL.
    """
    try:
        resp = await client.get(LUX_DATASET_PAGE)
        resp.raise_for_status()
    except Exception as e:
        print(f"[luxembourg.py] ❌ Failed to fetch dataset page: {e}", flush=True)
        return None

    match = GTFS_URL_REGEX.search(resp.text)
    if not match:
        print("[luxembourg.py] ❌ No GTFS ZIP link found on page", flush=True)
        return None

    return match.group(0)


async def fetch_luxembourg(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 60,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Luxembourg stops from the national GTFS feed.

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[luxembourg.py] fetch_luxembourg: Starting fetch...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        gtfs_url = await _get_latest_lux_gtfs_url(client)
        if not gtfs_url:
            return []

        if debug:
            print(f"[luxembourg.py] Using GTFS URL: {gtfs_url}", flush=True)

        # Download GTFS ZIP
        try:
            resp = await client.get(gtfs_url)
            resp.raise_for_status()
            zip_bytes = io.BytesIO(resp.content)
        except Exception as e:
            print(f"[luxembourg.py] ❌ Failed to download GTFS ZIP: {e}", flush=True)
            return []

        stops_by_id: Dict[str, Dict[str, Any]] = {}

        try:
            with zipfile.ZipFile(zip_bytes) as z:
                if "stops.txt" not in z.namelist():
                    print("[luxembourg.py] ⚠️ stops.txt not found in GTFS", flush=True)
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
                            "source": "luxembourg",
                        }

        except zipfile.BadZipFile as e:
            print(f"[luxembourg.py] ⚠️ Bad ZIP file: {e}", flush=True)
        except Exception as e:
            print(f"[luxembourg.py] ⚠️ Error parsing GTFS: {e}", flush=True)

        results = list(stops_by_id.values())

        print(
            f"[luxembourg.py] fetch_luxembourg: Fetched {len(results)} Luxembourg stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[luxembourg.py] fetch_luxembourg: Client closed", flush=True)
