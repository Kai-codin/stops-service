print("[Australia.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import zipfile
import csv
import io

print("[Australia.py] Imports done", flush=True)

Australia_ENDPOINTS = [
    "https://s3.transitpdf.com/files/uran/improved-gtfs-hobart.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-launceston.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-greater-sydney.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ptv-interstate.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ptv-metropolitan-bus.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ptv-metropolitan-train.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ptv-metropolitan-tram.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ptv-regional-bus.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ptv-skybus.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-adelaidemetro.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-port-phillip-ferries.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-bow-qconnect.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-bun-qconnect.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-cns.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gym.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-inn-gtfs.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-kil-christensens.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-lgt-queensland.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mag-qconnect.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mal-qconnect.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mhb.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mif.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mky.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-north-stradbroke.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-rockhampton.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-seq.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-tsv.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-twb.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-war.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-wht.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-wlb.zip"
]


async def fetch_Australia(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Australia stops from GTFS ZIP feeds.

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[Australia.py] fetch_Australia: Starting fetch from Australia...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        stops_by_id: Dict[str, Dict[str, Any]] = {}

        for endpoint in Australia_ENDPOINTS:
            print(f"[Australia.py] fetch_Australia: Downloading {endpoint}", flush=True)

            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
            except Exception as e:
                print(f"[Australia.py] ⚠️ Failed to download {endpoint}: {e}", flush=True)
                continue

            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    if "stops.txt" not in z.namelist():
                        print("[Australia.py] ⚠️ stops.txt not found in archive", flush=True)
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
                                "source": "Australia",
                            }

            except zipfile.BadZipFile as e:
                print(f"[Australia.py] ⚠️ Bad ZIP file: {e}", flush=True)
            except Exception as e:
                print(f"[Australia.py] ⚠️ Error parsing GTFS: {e}", flush=True)

        results = list(stops_by_id.values())

        print(
            f"[Australia.py] fetch_Australia: Fetched {len(results)} Australia stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[Australia.py] fetch_Australia: Client closed", flush=True)
