print("[singapore.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx

print("[singapore.py] Imports done", flush=True)

SINGAPORE_DATASET_ID = "d_3f172c6feb3f4f92a2f47d93eed2908a"
SINGAPORE_API_URL = "https://api-open.data.gov.sg/v1/public/api/datasets"


async def fetch_singapore(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch Singapore bus stops from data.gov.sg API (GeoJSON format).

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[singapore.py] fetch_singapore: Starting fetch from Singapore...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        stops_by_id: Dict[str, Dict[str, Any]] = {}

        # Step 1: Get the download URL
        poll_url = f"{SINGAPORE_API_URL}/{SINGAPORE_DATASET_ID}/poll-download"
        print(f"[singapore.py] fetch_singapore: Getting download URL from {poll_url}", flush=True)

        try:
            resp = await client.get(poll_url)
            resp.raise_for_status()
            poll_data = resp.json()
        except Exception as e:
            print(f"[singapore.py] ⚠️ Failed to get download URL: {e}", flush=True)
            return []

        if poll_data.get("code") != 0:
            print(f"[singapore.py] ⚠️ API error: {poll_data.get('errMsg', 'Unknown error')}", flush=True)
            return []

        download_url = poll_data.get("data", {}).get("url")
        if not download_url:
            print("[singapore.py] ⚠️ No download URL in response", flush=True)
            return []

        # Step 2: Download the GeoJSON data
        print(f"[singapore.py] fetch_singapore: Downloading GeoJSON from {download_url}", flush=True)

        try:
            resp = await client.get(download_url)
            resp.raise_for_status()
            geojson = resp.json()
        except Exception as e:
            print(f"[singapore.py] ⚠️ Failed to download GeoJSON: {e}", flush=True)
            return []

        # Step 3: Parse GeoJSON features
        features = geojson.get("features", [])
        print(f"[singapore.py] fetch_singapore: Parsing {len(features)} features", flush=True)

        for feature in features:
            geometry = feature.get("geometry", {})
            properties = feature.get("properties", {})

            # GeoJSON coordinates are [longitude, latitude]
            coordinates = geometry.get("coordinates", [])
            if len(coordinates) < 2:
                continue

            lon_f = coordinates[0]
            lat_f = coordinates[1]

            try:
                lon_f = float(lon_f)
                lat_f = float(lat_f)
            except (ValueError, TypeError):
                continue

            # Skip invalid coordinates
            if lat_f == 0 and lon_f == 0:
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

            stop_id = str(properties.get("BUS_STOP_NUM", ""))
            if not stop_id:
                continue

            # Use bus stop number as name since no description is provided
            name = stop_id

            stops_by_id[stop_id] = {
                "id": stop_id,
                "name": name,
                "lat": lat_f,
                "lon": lon_f,
                "bearing": "",
                "source": "singapore",
            }

        results = list(stops_by_id.values())

        print(
            f"[singapore.py] fetch_singapore: Fetched {len(results)} Singapore stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[singapore.py] fetch_singapore: Client closed", flush=True)
