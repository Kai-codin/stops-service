# italy.py
print("[italy.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import math
import json
import zipfile

print("[italy.py] Imports done", flush=True)

ITALY_ENDPOINTS = {
    "https://s3.transitpdf.com/files/uran/improved-gtfs-atp-nuoro.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ferrotramviaria-spa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gtt.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-navigazione2.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-debole-extraurbano-cev.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-debole-extraurbano-livorno.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-debole-extraurbano-pistoia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-debole-extraurbano-prato.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-debole-urbano-pisa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-civitella-in-valdichiana.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-lucca.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-massa-carrara.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-monte-sansavino.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-pisa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-sansepolcro.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-siena.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-extraurbano-unione-comunidel-casentino.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-urbano-pontedera.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-emg-urbano-volterra.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanoarezzo.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanoempolese.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanofirenze.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanogrosseto.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanolivorno.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanolucca.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanomassacar.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanopisa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanopistoia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbanoprato.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gest.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-lineeregionali.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-servizi-area-poggibonsi.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-servizi-sostitutivi-ferro.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-tft.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-toremar.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-trenitalia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanoareametropoli.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanoarezzo.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanocarrara.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanocecina.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanocertaldo.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanochiancianoter.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanochiusi.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanocollevaldelsa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanoempoli.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanofollonica.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanogrosseto.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanointercomunale.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanointercomunale.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanolivorno.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanolucca.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanomassa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanomassaecarrara.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanomontecatinite.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanomontepulciano.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanopescia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanopiombino.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanopisa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanopistoia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanoportoferraio.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanoprato.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanorosignanomari.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanosangimignano.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanosiena.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-urbanoviareggio.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-dati-aspo.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-orari-traghetti.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-orari-trenitalia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-rome-static-gtfs.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-societ-gestione-multipla-spa.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-brindisi.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-core-calabria.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-extraurbano.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-francavilla.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-piemonte-autobus-regionali.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-stpbrindisi-it-1.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-wimob-it-1.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gommagtfsbo.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gommagtfsfe.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-gtfsmex.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-lombardy-trenord.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-tte-extraurbano.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-tte-urbano.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-amtgenova.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-arst-cagliari-it.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-amat.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-mobilitadimarca2.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-alilaguna.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-autolinee-varesine.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-automobilistico.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-cagliari.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-ctmcagliari.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-dati-grimaldi.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-dati-privati.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-olbia.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-azienda-catania.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-movibus-srl.zip",
    "https://s3.transitpdf.com/files/uran/improved-gtfs-actv.zip"
}


async def fetch_italy(
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
    timeout: int = 30,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch stops via the Digitransit ITALY GraphQL endpoint.

    Note: this uses the simple `stops` query and will filter client-side if bbox provided.
    (If you know a GraphQL argument for bbox on the endpoint, you can adapt the query to pass it.)

    Returns list of dicts with keys: id, name, lat, lon, bearing, source
    """
    print("[italy.py] fetch_italy: Starting fetch from ITALY...", flush=True)
    close_client = False
    if client is None:
        print("[italy.py] fetch_italy: Creating temporary client...", flush=True)
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        payload = {
            "data": {
                "stops": {}
            }
        }
        for endpoint in ITALY_ENDPOINTS:
            print(f"[italy.py] fetch_italy: Posting to {endpoint}...", flush=True)
            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
                filename = endpoint.split('/')[-1]
                with open(filename,'wb') as output_file:
                    output_file.write(resp.content)
                print(f'[italy.py] Downloading {filename} completed', flush=True)
                
                try:
                    with zipfile.ZipFile(filename, 'r') as z:
                        # Check if stops.txt exists in archive
                        if 'stops.txt' not in z.namelist():
                            print(f"[italy.py] ⚠️ No stops.txt in {filename}, skipping", flush=True)
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
                    print(f"[italy.py] ⚠️ Bad zip file {filename}: {e}", flush=True)
                    continue
                except Exception as e:
                    print(f"[italy.py] ⚠️ Error processing {filename}: {e}", flush=True)
                    continue
            except Exception as e:
                print(f"[italy.py] ⚠️ Error fetching {endpoint}: {e}", flush=True)
                continue

        stops = payload.get("data", {}).get("stops", {})
        print(f"[italy.py] fetch_italy: Got {len(stops)} stops from GTFS", flush=True)

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
                "source": "italy",
                # you can attach zoneId etc if useful
            }
            results.append(normalized)

        print(f"[italy.py] fetch_italy: Fetched {len(results)} ITALY stops", flush=True)
        return results      
    finally:
        if close_client:
            print("[italy.py] fetch_italy: Closing client...", flush=True)
            await client.aclose()
