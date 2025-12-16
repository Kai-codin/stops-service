print("[italy.py] Module loading...", flush=True)

from typing import List, Optional, Dict, Any
import httpx
import zipfile
import csv
import io

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
    Fetch Italy stops from GTFS ZIP feeds.

    Returns list of dicts with keys:
    id, name, lat, lon, bearing, source
    """
    print("[italy.py] fetch_italy: Starting fetch from italy...", flush=True)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        stops_by_id: Dict[str, Dict[str, Any]] = {}

        for endpoint in ITALY_ENDPOINTS:
            print(f"[italy.py] fetch_italy: Downloading {endpoint}", flush=True)

            try:
                resp = await client.get(endpoint)
                resp.raise_for_status()
            except Exception as e:
                print(f"[italy.py] ⚠️ Failed to download {endpoint}: {e}", flush=True)
                continue

            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    if "stops.txt" not in z.namelist():
                        print("[italy.py] ⚠️ stops.txt not found in archive", flush=True)
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
                                "source": "italy",
                            }

            except zipfile.BadZipFile as e:
                print(f"[italy.py] ⚠️ Bad ZIP file: {e}", flush=True)
            except Exception as e:
                print(f"[italy.py] ⚠️ Error parsing GTFS: {e}", flush=True)

        results = list(stops_by_id.values())

        print(
            f"[italy.py] fetch_italy: Fetched {len(results)} italy stops",
            flush=True,
        )

        return results

    finally:
        if close_client:
            await client.aclose()
            print("[italy.py] fetch_italy: Client closed", flush=True)