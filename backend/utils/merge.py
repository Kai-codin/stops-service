# merge.py
print("[merge.py] Module loading started...", flush=True)

import os
import json
import asyncio
import datetime
from pathlib import Path
from typing import List, Dict, Any
import logging

print("[merge.py] Standard library imports done", flush=True)

import asyncpg
import httpx

print("[merge.py] asyncpg and httpx imports done", flush=True)

# Import your source fetchers
# --- OPTIONAL SINGLE SOURCE MODE ---
import sys

SINGLE_SOURCE = None
if len(sys.argv) > 1:
    SINGLE_SOURCE = sys.argv[1].lower()
    print(f"[merge.py] Running in single-source mode: {SINGLE_SOURCE}", flush=True)
else:
    print("[merge.py] Running in all-sources mode", flush=True)

from pathlib import Path

# DEBUG toggle: when True, fetchers should fetch small sample/page only
debug = False

# Add project root to import path (so "sources.*" imports work)
sys.path.append(str(Path(__file__).resolve().parent.parent))

# --- CONFIG ---
print("[merge.py] Config section starting...", flush=True)
DATA_DIR = Path("data")
# Use environment DATABASE_URL if present, otherwise fallback to a common docker-compose name
DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/stops")
print(f"[merge.py] DB_DSN={DB_DSN}", flush=True)
print("[merge.py] Module load complete", flush=True)


async def ensure_dir(path: Path):
    print(f"[merge.py] ensure_dir: {path}", flush=True)
    path.mkdir(parents=True, exist_ok=True)


def save_json_sync(path: Path, data: Any):
    """Synchronous JSON save used by asyncio.to_thread"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


async def save_json(path: Path, data: Any):
    """Offload JSON file writing to a thread"""
    print(f"[merge.py] save_json: {path}", flush=True)
    await asyncio.to_thread(save_json_sync, path, data)


async def dump_source_data(source: str, data: List[Dict[str, Any]]):
    """Save raw source data to data/{source}/{source}-{date}.json"""
    print(f"[merge.py] dump_source_data: source={source}, len={len(data)}", flush=True)
    date = datetime.date.today().strftime("%Y%m%d")
    dir_path = DATA_DIR / source
    await ensure_dir(dir_path)
    file_path = dir_path / f"{source}-{date}.json"
    # use threaded write to avoid blocking event loop
    await save_json(file_path, data)
    print(f"✅ Saved {len(data)} stops for {source} → {file_path}", flush=True)


def normalize_for_db(stop: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reduce to minimal required schema:
    {"bearing": "", "name": "stop name", "location": [lon, lat], "source": "...", "created_at": datetime}
    """
    lon = None
    lat = None
    # allow both location array and lon/lat keys
    if stop.get("location") and isinstance(stop.get("location"), (list, tuple)) and len(stop["location"]) >= 2:
        lon = float(stop["location"][0])
        lat = float(stop["location"][1])
    else:
        # some sources use lon/lat keys
        try:
            lon = float(stop.get("lon")) if stop.get("lon") is not None else None
            lat = float(stop.get("lat")) if stop.get("lat") is not None else None
        except (TypeError, ValueError):
            lon = None
            lat = None

    created = stop.get("created_at")
    if isinstance(created, str):
        try:
            created_at = datetime.datetime.fromisoformat(created)
        except ValueError:
            created_at = datetime.datetime.utcnow()
    elif isinstance(created, datetime.datetime):
        created_at = created
    else:
        created_at = datetime.datetime.utcnow()


    return {
        "bearing": stop.get("bearing", "") or "",
        "name": stop.get("name", "") or stop.get("common_name", "") or "",
        "location": [lon, lat],
        "source": stop.get("source", "") or "",
        "created_at": created_at,
    }


async def save_to_db(stops: List[Dict[str, Any]], source_only: str = None):
    print(f"[merge.py] save_to_db: source_only={source_only}", flush=True)
    """Insert merged stops into PostgreSQL"""
    print(f"[merge.py] save_to_db: connecting to {DB_DSN}", flush=True)
    conn = await asyncpg.connect(DB_DSN)
    print(f"[merge.py] Connected to DB", flush=True)

    # Create table if it doesn't exist
    print(f"[merge.py] Creating stops table if needed...", flush=True)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stops (
            id SERIAL PRIMARY KEY,
            name TEXT,
            bearing TEXT,
            location DOUBLE PRECISION[],
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    print("Ensured stops table exists", flush=True)

    # Clear old data (optional)
    if source_only:
        print(f"[merge.py] Deleting existing stops from source '{source_only}'...", flush=True)
        await conn.execute("DELETE FROM stops WHERE source = $1;", source_only)
        print(f"Deleted existing stops from source '{source_only}'", flush=True)
    else:
        print(f"[merge.py] Truncating stops table...", flush=True)
        await conn.execute("TRUNCATE TABLE stops;")
        print("Truncated stops table", flush=True)

    # Prepare records for insert: ensure location values are proper numeric tuples
    records = []
    for s in stops:
        loc = s.get("location") or [None, None]
        # ensure list of two floats or nulls
        lon = float(loc[0]) if loc[0] is not None else None
        lat = float(loc[1]) if loc[1] is not None else None
        records.append((s.get("name"), s.get("bearing"), [lon, lat], s.get("source"), s.get("created_at")))

    print(f"[merge.py] Inserting {len(records)} stops...", flush=True)

    # Asyncpg supports copy_records_to_table which is faster for bulk, but keep executemany for simplicity
    await conn.executemany(
        "INSERT INTO stops (name, bearing, location, source, created_at) VALUES ($1, $2, $3, $4, $5);",
        records,
    )

    await conn.close()
    print(f"💾 Inserted {len(records)} merged stops into database.", flush=True)


async def fetch_all_sources():
    """Fetch all sources concurrently, or just one if SINGLE_SOURCE is set."""
    print("[merge.py] fetch_all_sources: Starting", flush=True)
    async with httpx.AsyncClient() as client:
        print("[merge.py] fetch_all_sources: AsyncClient created", flush=True)

        # Available fetchers
        available = {}

        try:
            from sources.hsl import fetch_hsl
            available["hsl"] = fetch_hsl
        except Exception as e:
            print(f"[merge.py] HSL import skipped: {e}", flush=True)

        try:
            from sources.uk import fetch_uk
            available["uk"] = fetch_uk
        except Exception as e:
            print(f"[merge.py] UK import skipped: {e}", flush=True)

        try:
            from sources.eu import fetch_eu
            available["eu"] = fetch_eu
        except Exception as e:
            print(f"[merge.py] EU import skipped: {e}", flush=True)

        try:
            from sources.varely import fetch_varely
            available["varely"] = fetch_varely
        except Exception as e:
            print(f"[merge.py] Varely import skipped: {e}", flush=True)

        try:
            from sources.finland import fetch_finland
            available["finland"] = fetch_finland
        except Exception as e:
            print(f"[merge.py] Finland import skipped: {e}", flush=True)

        try:
            from sources.waltti import fetch_waltti
            available["waltti"] = fetch_waltti
        except Exception as e:
            print(f"[merge.py] Waltti import skipped: {e}", flush=True)

        try:
            from sources.france import fetch_france
            available["france"] = fetch_france
        except Exception as e:
            print(f"[merge.py] France import skipped: {e}", flush=True)

        try:
            from sources.italy import fetch_italy
            available["italy"] = fetch_italy
        except Exception as e:
            print(f"[merge.py] Italy import skipped: {e}", flush=True)

        try:
            from sources.slovakia import fetch_slovakia
            available["slovakia"] = fetch_slovakia
        except Exception as e:
            print(f"[merge.py] Slovakia import skipped: {e}", flush=True)

        try:
            from sources.poland import fetch_poland
            available["poland"] = fetch_poland
        except Exception as e:
            print(f"[merge.py] Poland import skipped: {e}", flush=True)

        try:
            from sources.greece import fetch_greece
            available["greece"] = fetch_greece
        except Exception as e:
            print(f"[merge.py] Greece import skipped: {e}", flush=True)

        try:
            from sources.switzerland import fetch_switzerland
            available["switzerland"] = fetch_switzerland
        except Exception as e:
            print(f"[merge.py] Switzerland import skipped: {e}", flush=True)

        try:
            from sources.jersey import fetch_jersey
            available["jersey"] = fetch_jersey
        except Exception as e:
            print(f"[merge.py] Jersey import skipped: {e}", flush=True)

        try:
            from sources.germany import fetch_germany
            available["germany"] = fetch_germany
        except Exception as e:
            print(f"[merge.py] Germany import skipped: {e}", flush=True)

        try:
            from sources.netherlands import fetch_netherlands
            available["netherlands"] = fetch_netherlands
        except Exception as e:
            print(f"[merge.py] Netherlands import skipped: {e}", flush=True)

        # If single-source specified, only run that one
        if SINGLE_SOURCE:
            if SINGLE_SOURCE not in available:
                raise ValueError(f"Unknown source '{SINGLE_SOURCE}'. Available: {list(available.keys())}")
            tasks = {SINGLE_SOURCE: available[SINGLE_SOURCE](client=client, debug=debug)}
        else:
            tasks = {name: fn(client=client, debug=debug) for name, fn in available.items()}

        print(f"[merge.py] fetch_all_sources: Fetching {list(tasks.keys())}", flush=True)
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        print("[merge.py] fetch_all_sources: Tasks complete", flush=True)

        merged: List[Dict[str, Any]] = []
        for (source, result) in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                print(f"⚠️ Error fetching {source}: {result}", flush=True)
                continue

            await dump_source_data(source, result)
            for item in result:
                item.setdefault("source", source)
            merged.extend(result)
            print(f"Fetched {len(result)} stops from {source}", flush=True)

        print(f"[merge.py] fetch_all_sources: Returning {len(merged)} total stops", flush=True)
        return merged


async def main():
    print("[merge.py] main() started", flush=True)
    print("🚀 Fetching and merging stop data...", flush=True)
    data = await fetch_all_sources()

    print(f"📦 Merging {len(data)} total stops...", flush=True)
    # Normalise and drop entries that don't have lat/lon
    normalized = []
    for s in data:
        norm = normalize_for_db(s)
        if norm["location"][0] is not None and norm["location"][1] is not None:
            normalized.append(norm)

    print(f"[merge.py] Normalized {len(normalized)} stops", flush=True)

    print(f"[merge.py] Saving to DB...", flush=True)
    await save_to_db(normalized, source_only=SINGLE_SOURCE)
    print("✅ Merge complete.", flush=True)


if __name__ == "__main__":
    print("[merge.py] __main__ block executing", flush=True)
    asyncio.run(main())
    print("[merge.py] __main__ block complete", flush=True)
