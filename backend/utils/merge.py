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
import sys
from pathlib import Path

# Add project root to import path
sys.path.append(str(Path(__file__).resolve().parent.parent))

print("[merge.py] About to import fetch_uk...", flush=True)
from sources.uk import fetch_uk
print("[merge.py] fetch_uk imported", flush=True)

print("[merge.py] About to import fetch_hsl...", flush=True)
from sources.hsl import fetch_hsl
print("[merge.py] fetch_hsl imported", flush=True)

# --- CONFIG ---
print("[merge.py] Config section starting...", flush=True)
DATA_DIR = Path("data")
DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/stops")
print(f"[merge.py] DB_DSN={DB_DSN}", flush=True)
print("[merge.py] Module load complete", flush=True)


async def ensure_dir(path: Path):
    print(f"[merge.py] ensure_dir: {path}", flush=True)
    path.mkdir(parents=True, exist_ok=True)


async def save_json(path: Path, data: Any):
    print(f"[merge.py] save_json: {path}", flush=True)
    async with asyncio.to_thread(open, path, "w") as f:
        json.dump(data, f, indent=2)


async def dump_source_data(source: str, data: List[Dict[str, Any]]):
    """Save raw source data to data/{source}/{source}-{date}.json"""
    print(f"[merge.py] dump_source_data: source={source}, len={len(data)}", flush=True)
    date = datetime.date.today().strftime("%Y%m%d")
    dir_path = DATA_DIR / source
    await ensure_dir(dir_path)
    file_path = dir_path / f"{source}-{date}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved {len(data)} stops for {source} → {file_path}", flush=True)


def normalize_for_db(stop: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reduce to minimal required schema:
    {"bearing": "", "name": "stop name", "location": [lon, lat]}
    """
    # print(f"[merge.py] normalize_for_db: {stop.get('name')}", flush=True)  # verbose
    return {
        "bearing": stop.get("bearing", ""),
        "name": stop.get("name", ""),
        "location": [
            float(stop["lon"]) if stop.get("lon") is not None else None,
            float(stop["lat"]) if stop.get("lat") is not None else None,
        ],
    }


async def save_to_db(stops: List[Dict[str, Any]]):
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
            location DOUBLE PRECISION[]
        );
        """
    )
    print("Ensured stops table exists", flush=True)

    # Clear old data (optional)
    print(f"[merge.py] Truncating stops table...", flush=True)
    await conn.execute("TRUNCATE TABLE stops;")
    print("Truncated stops table", flush=True)

    # Bulk insert
    print(f"[merge.py] Inserting {len(stops)} stops...", flush=True)
    await conn.executemany(
        "INSERT INTO stops (name, bearing, location) VALUES ($1, $2, $3);",
        [(s["name"], s["bearing"], s["location"]) for s in stops],
    )

    await conn.close()
    print(f"💾 Inserted {len(stops)} merged stops into database.", flush=True)


async def fetch_all_sources():
    """Fetch all sources concurrently."""
    print("[merge.py] fetch_all_sources: Starting", flush=True)
    async with httpx.AsyncClient() as client:
        print("[merge.py] fetch_all_sources: AsyncClient created", flush=True)
        tasks = {
            "uk": fetch_uk(client=client),
            "hsl": fetch_hsl(client=client),
            # add more here later, e.g. "waltti": fetch_waltti(client=client),
        }

        print("[merge.py] fetch_all_sources: Awaiting tasks...", flush=True)
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        print("[merge.py] fetch_all_sources: Tasks complete", flush=True)
        merged: List[Dict[str, Any]] = []

        for (source, result) in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                print(f"⚠️  Error fetching {source}: {result}", flush=True)
                continue
            print(f"[merge.py] Dumping source data for {source}...", flush=True)
            await dump_source_data(source, result)
            merged.extend(result)
            print(f"Fetched {len(result)} stops from {source}", flush=True)

        print(f"[merge.py] fetch_all_sources: Returning {len(merged)} total stops", flush=True)
        return merged


async def main():
    print("[merge.py] main() started", flush=True)
    print("🚀 Fetching and merging stop data...", flush=True)
    data = await fetch_all_sources()

    print(f"📦 Merging {len(data)} total stops...", flush=True)
    normalized = [normalize_for_db(s) for s in data if s.get("lat") and s.get("lon")]
    print(f"[merge.py] Normalized {len(normalized)} stops", flush=True)

    print(f"[merge.py] Saving to DB...", flush=True)
    await save_to_db(normalized)
    print("✅ Merge complete.", flush=True)


if __name__ == "__main__":
    print("[merge.py] __main__ block executing", flush=True)
    asyncio.run(main())
    print("[merge.py] __main__ block complete", flush=True)
