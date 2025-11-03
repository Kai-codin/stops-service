# merge.py
import os
import json
import asyncio
import datetime
from pathlib import Path
from typing import List, Dict, Any
import logging

import asyncpg
import httpx

# Import your source fetchers
import sys
from pathlib import Path

# Add project root to import path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from sources.uk import fetch_uk
from sources.hsl import fetch_hsl

# --- CONFIG ---
DATA_DIR = Path("data")
DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/stops")


async def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


async def save_json(path: Path, data: Any):
    async with asyncio.to_thread(open, path, "w") as f:
        json.dump(data, f, indent=2)


async def dump_source_data(source: str, data: List[Dict[str, Any]]):
    """Save raw source data to data/{source}/{source}-{date}.json"""
    date = datetime.date.today().strftime("%Y%m%d")
    dir_path = DATA_DIR / source
    await ensure_dir(dir_path)
    file_path = dir_path / f"{source}-{date}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved {len(data)} stops for {source} → {file_path}")


def normalize_for_db(stop: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reduce to minimal required schema:
    {"bearing": "", "name": "stop name", "location": [lon, lat]}
    """
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
    conn = await asyncpg.connect(DB_DSN)
    print(f"Connecting to DB: {DB_DSN}")

    # Create table if it doesn't exist
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
    print("Ensured stops table exists")

    # Clear old data (optional)
    await conn.execute("TRUNCATE TABLE stops;")
    print("Truncated stops table")

    # Bulk insert
    await conn.executemany(
        "INSERT INTO stops (name, bearing, location) VALUES ($1, $2, $3);",
        [(s["name"], s["bearing"], s["location"]) for s in stops],
    )

    await conn.close()
    print(f"💾 Inserted {len(stops)} merged stops into database.")


async def fetch_all_sources():
    """Fetch all sources concurrently."""
    async with httpx.AsyncClient() as client:
        tasks = {
            "uk": fetch_uk(client=client),
            "hsl": fetch_hsl(client=client),
            # add more here later, e.g. "waltti": fetch_waltti(client=client),
        }

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        merged: List[Dict[str, Any]] = []

        for (source, result) in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                print(f"⚠️  Error fetching {source}: {result}")
                continue
            await dump_source_data(source, result)
            merged.extend(result)
            print(f"Fetched {len(result)} stops from {source}")

        return merged


async def main():
    print("🚀 Fetching and merging stop data...")
    data = await fetch_all_sources()

    print(f"📦 Merging {len(data)} total stops...")
    normalized = [normalize_for_db(s) for s in data if s.get("lat") and s.get("lon")]

    await save_to_db(normalized)
    print("✅ Merge complete.")


if __name__ == "__main__":
    asyncio.run(main())
