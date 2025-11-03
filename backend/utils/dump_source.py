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

import sys

SINGLE_SOURCE = None
if len(sys.argv) > 1:
    SINGLE_SOURCE = sys.argv[1].lower()
    print(f"[merge.py] Running in single-source mode: {SINGLE_SOURCE}", flush=True)

else:
    print("[merge.py] Running in all-sources mode", flush=True)

async def main():
    # take in a source name and remove it from the db
    async def remove_source_data(source: str):
        print(f"[merge.py] remove_source_data: Removing data for source: {source}", flush=True)
        DATABASE_URL = os.getenv("DATABASE_URL")
        if not DATABASE_URL:
            print("[merge.py] DATABASE_URL not set. Exiting.", flush=True)
            return

        conn = await asyncpg.connect(DATABASE_URL)
        try:
            result = await conn.execute("DELETE FROM stops WHERE source = $1", source)
            print(f"[merge.py] remove_source_data: Removed records for source {source}: {result}", flush=True)
        finally:
            await conn.close()

    if SINGLE_SOURCE:
        await remove_source_data(SINGLE_SOURCE)
    else:
        print("[merge.py] No source specified for removal. Exiting.", flush=True)

if __name__ == "__main__":
    print("[merge.py] __main__ block executing", flush=True)
    asyncio.run(main())
    print("[merge.py] __main__ block complete", flush=True)