import os
import sqlalchemy
from sqlalchemy import text

# Database URL
DATABASE_URL = os.getenv("DATABASE_URL")

def create_indexes():
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL not set.")
        return

    try:
        print(f"Connecting to {DATABASE_URL}...")
        # Use AUTOCOMMIT to allow certain schema changes and to avoid transaction wrapping.
        engine = sqlalchemy.create_engine(DATABASE_URL).execution_options(isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            # 1. Check schema (use PRAGMA for sqlite and information_schema for others)
            print("Checking schema...")
            if engine.dialect.name == "sqlite":
                cols = [row[1] for row in conn.execute(text("PRAGMA table_info('stops');"))]
            else:
                cols = [
                    row[0]
                    for row in conn.execute(
                        text("""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_name='stops';
                        """)
                    )
                ]

            # 2. Create Name Index (used for sorting)
            print("Creating index on 'name'...")
            try:
                if engine.dialect.name == "sqlite":
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stops_name ON stops (name);"))
                else:
                    conn.execute(text("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stops_name ON stops (name);"))
                print("✅ Index 'idx_stops_name' created.")
            except Exception as e:
                print(f"⚠️ Failed to create name index: {e}")

            # 3. Create Location Indexes (used for bounding box)
            if "lon" in cols and "lat" in cols:
                print("Detected 'lon' and 'lat' columns. Creating composite index...")
                try:
                    if engine.dialect.name == "sqlite":
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stops_lon_lat ON stops (lon, lat);"))
                    else:
                        conn.execute(text("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stops_lon_lat ON stops (lon, lat);"))
                    print("✅ Index 'idx_stops_lon_lat' created.")
                except Exception as e:
                    print(f"⚠️ Failed to create lon/lat index: {e}")
            elif "location" in cols:
                print("Detected 'location' array column. Creating functional indexes (Postgres only)...")
                if engine.dialect.name != "sqlite":
                    try:
                        conn.execute(text("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stops_location_lon_lat ON stops ((location[1]), (location[2]));"))
                        print("✅ Index 'idx_stops_location_lon_lat' created.")
                    except Exception as e:
                        print(f"⚠️ Failed to create location array index: {e}")

            print("🎉 Indexing complete!")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_indexes()
