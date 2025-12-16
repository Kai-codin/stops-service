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
        # Use AUTOCOMMIT to allow CREATE INDEX CONCURRENTLY if needed, 
        # and to avoid transaction wrapping for schema changes.
        engine = sqlalchemy.create_engine(DATABASE_URL).execution_options(isolation_level="AUTOCOMMIT")
        
        with engine.connect() as conn:
            # 1. Check schema
            print("Checking schema...")
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
                # Using CONCURRENTLY to avoid locking the table for reads/writes
                conn.execute(text("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stops_name ON stops (name);"))
                print("✅ Index 'idx_stops_name' created.")
            except Exception as e:
                print(f"⚠️ Failed to create name index (trying without CONCURRENTLY): {e}")
                try:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stops_name ON stops (name);"))
                    print("✅ Index 'idx_stops_name' created.")
                except Exception as e2:
                    print(f"❌ Failed to create name index: {e2}")

            # 3. Create Location Indexes (used for bounding box)
            if "lon" in cols and "lat" in cols:
                print("Detected 'lon' and 'lat' columns. Creating composite index...")
                try:
                    conn.execute(text("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stops_lon_lat ON stops (lon, lat);"))
                    print("✅ Index 'idx_stops_lon_lat' created.")
                except Exception as e:
                    print(f"⚠️ Failed to create lon/lat index (trying without CONCURRENTLY): {e}")
                    try:
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stops_lon_lat ON stops (lon, lat);"))
                        print("✅ Index 'idx_stops_lon_lat' created.")
                    except Exception as e2:
                        print(f"❌ Failed to create lon/lat index: {e2}")
                    
            elif "location" in cols:
                print("Detected 'location' array column. Creating functional indexes...")
                try:
                    # Functional index for array elements
                    conn.execute(text("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stops_location_lon_lat ON stops ((location[1]), (location[2]));"))
                    print("✅ Index 'idx_stops_location_lon_lat' created.")
                except Exception as e:
                    print(f"⚠️ Failed to create location array index (trying without CONCURRENTLY): {e}")
                    try:
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stops_location_lon_lat ON stops ((location[1]), (location[2]));"))
                        print("✅ Index 'idx_stops_location_lon_lat' created.")
                    except Exception as e2:
                        print(f"❌ Failed to create location array index: {e2}")
            
            print("🎉 Indexing complete!")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_indexes()
