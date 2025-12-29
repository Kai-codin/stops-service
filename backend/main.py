print("[main.py] Module loading...", flush=True)

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import sqlalchemy
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware

print("[main.py] Imports done", flush=True)

app = FastAPI(title="Stops API")
print("[main.py] FastAPI app created", flush=True)

# Database URL
DATABASE_URL = os.getenv("DATABASE_URL")
engine = None


@app.on_event("startup")
def startup():
    print("[main.py] startup() called", flush=True)
    global engine
    if DATABASE_URL:
        try:
            print(f"[main.py] Creating engine for {DATABASE_URL}", flush=True)
            engine = sqlalchemy.create_engine(DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Connected to Postgres.", flush=True)
        except Exception as e:
            print(f"⚠️ Could not connect to database: {e}", flush=True)
    else:
        print("⚠️ DATABASE_URL not set.", flush=True)


@app.get("/")
def root():
    print("[main.py] GET / called", flush=True)
    return {"message": "🚏 Stops API — Running!"}


# --- Frontend setup ---
print("[main.py] Frontend setup starting...", flush=True)
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
print("[main.py] Frontend setup complete", flush=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",  # local dev frontend
        "https://stops.mybustimes.cc",  # production frontend
        "https://mybustimes.cc",  # optional main domain
        "https://www.mybustimes.cc",  # optional www domain
        "https://test.mybustimes.cc",  # optional www domain
        "https://dev.mybustimes.cc",  # optional www domain
        "https://local-dev.mybustimes.cc",  # local dev frontend (React)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1️⃣ Bounding box API endpoint ---
@app.get("/api/stops")
def api_stops(
    xmin: float = Query(...),
    xmax: float = Query(...),
    ymin: float = Query(...),
    ymax: float = Query(...),
    limit: int = Query(10000),
    offset: int = Query(0),
):
    """Return stops within a bounding box"""
    print(f"[main.py] GET /api/stops bbox=({xmin},{xmax},{ymin},{ymax})", flush=True)
    if not engine:
        return JSONResponse({"error": "Database not configured"}, status_code=500)

    try:
        with engine.connect() as conn:
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

            if "lon" in cols and "lat" in cols:
                query = text("""
                    SELECT name, bearing, lon, lat
                    FROM stops
                    WHERE lon BETWEEN :xmin AND :xmax
                    AND lat BETWEEN :ymin AND :ymax
                    ORDER BY name
                    LIMIT :limit OFFSET :offset
                """)
            elif "location" in cols:
                query = text("""
                    SELECT name, bearing, location[1] AS lon, location[2] AS lat
                    FROM stops
                    WHERE location[1] BETWEEN :xmin AND :xmax
                    AND location[2] BETWEEN :ymin AND :ymax
                    ORDER BY name
                    LIMIT :limit OFFSET :offset
                """)
            else:
                return JSONResponse({"error": "No location columns found"}, status_code=500)

            result = conn.execute(
                query,
                {"xmin": xmin, "xmax": xmax, "ymin": ymin, "ymax": ymax, "limit": limit, "offset": offset},
            )
            stops = [dict(row._mapping) for row in result]
            print(f"[main.py] Returning {len(stops)} stops", flush=True)
            return stops

    except Exception as e:
        print(f"⚠️ Query failed: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)

# --- 2️⃣ Paginated list API endpoint ---
@app.get("/api/allstops")
def api_all_stops(
    limit: int = Query(5000),
    offset: int = Query(0),
):
    """Return all stops paginated"""
    print(f"[main.py] GET /api/allstops limit={limit} offset={offset}", flush=True)
    if not engine:
        return JSONResponse({"error": "Database not configured"}, status_code=500)

    try:
        with engine.connect() as conn:
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

            if "lon" in cols and "lat" in cols:
                query = text("""
                    SELECT name, bearing, lon, lat
                    FROM stops
                    ORDER BY name
                    LIMIT :limit OFFSET :offset
                """)
            elif "location" in cols:
                query = text("""
                    SELECT name, bearing, location[1] AS lon, location[2] AS lat
                    FROM stops
                    ORDER BY name
                    LIMIT :limit OFFSET :offset
                """)
            else:
                return JSONResponse({"error": "No location columns found"}, status_code=500)

            result = conn.execute(query, {"limit": limit, "offset": offset})
            stops = [dict(row._mapping) for row in result]
            print(f"[main.py] Returning {len(stops)} stops", flush=True)
            return stops

    except Exception as e:
        print(f"⚠️ Query failed: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)


# --- 3️⃣ Viewer page ---
@app.get("/stops", response_class=HTMLResponse)
def stops_page(request: Request):
    print("[main.py] GET /stops called", flush=True)
    return templates.TemplateResponse("stops.html", {"request": request})

@app.get("/data", response_class=JSONResponse)
def data_page():
    if not engine:
        return JSONResponse({"error": "Database not configured"}, status_code=500)

    try:
        with engine.connect() as conn:
            # Get total stops per source
            source_counts = conn.execute(text("""
                SELECT source, COUNT(*) as count 
                FROM stops 
                GROUP BY source
            """))
            source_counts = {row.source: row.count for row in source_counts}

            # Get most recent update timestamp
            last_update = conn.execute(text("""
                SELECT MAX(created_at) as last_update 
                FROM stops
            """)).scalar()

            total_stops = sum(source_counts.values())

            # Create data sources list with actual counts
            data_sources = []
            source_urls = {
                "UK": "https://bustimes.org/api/stops/",
                "Finland": "https://api.digitransit.fi/routing/v2/finland/gtfs/v1",
                "HSL": "https://api.digitransit.fi/routing/v2/hsl/gtfs/v1",
                "VARELY": "https://api.digitransit.fi/routing/v2/varely/gtfs/v1",
                "Waltti": "https://api.digitransit.fi/routing/v2/waltti/gtfs/v1",
                "France": "https://transport.data.gouv.fr/api/gtfs-stops",
                "Italy": "https://busmaps.com/en/italy/feedlist",
                "Slovakia": "https://busmaps.com/en/slovakia/feedlist",
                "Poland": "https://busmaps.com/en/poland/feedlist",
                "Greece": "https://busmaps.com/en/greece/feedlist",
                "Switzerland": "https://data.oev-info.ch/explore/dataset/stop-points-today/",
                "Jersey": "https://github.com/jclgoodwin/bustimes.org/blob/main/busstops/jersey-bus-stops.json",
                "Germany": "https://download.gtfs.de/germany/free/latest.zip",
                "Netherlands": "https://gtfs.ovapi.nl/nl/",
                "Luxembourg": "https://data.public.lu/en/datasets/horaires-et-arrets-des-transport-publics-gtfs/",
                "Sweden": "https://api.resrobot.se/v2.1/gtfs/sweden.zip",
                "Guernsey": "https://ticketless-app.api.urbanthings.cloud/api/2/transit/stops/",
            }

            for source, url in source_urls.items():
                if source.lower() == "uk":
                    source = "ukbuses"
                else:
                    source = source.lower()
                data_sources.append({
                    "source": source,
                    "stops_count": source_counts.get(source, 0),
                    "source_url": url
                })


    except Exception as e:
        print(f"⚠️ Query failed: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)

    print("[main.py] GET /data called", flush=True)
    message = {
        "data_sources": data_sources,
        "health": [
            {"Total_stops_in_db": total_stops},
            {"Last_update": last_update},
        ]
    }
    return {"message": message}
