print("[main.py] Module loading...", flush=True)

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import sqlalchemy
from sqlalchemy import text

print("[main.py] Imports done", flush=True)

app = FastAPI(title="Stops API")
print("[main.py] FastAPI app created", flush=True)

# Database URL (from env)
DATABASE_URL = os.getenv("DATABASE_URL")

# Setup DB engine
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
    return {"message": "🚏 Stops API — Coming Soon!"}


# --- Frontend setup ---
print("[main.py] Frontend setup starting...", flush=True)
# Create static and template directories if missing
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)
print("[main.py] Created templates and static dirs", flush=True)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
print("[main.py] Frontend setup complete", flush=True)


@app.get("/stops", response_class=HTMLResponse)
def stops_page(request: Request):
    """Simple page to show stop data"""
    print("[main.py] GET /stops called", flush=True)
    stops = []
    if engine:
        try:
            print("[main.py] Querying stops from database...", flush=True)
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT name, bearing, lon, lat FROM stops ORDER BY name LIMIT 100")
                )
                stops = [dict(row._mapping) for row in result]
            print(f"[main.py] Retrieved {len(stops)} stops from database", flush=True)
        except Exception as e:
            print(f"⚠️ Failed to query stops: {e}", flush=True)

    return templates.TemplateResponse(
        "stops.html",
        {"request": request, "stops": stops},
    )
