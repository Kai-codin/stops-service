from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import sqlalchemy
from sqlalchemy import text

app = FastAPI(title="Stops API")

# Database URL (from env)
DATABASE_URL = os.getenv("DATABASE_URL")

# Setup DB engine
engine = None


@app.on_event("startup")
def startup():
    global engine
    if DATABASE_URL:
        try:
            engine = sqlalchemy.create_engine(DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Connected to Postgres.")
        except Exception as e:
            print(f"⚠️ Could not connect to database: {e}")
    else:
        print("⚠️ DATABASE_URL not set.")


@app.get("/")
def root():
    return {"message": "🚏 Stops API — Coming Soon!"}


# --- Frontend setup ---
# Create static and template directories if missing
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/stops", response_class=HTMLResponse)
def stops_page(request: Request):
    """Simple page to show stop data"""
    stops = []
    if engine:
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT name, bearing, lon, lat FROM stops ORDER BY name LIMIT 100")
                )
                stops = [dict(row._mapping) for row in result]
        except Exception as e:
            print(f"⚠️ Failed to query stops: {e}")

    return templates.TemplateResponse(
        "stops.html",
        {"request": request, "stops": stops},
    )
