from fastapi import FastAPI
import os
import sqlalchemy

app = FastAPI(title="Stops API")

# Database URL (from env)
DATABASE_URL = os.getenv("DATABASE_URL")

@app.on_event("startup")
def startup():
    if DATABASE_URL:
        try:
            engine = sqlalchemy.create_engine(DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
            print("✅ Connected to Postgres.")
        except Exception as e:
            print(f"⚠️ Could not connect to database: {e}")
    else:
        print("⚠️ DATABASE_URL not set.")

@app.get("/")
def root():
    return {"message": "🚏 Stops API — Coming Soon!"}
