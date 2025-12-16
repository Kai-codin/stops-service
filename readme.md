# Stops Service

A comprehensive service to aggregate, store, and serve public transport stops from various countries and sources. This project provides a unified API to access stop data and includes a simple web viewer.

## Features

- **Data Aggregation**: Fetches and merges public transport stops from multiple international sources (GTFS, APIs, etc.).
- **Unified API**: Provides a RESTful API to query stops by bounding box or paginated lists.
- **Web Viewer**: Includes a built-in HTML/JS frontend to visualize stops in a table format.
- **Dockerized**: Fully containerized with Docker and Docker Compose for easy deployment.
- **PostGIS Support**: Uses PostgreSQL (with PostGIS capabilities implied for location data) for efficient spatial queries.

## Supported Data Sources

The service currently aggregates data from the following sources:

- **United Kingdom** (BusTimes.org)
- **Finland** (Digitransit - National, HSL, Varely, Waltti)
- **France** (Transport.data.gouv.fr)
- **Germany** (GTFS.de)
- **Italy** (BusMaps)
- **Slovakia** (BusMaps)
- **Poland** (BusMaps)
- **Greece** (BusMaps)
- **Switzerland** (OEV-Info)
- **Netherlands** (OVapi)
- **Luxembourg** (Open Data Luxembourg)
- **Jersey** (BusTimes.org)
- **Sweden** (Resrobot.se)

## Tech Stack

- **Backend**: Python 3.9+, FastAPI
- **Database**: PostgreSQL 16
- **ORM/Data Access**: SQLAlchemy, asyncpg
- **HTTP Client**: httpx
- **Containerization**: Docker, Docker Compose

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Configuration

### API Keys

Some data sources require API keys. Create a file named `apikey.env` in the `backend/sources/` directory to store these keys.

**File:** `backend/sources/apikey.env`

```env
SWEDEN_KEY=your_sweden_api_key_here
```

## Installation & Running

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/Kai-codin/stops-service.git
    cd stops-service
    ```

2.  **Start the services:**

    ```bash
    docker compose up --build
    ```

    This will start the PostgreSQL database and the FastAPI backend.

3.  **Access the application:**

    - **API Root**: [http://localhost:8991](http://localhost:8991)
    - **Stops Viewer**: [http://localhost:8991/stops](http://localhost:8991/stops)
    - **Data Stats**: [http://localhost:8991/data](http://localhost:8991/data)

## API Usage

### Get Stops in Bounding Box

```http
GET /api/stops?xmin={min_lon}&xmax={max_lon}&ymin={min_lat}&ymax={max_lat}&limit={limit}
```

**Parameters:**
- `xmin`, `xmax`: Longitude bounds.
- `ymin`, `ymax`: Latitude bounds.
- `limit`: (Optional) Max number of stops to return (default: 10000).

### Get All Stops (Paginated)

```http
GET /api/allstops?limit={limit}&offset={offset}
```

**Parameters:**
- `limit`: (Optional) Number of stops per page (default: 5000).
- `offset`: (Optional) Pagination offset (default: 0).

## Data Management

### Merging Data

The project includes a utility script to fetch and merge data from sources. This is typically run within the backend container or as a separate task.

To run the merge script manually (e.g., for Luxembourg):

```bash
# Inside the backend container or with proper python env
python -m utils.merge luxembourg
```

To run for all sources:

```bash
python -m utils.merge
```

## Project Structure

```
stops-service/
├── compose.yaml          # Docker Compose configuration
├── readme.md             # Project documentation
└── backend/              # Backend application code
    ├── Dockerfile        # Backend Docker image definition
    ├── main.py           # FastAPI entry point
    ├── requirements.txt  # Python dependencies
    ├── sources/          # Source-specific fetcher modules
    ├── templates/        # HTML templates (Frontend)
    └── utils/            # Utility scripts (merge, dump)
```
