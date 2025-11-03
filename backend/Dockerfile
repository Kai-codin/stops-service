FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies for psycopg2 and building deps
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy dependencies first (for caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . /app

# Expose port (for FastAPI)
EXPOSE 8991

# Run the app with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8991"]
