# Dockerfile for the Ingestion Service
# ─────────────────────────────────────────────────────────────────
# A Dockerfile is a recipe: "here's how to build a container for this service."
# We won't run this with Docker yet (that's Week 6 when we Docker Compose everything).
# But writing it now is good practice.
#
# How to read this:
#   FROM    → start with an existing base image (Python 3.12 here)
#   WORKDIR → all commands below run inside this folder in the container
#   COPY    → copy files from your machine into the container
#   RUN     → run a shell command while building the image
#   CMD     → the command that runs when the container starts

FROM python:3.12-slim

WORKDIR /app

# Copy requirements first — Docker caches this layer.
# If requirements.txt didn't change, it won't re-install packages on rebuild.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the service code
COPY . .

# Start the ingestion service
CMD ["python", "main.py"]

