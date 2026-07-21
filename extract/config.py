"""Central configuration for the MBTA extractor.

Nothing is hardcoded across the codebase — structural settings live here,
secrets come from environment variables. One place to change behavior.
"""
import os

# --- MBTA API ---
MBTA_BASE_URL = "https://api-v3.mbta.com"
MBTA_API_KEY = os.environ.get("MBTA_API_KEY")  # optional; raises rate limit 20 -> 1000/min

# --- Request behavior ---
REQUEST_TIMEOUT = 30     # seconds; never let a call hang forever
PAGE_LIMIT = 200         # records per page (JSON:API page[limit])
MAX_RETRIES = 5          # transient errors (429 / 5xx)
BACKOFF_FACTOR = 1.0     # exponential backoff base

# The routes we analyze. /predictions REQUIRES a filter, so we scope to a
# defined set rather than trying to pull the whole system.
TARGET_ROUTES = [
    "Red", "Orange", "Blue",
    "Green-B", "Green-C", "Green-D", "Green-E",
    "1", "66", "111",
]
