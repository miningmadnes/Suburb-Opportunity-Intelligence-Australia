import os
import requests
import time
import csv
import json
import math
from shapely.geometry import shape, Point
from dotenv import load_dotenv

# ==============================
# CONFIG
# ==============================

load_dotenv(r"C:\Users\61481\Code\SOIA\variables.env")
API_KEY = os.getenv("GOOGLE_API")

OUTPUT_FILE = "kensington_business_leads.csv"

print("API key loaded:", API_KEY is not None)

# ==============================
# HELPER FUNCTIONS
# ==============================

def meters_to_lat_deg(meters):
    return meters / 111320


def meters_to_lng_deg(meters, lat):
    return meters / (111320 * math.cos(math.radians(lat)))


def generate_search_points_in_polygon(geojson_geometry, step_meters=150):

    polygon = shape(geojson_geometry)

    min_lng, min_lat, max_lng, max_lat = polygon.bounds

    lat_step = meters_to_lat_deg(step_meters)

    points = []
    lat = min_lat

    while lat <= max_lat:

        lng_step = meters_to_lng_deg(step_meters, lat)
        lng = min_lng

        while lng <= max_lng:

            p = Point(lng, lat)

            if polygon.contains(p):
                points.append((lat, lng))

            lng += lng_step

        lat += lat_step

    return points


# ==============================
# GOOGLE API FUNCTIONS
# ==============================

def nearby_search(lat, lng, radius, keyword):

    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

    params = {
        "location": f"{lat},{lng}",
        "radius": int(radius),
        "keyword": keyword,
        "key": API_KEY
    }

    results = []

    while True:

        r = requests.get(url, params=params).json()

        status = r.get("status")

        if status == "OVER_QUERY_LIMIT":
            print("Rate limit hit — waiting 10 seconds")
            time.sleep(10)
            continue  # retry the point

        if status not in ("OK", "ZERO_RESULTS"):
            print(f"API warning at ({lat},{lng}): {status} — {r.get('error_message', '')}")
            break

        if "results" in r:
            results.extend(r["results"])

        if "next_page_token" in r:

            time.sleep(3)

            params = {
                "pagetoken": r["next_page_token"],
                "key": API_KEY
            }

        else:
            break

    return results


# ==============================
# MAIN SCAN FUNCTION
# ==============================

def scan_businesses_in_sa2(
        sa2_geometry,
        step_meters=150,
        radius=100,
        keyword="Restaurant",
        max_points=200
):

    polygon = shape(sa2_geometry)

    all_place_ids = set()
    leads = []

    search_points = generate_search_points_in_polygon(
        sa2_geometry,
        step_meters
    )

    while len(search_points) > max_points:
        step_meters = int(step_meters * 1.5)
        radius = int(step_meters / 1.73)
        radius = max(100, min(radius, 50000))  # Google max is 50000
        search_points = generate_search_points_in_polygon(sa2_geometry, step_meters)

    print(f"Scanning {len(search_points)} search points...")

    for i, (lat, lng) in enumerate(search_points):

        if i % 50 == 0:
            print(f"  Progress: {i}/{len(search_points)} points, {len(leads)} leads so far")

        businesses = nearby_search(lat, lng, radius, keyword)

        for b in businesses:

            pid = b["place_id"]

            if pid in all_place_ids:
                continue

            # Pre-filter using data already in nearby_search response
            if b.get("business_status") != "OPERATIONAL":
                continue

            if b.get("rating", 0) <= 3:
                continue

            if b.get("user_ratings_total", 0) <= 10:
                continue

            # Check polygon containment using nearby_search geometry
            biz_location = b.get("geometry", {}).get("location", {})
            biz_lat = biz_location.get("lat")
            biz_lng = biz_location.get("lng")

            if biz_lat is None or biz_lng is None:
                continue

            if not polygon.contains(Point(biz_lng, biz_lat)):
                continue

            all_place_ids.add(pid)

            leads.append([
                b.get("name", ""),
                b.get("vicinity", ""),
                json.dumps(b.get("geometry", {})),
                b.get("business_status", ""),
                ", ".join(b.get("types", [])),
                b.get("rating"),
                b.get("user_ratings_total")
            ])

    return leads


# ==============================
# SAVE CSV
# ==============================

def save_csv(leads):

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)

        writer.writerow([
            "Name",
            "Address",
            "Geometry",
            "Business Status",
            "Types",
            "Rating",
            "Reviews"
        ])

        writer.writerows(leads)


# ==============================
# RUN AS SCRIPT
# ==============================

if __name__ == "__main__":

    print("Running directory scanner standalone")

    with open("sa2_geometry.json") as f:
        sa2_geometry = json.load(f)

    leads = scan_businesses_in_sa2(
        sa2_geometry,
        step_meters=150,
        radius=100,
        keyword="Restaurant"
    )

    save_csv(leads)

    print(f"Done. Saved {len(leads)} leads to {OUTPUT_FILE}")
