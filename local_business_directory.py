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


def generate_search_points_in_polygon(geojson_geometry, step_meters=100):

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
        "radius": radius,
        "keyword": keyword,
        "key": API_KEY
    }

    results = []

    while True:

        r = requests.get(url, params=params).json()

        print("Nearby status:", r.get("status"),
              "results:", len(r.get("results", [])))

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


def get_details(place_id):

    url = "https://maps.googleapis.com/maps/api/place/details/json"

    params = {
        "place_id": place_id,
        "fields": "name,rating,user_ratings_total,formatted_phone_number,formatted_address,geometry,business_status,types",
        "key": API_KEY
    }

    r = requests.get(url, params=params).json()

    return r.get("result", {})


# ==============================
# MAIN SCAN FUNCTION
# ==============================

def scan_businesses_in_sa2(
        sa2_geometry,
        step_meters=100,
        radius=100,
        keyword="Restaurant"
):

    polygon = shape(sa2_geometry)

    all_place_ids = set()
    leads = []

    search_points = generate_search_points_in_polygon(
        sa2_geometry,
        step_meters
    )

    print(f"Generated {len(search_points)} search points inside SA2")

    for lat, lng in search_points:

        print(f"Searching at {lat},{lng}")

        businesses = nearby_search(lat, lng, radius, keyword)

        for b in businesses:

            pid = b["place_id"]

            if pid in all_place_ids:
                continue

            all_place_ids.add(pid)

            details = get_details(pid)

            status = details.get("business_status")
            rating = details.get("rating")
            reviews = details.get("user_ratings_total")
            phone = details.get("formatted_phone_number")

            biz_geometry = details.get("geometry", {})
            biz_location = biz_geometry.get("location", {})

            biz_lat = biz_location.get("lat")
            biz_lng = biz_location.get("lng")

            if biz_lat is None or biz_lng is None:
                continue

            if not polygon.contains(Point(biz_lng, biz_lat)):
                continue

            if (
                status == "OPERATIONAL"
                and rating is not None and rating > 3
                and reviews is not None and reviews > 10
                and phone
            ):

                leads.append([
                    details.get("name", ""),
                    details.get("formatted_address", ""),
                    json.dumps(details.get("geometry", {})),
                    status,
                    ", ".join(details.get("types", [])),
                    phone,
                    rating,
                    reviews
                ])

            time.sleep(0.1)

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
            "Phone",
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
        step_meters=100,
        radius=100,
        keyword="Restaurant"
    )

    save_csv(leads)

    print(f"Done. Saved {len(leads)} leads to {OUTPUT_FILE}")
