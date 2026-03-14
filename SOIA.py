import os
import pandas as pd
import snowflake.connector
import local_business_directory
import json
import math
from shapely.geometry import shape
from shapely.geometry import Point
from shapely.ops import transform
from pyproj import Transformer
from local_business_directory import scan_businesses_in_sa2
from dotenv import load_dotenv

sa2=input("Gimme Sa2 code pls: ")
state=input("What State is this in? ")

# Connects into the snowflake server using the encoded login information in variables.env
load_dotenv(r"C:\Users\61481\Code\SOIA\variables.env")

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

# Defines the run_query function for creating tables to keep code cleaner when running queries
def run_query(query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetch_pandas_all()
    finally:
        cur.close()

# Defines the run_query_value function for determining single values in tables
def run_query_value(query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetchone()[0]
    finally:
        cur.close()

# Main

# Get Number of People
value = run_query_value(f"""
               select obs_value
               from abs_socioeconomic_indexes_for_areas_seifa_2021_data__free.seifa.seifa_sa2
where seifa_sa2 = '{sa2}' and unit_of_measure = 'Persons'
               Order by obs_value desc
               limit 1               
               """)
print(value)

# Get Number of Businesses

geometry_string= run_query_value(f"""
                                 select geometry
from geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020
where sa2_code_2021 = '{sa2}'
limit 1
""")

print(geometry_string)

sa2_geometry=json.loads(geometry_string)

leads = scan_businesses_in_sa2(
    sa2_geometry,
    step_meters=1000,
    radius=100,
    keyword="Cafe"
)

print("Number of leads:", len(leads))

# Does S/D Now Calculation

if value is not None:
    SDNow = len(leads) / float(value)
    print("Supply/Demand Now: ", SDNow)
else:
    print("No value returned from population numbers, check sa2")

# Get the geometry bounds to be the same as the projection in the database

sa2_geom = shape(json.loads(geometry_string))

transformer = Transformer.from_crs(
    "EPSG:4326",   # SA2 lon/lat
    "EPSG:3857",   # road CRS
    always_xy=True
)

sa2_projected = transform(transformer.transform, sa2_geom)

roads_total=run_query(f"""
                      select HIERARCHY, shape_length, geometry
                      from transport__lines_and_fixtures__australia__free.transport_aus_free.ga_national_roads_aus_gda2020
                      where state = '{state}' and trafficability = '2WD' and status = 'OPERATIONAL'
""")

total_road_length=0

for _, row in roads_total.iterrows():
    geom_value = row["GEOMETRY"]

    if geom_value is None:
        continue

    if isinstance(geom_value, str):
        road_geom = shape(json.loads(geom_value))
    else:
        road_geom = shape(geom_value)

    if road_geom.intersects(sa2_projected):
        clipped = road_geom.intersection(sa2_projected)
        total_road_length += clipped.length

area=float(run_query_value(f"""
                    select area_albers_sqkm
                    from geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020
                    where sa2_code_2021 = '{sa2}'
                     """))

nat_roads_density=total_road_length/area

print("Roads Density: ", nat_roads_density, "km/m^2")

# Finding average distance to hospital

minx, miny, maxx, maxy = sa2_geom.bounds

hospitals_df=run_query(f"""
                       select latitude, longitude
from healthcare__locations__statistics__australia__free.healthcare_aus_free.aihw_hospital_mapping
where open_closed ='Open' and sector = 'Public'  and type = 'Hospital'
                       """)

hospitals_df = hospitals_df[
    (hospitals_df["LONGITUDE"] >= minx - 0.2) &
    (hospitals_df["LONGITUDE"] <= maxx + 0.2) &
    (hospitals_df["LATITUDE"] >= miny - 0.2) &
    (hospitals_df["LATITUDE"] <= maxy + 0.2)
]

step_metres = math.sqrt((area * 1000000) / 100)

def meters_to_lat_deg(meters):
    return meters / 111320

def meters_to_lng_deg(meters, lat):
    return meters / (111320 * math.cos(math.radians(lat)))

def generate_sample_points_in_polygon(geojson_geometry, step_meters=step_metres):
    polygon = shape(json.loads(geojson_geometry)) if isinstance(geojson_geometry, str) else shape(geojson_geometry)

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
                points.append(p)
            lng += lng_step

        lat += lat_step

    return points

def average_distance_to_nearest_hospital(sa2_geometry_string, hospitals_df, step_meters=step_metres):
    sa2_geom = shape(json.loads(sa2_geometry_string))

    # project everything into metres
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    sa2_projected = transform(transformer.transform, sa2_geom)

    sample_points = generate_sample_points_in_polygon(sa2_geometry_string, step_meters=step_meters)
    projected_sample_points = [transform(transformer.transform, p) for p in sample_points]

    hospital_points = []
    for _, row in hospitals_df.iterrows():
        lon = row["LONGITUDE"]
        lat = row["LATITUDE"]

        if lon is None or lat is None:
            continue

        hx, hy = transformer.transform(lon, lat)
        hospital_points.append(Point(hx, hy))

    if not hospital_points:
        return None

    nearest_distances = []

    for sample_point in projected_sample_points:
        min_distance = min(sample_point.distance(h) for h in hospital_points)
        nearest_distances.append(min_distance)

    avg_distance = sum(nearest_distances) / len(nearest_distances)

    return avg_distance

avg_dist_m = average_distance_to_nearest_hospital(
    geometry_string,
    hospitals_df,
    step_meters=step_metres
)

print("Average nearest hospital distance (km):", avg_dist_m / 1000)

# Finding petrol stations in SA2
petrol_df=run_query(f"""
                       select geometry
from transport__lines_and_fixtures__australia__free.transport_aus_free.ga_petrol_station_locations_gda2020
where station_state = '{state}'
""")

petrol_count=0

for _, row in petrol_df.iterrows():
    geom=row["GEOMETRY"]
    if geom is None:
        continue

    if isinstance(geom, str):
        geom = json.loads(geom)

    lon = geom["coordinates"][0]
    lat = geom["coordinates"][1]

    petrol_point = Point(lon,lat)

    if sa2_geom.contains(petrol_point):
        petrol_count += 1

petrol_density=petrol_count/area
print("Petrol Density: ", petrol_density)


stations_df=run_query(f"""
                       select geometry
from transport__lines_and_fixtures__australia__free.transport_aus_free.ga_petrol_station_locations_gda2020
where state = '{state}'
""")

stations_count=0

for _, row in stations_df.iterrows():
    geom=row["GEOMETRY"]
    if geom is None:
        continue

    if isinstance(geom, str):
        geom = json.loads(geom)

    lon = geom["coordinates"][0]
    lat = geom["coordinates"][1]

    stations_point = Point(lon,lat)

    if sa2_geom.contains(stations_point):
        stations_count += 1

stations_density=stations_count/area
print("Stations Density: ", stations_density)
