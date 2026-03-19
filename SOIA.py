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
from scipy.spatial import KDTree

sa2=input("Gimme Sa2 code pls: ")
state="NSW"
niche=input("What niche are you interested in? ")

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

sa2_row = run_query(f"""
                    SELECT sa2_name_2021, area_albers_sqkm, geometry
    FROM geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020
    WHERE sa2_code_2021 = '{sa2}'
                    """)

sa2_name = sa2_row["SA2_NAME_2021"].iloc[0]
area = float(sa2_row["AREA_ALBERS_SQKM"].iloc[0])
geometry_string = sa2_row["GEOMETRY"].iloc[0]

print("Sa2 Name: ", sa2_name)

value = run_query_value(f"""
               select obs_value
               from abs_socioeconomic_indexes_for_areas_seifa_2021_data__free.seifa.seifa_sa2
where seifa_sa2 = '{sa2}' and unit_of_measure = 'Persons'
               Order by obs_value desc
               limit 1               
               """)
print("Population of the Sa2: ", value)

# Get Number of Businesses

sa2_geometry=json.loads(geometry_string)

radius_calculation=math.sqrt(area)*100
radius_calculation=max(100, min(radius_calculation, 5000))
step_metres_calculation=radius_calculation*1.73

population_density = float(value) / area  # people per km²

if population_density < 1:
    max_points = 20
elif population_density < 10:
    max_points = 80
else:
    max_points = 200

leads = scan_businesses_in_sa2(
    sa2_geometry,
    step_meters=step_metres_calculation,
    radius=radius_calculation,
    keyword=niche,
    max_points=max_points
)

print("Number of leads:", len(leads))

# Does S/D Now Calculation

if value is not None:
    SDNow = len(leads) / float(value)
    print("Supply/Demand Now: ", SDNow)
else:
    print("No value returned from population numbers, check sa2")

# Get the geometry bounds to be the same as the projection in the database

# Outlines SA2 Bounds
sa2_geom = shape(json.loads(geometry_string))

transformer = Transformer.from_crs(
    "EPSG:4326",   # SA2 lon/lat
    "EPSG:3857",   # road CRS
    always_xy=True
)

sa2_projected = transform(transformer.transform, sa2_geom)

deg_minx, deg_miny, deg_maxx, deg_maxy = sa2_geom.bounds

proj_minx, proj_miny, proj_maxx, proj_maxy =sa2_projected.bounds

buffer = 5000 
proj_minx -= buffer
proj_miny -= buffer
proj_maxx += buffer
proj_maxy += buffer

roads_total=run_query(f"""
                      SELECT shape_length, ST_ASGEOJSON(geometry) AS geometry
                      FROM transport__lines_and_fixtures__australia__free.transport_aus_free.ga_national_roads_aus_gda2020
                      WHERE state = '{state}' AND trafficability = '2WD' AND status = 'OPERATIONAL'
                      AND ST_XMIN(geometry) BETWEEN {proj_minx} AND {proj_maxx}
                      AND ST_YMIN(geometry) BETWEEN {proj_miny} AND {proj_maxy}
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

nat_roads_density=(total_road_length/1000)/area

print("Roads Density: ", nat_roads_density, "km/km^2")

# Finding average distance to hospital

buffer_deg=0.5
deg_minx -= buffer_deg
deg_miny -= buffer_deg
deg_maxx += buffer_deg
deg_maxy += buffer_deg

hospitals_df=run_query(f"""
                       select latitude, longitude
from healthcare__locations__statistics__australia__free.healthcare_aus_free.aihw_hospital_mapping
where open_closed ='Open' and sector = 'Public'  and type = 'Hospital' and longitude between {deg_minx} and {deg_maxx} and latitude between {deg_miny} and {deg_maxy}
                       """)

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

    tree = KDTree([(h.x, h.y) for h in hospital_points])
    distances, _ = tree.query([(p.x, p.y) for p in projected_sample_points])

    return distances.mean()

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
from transport__lines_and_fixtures__australia__free.transport_aus_free.ga_railway_stations_aus_gda2020
                      WHERE source_jurisdiction = '{state}'
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

# Permit

def rolling_growth_score(series, value, window=12):
    series = pd.to_numeric(series, errors="coerce")
    per_capita_series = series / value
    rolling_series = per_capita_series.rolling(window=window, min_periods=window).mean()
    growth = rolling_series.pct_change().iloc[-1]
    acceleration = rolling_series.pct_change().diff().iloc[-1]
    return growth, acceleration

building_types = {
    "Residential": "100: Total Residential",
    "Non-Residential": "800: Dwellings excluding new residential",
    "Commercial": "200: Commercial Buildings - Total",
    "Industrial": "300: Industrial Buildings - Total",
    "Education": "410: Education buildings",
    "Health": "440: Health buildings",
    "Entertainment": "450: Entertainment and recreation buildings",
}

type_list = ", ".join(f"'{v}'" for v in building_types.values())

permits_df = run_query(f"""
    SELECT building_type, obs_value, time_period
    FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
    WHERE region_type = 'SA2: Statistical Area Level 2'
      AND region = '{sa2}: {sa2_name}'
      AND measure = '2: Value of building jobs'
      AND building_type IN ({type_list})
      AND work_type = 'TOT: Total Work'
      AND sector = '9: Total Sectors'
    ORDER BY time_period ASC
""")

for label, btype in building_types.items():
    series = permits_df[permits_df["BUILDING_TYPE"] == btype]["OBS_VALUE"]
    growth, acceleration = rolling_growth_score(series, value)
    print(f"{label} growth: {growth}")
    print(f"{label} acceleration: {acceleration}")

# Crime
postcodes_df = run_query(f"""
    SELECT poa_code_2021, ST_ASGEOJSON(p.geometry) AS geometry
    FROM geography__boundaries__insights__australia.geography_aus_free.abs_poa_2021_aust_gda2020 p
    JOIN geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020 s
        ON ST_INTERSECTS(
            TO_GEOMETRY(p.geometry),
            TO_GEOMETRY(s.geometry)
        )
    WHERE s.sa2_code_2021 = '{sa2}'
""")

matched = []

for _, row in postcodes_df.iterrows():
    geom_value = row["GEOMETRY"] if "GEOMETRY" in postcodes_df.columns else row["geometry"]

    if geom_value is None:
        continue

    if isinstance(geom_value, str):
        postcode_geom = shape(json.loads(geom_value))
    else:
        postcode_geom = shape(geom_value)

    if not postcode_geom.intersects(sa2_geom):
        continue

    intersection = postcode_geom.intersection(sa2_geom)
    weight = intersection.area / postcode_geom.area

    postcode_value = row["POA_CODE_2021"] if "POA_CODE_2021" in postcodes_df.columns else row["poa_code_2021"]

    if weight < 0.0001:
        continue

    matched.append({
        "postcode": postcode_value,
        "weight": weight
    })

weights_df = pd.DataFrame(matched)

postcode_list = ", ".join(f"'{p}'" for p in weights_df["postcode"].tolist())

postcode_crime_data=run_query(f"""
                                    WITH crime_unpivot AS (
        SELECT postcode, subcategory, month, value
        FROM (
                                        select * from crime__statistics__australia__free.crime_statistics_aus_free.nsw_boscar_postcode_crime_statistics
        WHERE postcode IN ({postcode_list})
        )
        UNPIVOT(value FOR month IN (dec_2022))
    ),
    crime_costs AS (
        SELECT postcode, month,
            CASE subcategory
                WHEN 'Murder' THEN value * 4500000
                WHEN 'Attempted murder' THEN value * 650000
                WHEN 'Manslaughter' THEN value * 3000000
                WHEN 'Domestic violence related assault' THEN value * 35000
                WHEN 'Non-domestic violence related assault' THEN value * 30000
                WHEN 'Assault Police' THEN value * 40000
                WHEN 'Sexual assault' THEN value * 260000
                WHEN 'Sexual touching, sexual act and other sexual offences' THEN value * 120000
                WHEN 'Abduction and kidnapping' THEN value * 450000
                WHEN 'Robbery without a weapon' THEN value * 12000
                WHEN 'Robbery with a firearm' THEN value * 45000
                WHEN 'Robbery with a weapon not a firearm' THEN value * 25000
                WHEN 'Blackmail and extortion' THEN value * 15000
                WHEN 'Intimidation, stalking and harassment' THEN value * 8000
                WHEN 'Other offences against the person' THEN value * 10000
                WHEN 'Break and enter dwelling' THEN value * 6500
                WHEN 'Break and enter non-dwelling' THEN value * 4000
                WHEN 'Receiving or handling stolen goods' THEN value * 3000
                WHEN 'Motor vehicle theft' THEN value * 8500
                WHEN 'Steal from motor vehicle' THEN value * 1200
                WHEN 'Steal from retail store' THEN value * 900
                WHEN 'Steal from dwelling' THEN value * 2000
                WHEN 'Steal from person' THEN value * 1500
                WHEN 'Stock theft' THEN value * 6000
                WHEN 'Fraud' THEN value * 3500
                WHEN 'Other theft' THEN value * 1200
                WHEN 'Arson' THEN value * 45000
                WHEN 'Malicious damage to property' THEN value * 3500
                WHEN 'Possession and/or use of cocaine' THEN value * 2500
                WHEN 'Possession and/or use of narcotics' THEN value * 2500
                WHEN 'Possession and/or use of cannabis' THEN value * 1000
                WHEN 'Possession and/or use of amphetamines' THEN value * 2000
                WHEN 'Possession and/or use of ecstasy' THEN value * 2000
                WHEN 'Possession and/or use of other drugs' THEN value * 1500
                WHEN 'Dealing, trafficking in cocaine' THEN value * 25000
                WHEN 'Dealing, trafficking in narcotics' THEN value * 25000
                WHEN 'Dealing, trafficking in cannabis' THEN value * 12000
                WHEN 'Dealing, trafficking in amphetamines' THEN value * 20000
                WHEN 'Dealing, trafficking in ecstasy' THEN value * 18000
                WHEN 'Dealing, trafficking in other drugs' THEN value * 15000
                WHEN 'Cultivating cannabis' THEN value * 10000
                WHEN 'Manufacture drug' THEN value * 35000
                WHEN 'Importing drugs' THEN value * 80000
                WHEN 'Other drug offences' THEN value * 3000
                WHEN 'Prohibited and regulated weapons offences' THEN value * 8000
                WHEN 'Trespass' THEN value * 800
                WHEN 'Offensive conduct' THEN value * 500
                WHEN 'Offensive language' THEN value * 200
                WHEN 'Criminal intent' THEN value * 1500
                WHEN 'Betting and gaming offences' THEN value * 1200
                WHEN 'Liquor offences' THEN value * 900
                WHEN 'Pornography offences' THEN value * 4000
                WHEN 'Prostitution offences' THEN value * 1500
                WHEN 'Escape custody' THEN value * 20000
                WHEN 'Breach Apprehended Violence Order' THEN value * 6000
                WHEN 'Breach bail conditions' THEN value * 4000
                WHEN 'Fail to appear' THEN value * 2000
                WHEN 'Resist or hinder officer' THEN value * 5000
                WHEN 'Other offences against justice procedures' THEN value * 2500
                WHEN 'Transport regulatory offences' THEN value * 500
                WHEN 'Other offences' THEN value * 1000
                ELSE 0
            END AS crime_cost
        FROM crime_unpivot
    )
    SELECT postcode, SUM(crime_cost) AS total_crime_cost
    FROM crime_costs
    GROUP BY postcode
                                  """)

postcode_crime_data["TOTAL_CRIME_COST"] = postcode_crime_data["TOTAL_CRIME_COST"].astype(float)    
postcode_crime_data["POSTCODE"] = postcode_crime_data["POSTCODE"].astype(str)
weights_df["postcode"] = weights_df["postcode"].astype(str)

crime_merged = postcode_crime_data.merge(weights_df, left_on="POSTCODE", right_on="postcode")
total_crime = (crime_merged["TOTAL_CRIME_COST"] * crime_merged["weight"]).sum() / value
print("Last known economic cost:", total_crime)

conn.close()
