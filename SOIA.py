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
niche=input("What niche are you interested in:? ")
postcode_nearby=int(input("Nearby Postcode: "))

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

# Get Sa2 Name
sa2_name=run_query_value(f"""
                         select sa2_name_2021
from geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020
where sa2_code_2021 = '{sa2}'
                         """)

print("Sa2 Name: ", sa2_name)

# Get Number of People
value = run_query_value(f"""
               select obs_value
               from abs_socioeconomic_indexes_for_areas_seifa_2021_data__free.seifa.seifa_sa2
where seifa_sa2 = '{sa2}' and unit_of_measure = 'Persons'
               Order by obs_value desc
               limit 1               
               """)
print("Population of the Sa2: ", value)

# Get Number of Businesses

geometry_string= run_query_value(f"""
                                 select geometry
from geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020
where sa2_code_2021 = '{sa2}'
limit 1
""")

sa2_geometry=json.loads(geometry_string)

leads = scan_businesses_in_sa2(
    sa2_geometry,
    step_meters=1000,
    radius=100,
    keyword=niche
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

nat_roads_density=(total_road_length/1000)/area

print("Roads Density: ", nat_roads_density, "m/km^2")

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

    # convert to per capita first
    per_capita_series = series / value

    # rolling average
    rolling_series = per_capita_series.rolling(window=window, min_periods=window).mean()

    # growth and acceleration
    growth = rolling_series.pct_change().iloc[-1]
    acceleration = rolling_series.pct_change().diff().iloc[-1]

    return growth, acceleration

# Total Residential
permit_value_residential=run_query(f"""
                       SELECT obs_value
FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
WHERE region_type = 'SA2: Statistical Area Level 2' and region = '{sa2}: {sa2_name}' and measure = '2: Value of building jobs' and building_type = '100: Total Residential' and work_type = 'TOT: Total Work' and sector = '9: Total Sectors'
ORDER By time_period asc
                       """)

growth, acceleration = rolling_growth_score(
    permit_value_residential["OBS_VALUE"],
    value
)

print("Residential growth:", growth)
print("Residential acceleration:", acceleration)

# Total non-residential
permit_value_nonresidential=run_query(f"""
                       SELECT obs_value
FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
WHERE region_type = 'SA2: Statistical Area Level 2' and region = '{sa2}: {sa2_name}' and measure = '2: Value of building jobs' and building_type = '800: Dwellings excluding new residential' and work_type = 'TOT: Total Work' and sector = '9: Total Sectors'
ORDER By time_period asc
                       """)

growth, acceleration = rolling_growth_score(
    permit_value_nonresidential["OBS_VALUE"],
    value
)

print("Non-Residential growth:", growth)
print("Non-Residential acceleration:", acceleration)

# Total Commercial
permit_value_commercial=run_query(f"""
                       SELECT obs_value
FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
WHERE region_type = 'SA2: Statistical Area Level 2' and region = '{sa2}: {sa2_name}' and measure = '2: Value of building jobs' and building_type = '200: Commercial Buildings - Total' and work_type = 'TOT: Total Work' and sector = '9: Total Sectors'
ORDER By time_period asc
                       """)

growth, acceleration = rolling_growth_score(
    permit_value_commercial["OBS_VALUE"],
    value
)

print("Commercial growth:", growth)
print("Commercial acceleration:", acceleration)

# Total Industrial
permit_value_industrial=run_query(f"""
                       SELECT obs_value
FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
WHERE region_type = 'SA2: Statistical Area Level 2' and region = '{sa2}: {sa2_name}' and measure = '2: Value of building jobs' and building_type = '300: Industrial Buildings - Total' and work_type = 'TOT: Total Work' and sector = '9: Total Sectors'
ORDER By time_period asc
                       """)

growth, acceleration = rolling_growth_score(
    permit_value_industrial["OBS_VALUE"],
    value
)

print("Industrial growth:", growth)
print("Industrial acceleration:", acceleration)

# Total Education
permit_value_education=run_query(f"""
                       SELECT obs_value
FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
WHERE region_type = 'SA2: Statistical Area Level 2' and region = '{sa2}: {sa2_name}' and measure = '2: Value of building jobs' and building_type = '410: Education buildings' and work_type = 'TOT: Total Work' and sector = '9: Total Sectors'
ORDER By time_period asc
                       """)

growth, acceleration = rolling_growth_score(
    permit_value_education["OBS_VALUE"],
    value
)

print("Education growth:", growth)
print("Education acceleration:", acceleration)

# Total Health
permit_value_health=run_query(f"""
                       SELECT obs_value
FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
WHERE region_type = 'SA2: Statistical Area Level 2' and region = '{sa2}: {sa2_name}' and measure = '2: Value of building jobs' and building_type = '440: Health buildings' and work_type = 'TOT: Total Work' and sector = '9: Total Sectors'
ORDER By time_period asc
                       """)

growth, acceleration = rolling_growth_score(
    permit_value_health["OBS_VALUE"],
    value
)

print("Health growth:", growth)
print("Health acceleration:", acceleration)

# Total Entertainment
permit_value_entertainment=run_query(f"""
                       SELECT obs_value
FROM construction_activity__australia__free.CONSTRUCTION_AUS_FREE.ABS_BUILDING_APPROVALS_ALL_LEVELS
WHERE region_type = 'SA2: Statistical Area Level 2' and region = '{sa2}: {sa2_name}' and measure = '2: Value of building jobs' and building_type = '450: Entertainment and recreation buildings' and work_type = 'TOT: Total Work' and sector = '9: Total Sectors'
ORDER By time_period asc
                       """)

growth, acceleration = rolling_growth_score(
    permit_value_entertainment["OBS_VALUE"],
    value
)

print("Entertainment growth:", growth)
print("Entertainment acceleration:", acceleration)


# Crime
postcode_lower_bound = postcode_nearby - 50
postcode_upper_bound = postcode_nearby + 50

postcodes_df = run_query(f"""
select poa_code_2021, area_albers_sqkm, geometry
from geography__boundaries__insights__australia.geography_aus_free.abs_poa_2021_aust_gda2020
where poa_code_2021 > '{postcode_lower_bound}'
  and poa_code_2021 < '{postcode_upper_bound}'
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
print(weights_df)

total_crime=0

for _, row in weights_df.iterrows():
    postcode_target=row["postcode"]
    weighting=row["weight"]

    postcode_crime_data=run_query_value(f"""
                                  WITH crime_unpivot AS (

    SELECT
        subcategory,
        month,
        value
    FROM (select*from crime__statistics__australia__free.crime_statistics_aus_free.nsw_boscar_postcode_crime_statistics
    where postcode = '{postcode_target}')

    
    UNPIVOT(
        value FOR month IN (
            dec_2022
        )
    )

),

crime_costs AS (

    SELECT
        month,

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

SELECT
    SUM(crime_cost) AS total_crime_cost
FROM crime_costs
GROUP BY month
ORDER BY to_date(month, 'mon_yyyy') desc
                                  """)
    
    total_crime *= weighting
    total_crime += postcode_crime_data

total_crime /= value
print("last known economic cost: ", total_crime)
