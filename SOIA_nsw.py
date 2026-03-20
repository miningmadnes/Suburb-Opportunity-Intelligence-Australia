import os
import csv
import pandas as pd
import snowflake.connector
import json
import math
from shapely.geometry import shape
from shapely.geometry import Point
from shapely.ops import transform
from pyproj import Transformer
from local_business_directory import scan_businesses_in_sa2
from dotenv import load_dotenv
from scipy.spatial import KDTree

niche=input("What niche are you interested in? ")

output_file = f"nsw_{niche}_results.csv"
state="NSW"

if not os.path.exists(output_file):
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "sa2_code", "sa2_name", "area_km2", "population",
            "irsad_score", "ier_score", "ieo_score", "irsd_score",
            "leads_count", "sd_now",
            "weighted_rating", "avg_reviews", "competition_strength",
            "roads_density_km_per_km2", "avg_hospital_dist_km",
            "petrol_density", "stations_density",
            "residential_growth", "residential_accel",
            "nonresidential_growth", "nonresidential_accel",
            "commercial_growth", "commercial_accel",
            "industrial_growth", "industrial_accel",
            "education_growth", "education_accel",
            "health_growth", "health_accel",
            "entertainment_growth", "entertainment_accel",
            "crime_cost_per_capita"
        ])

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

# Define Golobal Functions
def run_query(query): #DataFrame query
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetch_pandas_all()
    finally:
        cur.close()

def run_query_value(query): #Datapoint query
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetchone()[0]
    finally:
        cur.close()


transformer = Transformer.from_crs(
    "EPSG:4326",   # SA2 lon/lat
    "EPSG:3857",   # road CRS
    always_xy=True
)

sa2_list=run_query("""
select sa2_code_2021
from geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020
where state_name_2021 = 'New South Wales'
AND gccsa_name_2021 NOT LIKE 'Migratory - Offshore - Shipping (NSW)'
AND gccsa_name_2021 NOT LIKE 'No usual address (NSW)'
AND gccsa_name_2021 NOT LIKE '%Outside Australia%'
AND area_albers_sqkm > 0
order by area_albers_sqkm desc"""
)

completed = set()
if os.path.exists(output_file):
    with open(output_file, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            completed.add(row[0])  # sa2_code is first column

for _, row in sa2_list.iterrows():
    sa2_code = row["SA2_CODE_2021"]
    
    if sa2_code in completed:
        print(f"Skipping {sa2_code} — already done")
        continue
    
    try:
                # Get Number of People
        sa2_info = run_query(f"""
                            SELECT sa2_name_2021, area_albers_sqkm, geometry
            FROM geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020
            WHERE sa2_code_2021 = '{sa2_code}'
                            """)

        sa2_name = sa2_info["SA2_NAME_2021"].iloc[0]
        area = float(sa2_info["AREA_ALBERS_SQKM"].iloc[0])
        geometry_string = sa2_info["GEOMETRY"].iloc[0]

        print("Working on: ", sa2_name)

        seifa_df = run_query(f"""
            SELECT index_type, unit_of_measure, obs_value
            FROM abs_socioeconomic_indexes_for_areas_seifa_2021_data__free.seifa.seifa_sa2
            WHERE seifa_sa2 = '{sa2_code}'
            AND unit_of_measure IN ('Score', 'Persons')
        """)

        # Population — same across all indexes so just take first
        population_row = seifa_df[seifa_df["UNIT_OF_MEASURE"] == "Persons"]
        sa2_population = float(population_row["OBS_VALUE"].iloc[0])

        # Scores
        scores = seifa_df[seifa_df["UNIT_OF_MEASURE"] == "Score"]
        seifa = dict(zip(scores["INDEX_TYPE"], scores["OBS_VALUE"]))

        irsad_score = float(seifa.get("Index of Relative Socio-economic Advantage and Disadvantage", 0))
        ier_score   = float(seifa.get("Index of Economic Resources", 0))
        ieo_score   = float(seifa.get("Index of Education and Occupation", 0))
        irsd_score  = float(seifa.get("Index of Relative Socio-economic Disadvantage", 0))

        # Get Number of Businesses

        sa2_geometry=json.loads(geometry_string)

        radius_calculation=math.sqrt(area)*100
        radius_calculation=max(100, min(radius_calculation, 5000))
        step_metres_calculation=radius_calculation*1.73

        population_density = float(sa2_population) / area  # people per km²

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

        # Competition Quality Score
        if len(leads) > 0:
            # Weighted average rating — weights higher-reviewed businesses more heavily
            total_reviews = sum(lead[6] for lead in leads if lead[6])
            
            if total_reviews > 0:
                weighted_rating = sum(
                    lead[5] * lead[6] 
                    for lead in leads 
                    if lead[5] and lead[6]
                ) / total_reviews
            else:
                weighted_rating = 0

            avg_reviews = total_reviews / len(leads)

            # Competition strength — combines how good AND how established competitors are
            # Scale 0-1: higher = stronger competition, harder to enter
            competition_strength = (weighted_rating / 5) * min(avg_reviews / 500, 1)

        else:
            weighted_rating = 0
            avg_reviews = 0
            competition_strength = 0

        # Does S/D Now Calculation

        if sa2_population is not None:
            SDNow = len(leads) / float(sa2_population)
        else:
            print("No value returned from population numbers, check sa2")

        # Get the geometry bounds to be the same as the projection in the database

        # Outlines SA2 Bounds
        sa2_geom = shape(json.loads(geometry_string))

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

        avg_dist_km = avg_dist_m / 1000 if avg_dist_m is not None else None

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
            AND region = '{sa2_code}: {sa2_name}'
            AND measure = '2: Value of building jobs'
            AND building_type IN ({type_list})
            AND work_type = 'TOT: Total Work'
            AND sector = '9: Total Sectors'
            ORDER BY time_period ASC
        """)

        permit_results = {}
        for label, btype in building_types.items():
            series = permits_df[permits_df["BUILDING_TYPE"] == btype]["OBS_VALUE"]
            growth, acceleration = rolling_growth_score(series, sa2_population)
            permit_results[label] = (growth, acceleration)

        # Crime
        postcodes_df = run_query(f"""
            SELECT poa_code_2021, ST_ASGEOJSON(p.geometry) AS geometry
            FROM geography__boundaries__insights__australia.geography_aus_free.abs_poa_2021_aust_gda2020 p
            JOIN geography__boundaries__insights__australia.geography_aus_free.abs_sa2_2021_aust_gda2020 s
                ON ST_INTERSECTS(
                    TO_GEOMETRY(p.geometry),
                    TO_GEOMETRY(s.geometry)
                )
            WHERE s.sa2_code_2021 = '{sa2_code}'
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
        total_crime = (crime_merged["TOTAL_CRIME_COST"] * crime_merged["weight"]).sum() / sa2_population

        with open(output_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                sa2_code, sa2_name, area, sa2_population,
                irsad_score, ier_score, ieo_score, irsd_score,
                len(leads), SDNow,
                weighted_rating, avg_reviews, competition_strength,
                nat_roads_density, avg_dist_km,
                petrol_density, stations_density,
                permit_results["Residential"][0], permit_results["Residential"][1],
                permit_results["Non-Residential"][0], permit_results["Non-Residential"][1],
                permit_results["Commercial"][0], permit_results["Commercial"][1],
                permit_results["Industrial"][0], permit_results["Industrial"][1],
                permit_results["Education"][0], permit_results["Education"][1],
                permit_results["Health"][0], permit_results["Health"][1],
                permit_results["Entertainment"][0], permit_results["Entertainment"][1],
                total_crime
            ])
        print(f"Written {sa2_code} — {sa2_name}")

    except Exception as e:
        print(f"Failed on {sa2_code}: {e}")
        continue

conn.close()
