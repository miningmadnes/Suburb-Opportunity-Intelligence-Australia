import os
import pandas as pd
import snowflake.connector
import local_business_directory
import json
from local_business_directory import scan_businesses_in_sa2
from dotenv import load_dotenv

sa2=input("Gimme Sa2 code pls: ")

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
               from seifa.seifa_sa2
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
    step_meters=100,
    radius=100,
    keyword="Cafe"
)

print("Number of leads:", len(leads))

if value is not None:
    SDNow = len(leads) / float(value)
    print(SDNow)
else:
    print("No value returned from population numbers, check sa2")
