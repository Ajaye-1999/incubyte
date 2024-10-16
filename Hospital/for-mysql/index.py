
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect
from createTable import createTables


def getTables(engine):
    # If inspector is Not called again,
    # it will not refresh the tables from database, thus using function

    # Performing database inspection
    inspector = inspect(engine)

    # Getting list of tables from "incubyte" database
    all_tables = [tbl for tbl in inspector.get_table_names(schema=db)]

    return all_tables, inspector


df = pd.read_csv('C:/Users/1117a/OneDrive/Desktop/Hospital/Hospital/data/Customers_20241016161530.txt', sep="|", header=None)
print(df.shape[1])  # Prints the number of columns
print(df.columns)  # Displays all current column headers
print(df.head())
df.drop(columns=df.columns[0], inplace=True)

# header may or may not exist, so using `skiprows = 1` is not good idea
is_header = df.iloc[0, 0]
is_trailer = df.iloc[df.shape[0] - 1, 0]

# checking if Header Records exists
if is_header == 'H':
    df.drop(df.head(1).index, inplace=True)

# Naming the Columns
df.columns = ["D", "customerName", "customerID",
              "customerOpenDate", "lastConsultedDate",
              "vaccinationType", "doctorConsulted",
              "state", "country", "postCode",
              "dateofBirth", "activeCustomer"]

# Checking if Trailer record exists
if is_trailer == 'T':
    df.drop(df.tail(1).index, inplace=True)

# Dropping D columns as it of no use
del df['D']

# customerID is considered as float by pandas, so casting to int
df['customerID'] = df['customerID'].apply(np.int64)

# Setting customerID as index for faster operations
df.set_index('customerID')

# converting all empty string to nan, so that we can handle null country
# TOOK HELP FROM THIS SOURCE
# https://stackoverflow.com/a/21942746/11605100
df = df.replace(r'^\s*$', np.nan, regex=True)
df = df[df['country'].notna()]
df['lastConsultedDate'] = pd.to_datetime(df['lastConsultedDate'])

# Sort the DataFrame by 'lastconsulteddate' in descending order
df.sort_values(by='lastConsultedDate', ascending=False, inplace=True)

# Drop duplicates based on 'customerid', keeping the first occurrence (latest date)
df.drop_duplicates(subset='customerID', keep='first', inplace=True)

# here date is treated as string
print(df.info(), end="\n\n")

# Converting String to Dates
try:
    df['customerOpenDate'] = pd.to_datetime(
        df['customerOpenDate'], format='%Y%m%d',
        errors='coerce')
    df['lastConsultedDate'] = pd.to_datetime(
        df['lastConsultedDate'], format='%Y%m%d',
        errors='coerce')
    df['dateofBirth'] = pd.to_datetime(
        df['dateofBirth'], format='%d%m%Y',
        errors='coerce')
except Exception as e:
    print(e)


# TOOK HELP FROM THIS SOURCE:
# https://stackoverflow.com/a/13413845/11605100
# customerOpenDate should not be null
# coz these are not null in database, thus we have to drop those records
df = df[df['customerOpenDate'].notna()]

# here date is treated as date
print()
print(df.info(), end="\n\n")
print(df)
# exit(0)

# lower is applied here and not in `distinct_countries`
# coz we need to fetch rows also
df['country'] = df['country'].str.lower()

# Getting Distinct Countries
distinct_countries = df['country'].drop_duplicates()

print("\nDistinct Countries:\n", distinct_countries)

# Connecting to Database
print()
db = "incubyte"
try:
    engine = create_engine(
        "mysql+mysqlconnector://root:Ajayerao%401999@localhost/incubyte")
    engine.connect()
    print("Database Connected")
except Exception as e:
    print(e)

# Getting inspector and list of tables from "incubyte" database
existing_tables, inspector = getTables(engine)
print("Existing Tables:", existing_tables)

# creating tables that does not exists in distinct_countries
createTables(engine, inspector, db, distinct_countries, existing_tables)

# Getting inspector and list of tables from "incubyte" database
existing_tables, inspector = getTables(engine)
print("Existing Tables:", existing_tables)

#Add age and last_consulted_days tO THE Dataframe to df
if 'dateofBirth' in df.columns:
    # Convert 'dateofBirth' column to datetime if it's not already
    df['dateofBirth'] = pd.to_datetime(df['dateofBirth'], errors='coerce')
    
    # Convert 'lastconsultantdate' column to datetime if it's not already
    df['lastConsultedDate'] = pd.to_datetime(df['lastConsultedDate'], errors='coerce')

    # Get today's date
    today = pd.Timestamp(datetime.today())

    # Calculate 'age' by subtracting 'dateofBirth' from today's date, convert to years
    df['age'] = ((today - df['dateofBirth']).dt.days / 365.25).astype(int)

    # Calculate 'lastconsulteddays' as the number of days since the last consultant date
    df['last_consulted_days'] = (today - df['lastConsultedDate']).dt.days

    # Now you can proceed with the rest of your code
else:
    print("The 'dateofBirth' column is not present in the DataFrame.")
# inserting records as per country
for country in distinct_countries:
    my_filt = (df['country'] == country)
    try:
        print("Inserting Records in " + country)

        # `to_sql` this will create table if table does not exists,
        # which will avoid constraints like pk, so using if condition
        if country in existing_tables:
            df[my_filt].to_sql(
                name=country, con=engine,
                if_exists='append', index=False)
            print("Inserted")
        else:
            print(country + " table does Not exists")
    except Exception as e:
        print(e)
