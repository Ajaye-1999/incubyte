
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect
from createTable import createTables


def getTables(engine):
    # If the inspector isn't called again, the tables won't refresh from the database, 
    # so we're using the function to perform a database inspection.
    inspector = inspect(engine)

    # Getting list of tables from "incubyte" database
    all_tables = [tbl for tbl in inspector.get_table_names(schema=db)]

    return all_tables, inspector


df = pd.read_csv('C:/Users/1117a/OneDrive/Desktop/Hospital/Hospital/data/Customers_20241016161530.txt', sep="|", header=None)
print(df.shape[1])  
print(df.columns)  
print(df.head())
df.drop(columns=df.columns[0], inplace=True)


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

#Pandas has recognized customerID as a float, so it will be cast to an integer
df['customerID'] = df['customerID'].apply(np.int64)

# Setting customerID as index for faster operations
df.set_index('customerID')

# Converting all empty strings to NaN so we can properly handle null values in the country field.
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


# customerOpenDate should not be null because these values are not null in the database, 
# so we need to drop any records with null values in this field.
df = df[df['customerOpenDate'].notna()]

# here date is treated as date
print()
print(df.info(), end="\n\n")
print(df)

df['country'] = df['country'].str.lower()

# Getting Distinct Countries
distinct_countries = df['country'].drop_duplicates()

print("\nDistinct Countries:\n", distinct_countries)

# Connecting to Database
print()
db = "incubyte"
try:
    engine = create_engine(
        "mysql+mysqlconnector://root:*******@localhost/incubyte")
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

    
else:
    print("The 'dateofBirth' column is not present in the DataFrame.")
# inserting records as per country
for country in distinct_countries:
    my_filt = (df['country'] == country)
    try:
        print("Inserting Records in " + country)

        if country in existing_tables:
            df[my_filt].to_sql(
                name=country, con=engine,
                if_exists='append', index=False)
            print("Inserted")
        else:
            print(country + " table does Not exists")
    except Exception as e:
        print(e)
