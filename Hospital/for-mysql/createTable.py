from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

from sqlalchemy import text

def createTable(engine, tablename):
    try:
        with engine.connect() as con:
            con.execute(text(f"CALL createCountryTable(:tablename)"), {'tablename': tablename})
    except Exception as e:
        print(e)

# def createTable(engine, tablename):
#     try:
#         with engine.connect() as con:
#             con.execute("call createCountryTable(\"" + tablename + "\")")
#     except Exception as e:
#         print(e)


def createTables(engine, inspector, db, distinct_countries, existing_tables):
    for table in distinct_countries:
        if table in existing_tables:
            # using append can cause primary key error
            # thus its a good idea to drop tables first
            print(table + " already exists. Dropping")
            with engine.connect() as con:
                con.execute(text(f"DROP TABLE  IF EXISTS {table}"))
                # con.execute("drop table incubyte." + table)
                print("Dropped")
        print("trying to create " + table)
        try:
            createTable(engine, table)
            print("Created")
        except Exception as e:
            print(e)
