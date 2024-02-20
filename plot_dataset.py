

o

import pandas as pd
import sqlite3

# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("data/portal_mammals.sqlite")
df = pd.read_sql_query("SELECT * from surveys", con)

# Verify that result of SQL query is stored in the dataframe
print(df.head())

con.close()


df = pd.read_csv("fire_archive_M6_96619.csv",
                 usecols=["latitude", "longitude", "brightness",
                 "acq_date"], parse_dates=["acq_date"])df.head()


df.plot(x="longitude", y="latitude", kind="scatter", c="brightness",
        colormap="YlOrRd")



====

countries_gdf = geopandas.read_file("package.gpkg", layer='countries')



