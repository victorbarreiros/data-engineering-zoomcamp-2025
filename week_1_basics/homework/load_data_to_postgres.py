import pandas as pd
from time import time
from sqlalchemy import create_engine

df = pd.read_csv('green_tripdata_2019-10.csv', nrows=100)

engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')
engine.connect()

df_iter = pd.read_csv('green_tripdata_2019-10.csv', iterator=True, chunksize=100000)

df.head(n=0).to_sql(con=engine, name='green_taxi_data', if_exists='replace')

df_zones = pd.read_csv("taxi_zone_lookup.csv")
df_zones.head()
df_zones.to_sql(name='zones', con=engine, if_exists='replace')

while True:
    t_start = time()
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(con=engine, name='green_taxi_data', if_exists='append')
    t_end = time()

    print("Insert another chunk... in %.3f seconds" % (t_end - t_start) )


