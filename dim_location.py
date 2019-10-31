import petl as etl
import psycopg2
import pandas as pd
from sqlalchemy  import create_engine




def dimension_values():
    connection = psycopg2.connect(dbname='voyager', user='voy', password='voyager', host='172.16.0.45')
    engine = create_engine('postgresql://voy:voyager@172.16.0.45:5432/voyager')

    dim_loc = "select distinct(name), latitude, longitude,  altitude from weatherstations "

    table = etl.fromdb(connection, dim_loc)

    df = pd.DataFrame(table)

    df.columns = df.iloc[0]

    df = df.drop_duplicates()

    df = df.reset_index()
    df = df.drop('index', axis=1)
    df = df.reset_index()
    df = df.rename(columns={'index':'id'})


    df.to_sql("dim_loc", engine, if_exists='append', index=False, method='multi')

    print(df)

dimension_values()