import petl as etl
import psycopg2
import pandas as pd
from sqlalchemy  import create_engine




def dimension_values():
    connection = psycopg2.connect(dbname='voyager', user='voy', password='voyager', host='172.16.0.45')
    engine = create_engine('postgresql://voy:voyager@172.16.0.45:5432/voyager')

    knmi_dim_time = "select distinct((to_date(CAST(date AS text), 'YYYYMMDD'))::timestamp + interval '1h' * hour) as time from knmi_station_data"

    lmn_dim_time = "SELECT Distinct(left(timestamp, -6)) as time from luchtmeetnet_data"

    knmi_table = etl.fromdb(connection, knmi_dim_time)
    lmn_table = etl.fromdb(connection, lmn_dim_time)

    df_knmi = pd.DataFrame(knmi_table)
    df_lmn = pd.DataFrame(lmn_table)

    df_knmi.columns = df_knmi.iloc[0]
    df_knmi = df_knmi.drop(0)

    df_lmn.columns = df_lmn.iloc[0]
    df_lmn = df_lmn.drop(0)

    df_total = df_knmi.append(df_lmn)

    df_total = df_total.drop_duplicates()

    df_total = df_total.reset_index()
    df_total = df_total.drop('index', axis=1)
    df_total = df_total.reset_index()
    df_total = df_total.rename(columns={'index':'id'})

    df_total['time'] = df_total['time'].astype(str)

    df_total[['Year', 'Month', 'Day']] = df_total.time.str.split("-", expand=True, )
    df_total[['Day', 'Hour']] = df_total.Day.str.split(" ", expand=True, )
    df_total[['Hour', 'Minute', 'Second']] = df_total.Hour.str.split(":", expand=True, )
    df_total.to_sql('dim_time', engine, if_exists='append', index=False, method='multi')

    print(df_total)

dimension_values()