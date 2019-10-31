import petl as etl
import psycopg2
import pandas as pd
from sqlalchemy  import create_engine




def dimension_values():
    connection = psycopg2.connect(dbname='voyager', user='voy', password='voyager', host='172.16.0.45')
    engine = create_engine('postgresql://voy:voyager@172.16.0.45:5432/voyager')

    knmi_dim_com = "SELECT column_name as name FROM information_schema.columns " \
          "WHERE table_schema = 'public' " \
          "AND table_name = 'knmi_station_data'"

    lmn_dim_com = "SELECT name FROM luchtmeetnet_sensors"

    knmi_table = etl.fromdb(connection, knmi_dim_com)
    lmn_table = etl.fromdb(connection, lmn_dim_com)

    df_knmi = pd.DataFrame(knmi_table)
    df_lmn = pd.DataFrame(lmn_table)

    df_knmi.columns = df_knmi.iloc[0]
    df_knmi = df_knmi.drop(0)

    df_lmn.columns = df_lmn.iloc[0]
    df_lmn = df_lmn.drop(0)

    wid = df_knmi.loc[df_knmi['name']=='weather_station_id'].index
    df_knmi = df_knmi.drop(wid)

    date = df_knmi.loc[df_knmi['name'] == 'date'].index
    df_knmi = df_knmi.drop(date)

    hour = df_knmi.loc[df_knmi['name'] == 'hour'].index
    df_knmi = df_knmi.drop(hour)

    index = df_knmi.loc[df_knmi['name'] == 'index'].index
    df_knmi = df_knmi.drop(index)

    df_total = df_knmi.append(df_lmn)
    df_total = df_total.reset_index()
    df_total = df_total.drop('index',axis=1)
    df_total = df_total.reset_index()

    df_total = df_total.rename(columns={'index':'id'})


    print(df_total)

    df_total.to_sql('dim_com', engine, if_exists='append', index=False, method='multi')

dimension_values()