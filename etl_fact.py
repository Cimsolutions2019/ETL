import petl as etl
import psycopg2
import pandas as pd
from datetime import datetime
from sqlalchemy  import create_engine




def dimension_values():
    connection = psycopg2.connect(dbname='voyager', user='voy', password='voyager', host='172.16.0.45')
    engine = create_engine('postgresql://voy:voyager@172.16.0.45:5432/voyager')

    com = 'select id as id_component, name as component from dim_com'
    table_com = etl.fromdb(connection, com)

    loc = 'select id as id_location, name as name from dim_loc'
    table_loc = etl.fromdb(connection, loc)

    tim = 'select id as id_time, time as timestamp from dim_time'
    table_time = etl.fromdb(connection, tim)

    print(table_com)
    print(table_loc)
    print(table_time)

    for ran in range(0,65424,1000):
        sql = "select * from KNMI_station_data kk " \
              "RIGHT JOIN weatherstations w ON " \
              " CAST (kk.weather_station_id AS INTEGER)  = CAST (w.station_number AS INTEGER) " \
              "WHERE w.station_number NOT LIKE \'NL%%\' AND date > 20190901 LIMIT 1000 OFFSET %s" % ran
        print(sql)
        table = etl.fromdb(connection, sql)

        print('knmi')
        print(table)
        table.log_progress()
        table = etl.convert(table, 'date', str)
        table = etl.convert(table, 'hour', str)

        table = etl.convert(table, 'temperature', int)
        table = etl.convert(table, 'temperature_dew', int)
        table = etl.convert(table, 'temperature_min', int)
        table = etl.convert(table, 'wind_speed_avg', int)
        table = etl.convert(table, 'wind_speed', int)
        table = etl.convert(table, 'wind_speed_max', int)


        table = etl.convert(table, 'temperature', lambda v: v/10)
        table = etl.convert(table, 'temperature_dew', lambda v: v/10)
        table = etl.convert(table, 'temperature_min', lambda v: v/10)
        table = etl.convert(table, 'wind_speed_avg', lambda v: v/10)
        table = etl.convert(table, 'wind_speed', lambda v: v/10)
        table = etl.convert(table, 'wind_speed_max', lambda v: v/10)

        df = pd.DataFrame(table)
        df.columns = df.iloc[0]
        df = df.drop(0)
        df['timestamp'] = df['date'] + df['hour']

        df['weather_station_id'] = df['weather_station_id'].astype(str)
        df['timestamp'] = df['timestamp'].apply(custom_to_datetime)
        df['timestamp'] = df['timestamp'].astype(str)

        df = df.drop(columns=['date','hour'], axis=1)

        final_knmi_table = etl.fromdataframe(df)



        final_knmi_table = etl.melt(final_knmi_table, key=['weather_station_id','timestamp', 'id', 'latitude', 'longitude', 'name', 'station_number', 'data_source_id', 'altitude'])
        final_knmi_table = etl.rename(final_knmi_table, 'variable','component')
        print(final_knmi_table)

        final_knmi_table2 = etl.join(final_knmi_table, table_com, key='component')
        final_knmi_table2 = etl.join(final_knmi_table2, table_loc, key='name')
        final_knmi_table2 = etl.join(final_knmi_table2, table_time, key='timestamp')
        print('dos')

        print(final_knmi_table2)
        df = pd.DataFrame(final_knmi_table2)
        df.columns = df.iloc[0]
        df = df.drop(0)
        fact_source = df[['id_component', 'id_location', 'id_time', 'value','data_source_id','weather_station_id']]

        print(fact_source)
        fact_source.to_sql('fact_source', engine, if_exists='append', index=False, method='multi')




    for rn in range(0, 1148, 1000):
        print('lmn')
        final_lmn_table = etl.fromdb(connection,
                                     "select ld.id, ld.station_number, ld.value, ld.timestamp, ls.name as component, "
                                     "ws.id as lid, ws.latitude, ws.longitude, ws.data_source_id, ws.altitude, ws.name as name"
                                     " from luchtmeetnet_data ld "
                                     "right join luchtmeetnet_sensors ls on ld.formula = ls.formula "
                                     " join weatherstations ws on ld.station_number = ws.station_number "
                                     "where ws.station_number like \'NL%%\' AND timestamp > '2019-09-01' "
                                     "LIMIT 1000 OFFSET %s" % rn)
        final_lmn_table = etl.rename(final_lmn_table, {'station_number': 'weather_station_id'})
        final_lmn_table = etl.movefield(final_lmn_table, 'timestamp', 1)
        # print(final_lmn_table)
        # print(final_lmn_table)

        # print(table_com)
        final_lmn_table2 = etl.join(final_lmn_table, table_com, key='component')
        # print(final_lmn_table2)

        final_lmn_table2 = etl.join(final_lmn_table2, table_loc, key='name')
        # print(final_lmn_table2)
        df = pd.DataFrame(final_lmn_table2)
        df.columns = df.iloc[0]
        df = df.drop(0)
        df['timestamp'] = df['timestamp'].str[:-6]
        # print(df)

        final_lmn_table2 = etl.fromdataframe(df)

        final_lmn_table2 = etl.join(final_lmn_table2, table_time, key='timestamp')
        # print(final_lmn_table2)


        print(final_lmn_table2)
        final_lmn_df = pd.DataFrame(final_lmn_table2)
        final_lmn_df.columns = final_lmn_df.iloc[0]
        final_lmn_df = final_lmn_df.drop(0)
        fact_source = final_lmn_df[['id_component', 'id_location', 'id_time', 'value', 'data_source_id', 'weather_station_id']]
        print(fact_source)

        fact_source.to_sql('fact_source', engine, if_exists='append', index=False, method='multi')

        # df = pd.DataFrame(final_lmn_table)
        # df.columns = df.iloc[0]
        # df = df.drop(0)
        # # print(df)
        # dim_date = df[['id', 'timestamp']]
        # dim_location = df[['id_location', 'latitude','longitude','weather_station_name','altitude']]
        # dim_values = df[['id', 'component', 'name','value']]
        # fact_source = df[['id','id_location', 'data_source_id', 'weather_station_id']]
        #
        # dim_date = dim_date.drop_duplicates()
        # dim_location = dim_location.drop_duplicates()
        # fact_source = fact_source.drop_duplicates()
        #
        #
        #
        # dim_date = dim_date.rename(columns={'id': 'index'})
        # fact_source = fact_source.rename(columns={'weather_station_id': 'station_number'})
        # dim_location = dim_location.rename(columns={'weather_station_name':'name'})
        #
        #
        # dim_date['timestamp'] = pd.to_datetime(dim_date['timestamp'])
        # dim_date['year'] = [d.date().year for d in dim_date['timestamp']]
        # dim_date['month'] = [d.date().month for d in dim_date['timestamp']]
        # dim_date['day'] = [d.date().day for d in dim_date['timestamp']]
        # dim_date['hour'] = [d.time().hour for d in dim_date['timestamp']]
        # dim_date['minute'] = [d.time().minute for d in dim_date['timestamp']]
        # dim_date['second'] = [d.time().second for d in dim_date['timestamp']]
        # dim_date = dim_date.drop('timestamp', axis=1)
        #
        #
        # dim_values = dim_values[['id', 'component', 'name', 'value']]
        # dim_values = dim_values.rename(columns={'id':'index'})
        #
        # fact_source['id_values'] = fact_source['id']
        # fact_source['id_date'] = fact_source['id']
        #
        # fact_source = fact_source.drop(columns=['id'], axis=1)
        #
        # print('dim_date')
        # print(dim_date)
        # print('dim_location')
        # print(dim_location)
        # print('dim_values')
        # print(dim_values)
        # print('fact_source')
        # print(fact_source)

        # dim_date.to_sql('dim_date', engine, if_exists='append', index=False, method='multi')
        # dim_location.to_sql('dim_location', engine, if_exists='append', index=False, method='multi')
        # dim_values.to_sql('dim_values', engine, if_exists='append', index=False, method='multi')
        # fact_source.to_sql('fact_source', engine, if_exists='append', index=False, method='multi')


def custom_to_datetime(date):
    # If the time is 24, set it to 0 and increment day by 1
    if date[8:10] == '24':
        return pd.to_datetime(date[:-2], format = '%Y%m%d') + pd.Timedelta(days=1)
    else:
        return pd.to_datetime(date, format = '%Y%m%d%H')

dimension_values()
