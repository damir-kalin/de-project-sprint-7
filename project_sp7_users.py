import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window  import Window
import pyspark.sql.functions as F
import subprocess
from datetime import datetime, timedelta
import sys


check_path = lambda x: True if subprocess.run(['hdfs', 'dfs', '-ls', x], capture_output=True, text=True).stdout else False

def input_event_paths(base_path, date, depth):
    dt = datetime.strptime(date, '%Y-%m-%d')
    path_message = [f"{base_path}/date={(dt - timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]
    path_message = [x for x in path_message if check_path(x)]
    return path_message


def add_city_in_events(events, city):

    df = events.crossJoin(city)

    df = df.withColumn('distance', F.lit(2) 
                   * F.lit(6371) 
                   * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat'))
                                               - F.radians(F.col('city_lat')))/F.lit(2)),2)
                                   + (F.cos(F.radians(F.col('city_lat')))
                                      * F.cos(F.radians(F.col('lat')))
                                      * F.pow(F.sin((F.radians(F.col('lon'))
                                                   - F.radians(F.col('city_lon')))/F.lit(2)),2)))))

    return df.withColumn('min_distance', F.min(F.col('distance'))\
                   .over(Window.partitionBy('event', 
#                                         'event_type', 
                                        'lat',
                                        'lon')\
                    .orderBy(F.asc('distance'))))\
        .where(F.col('distance')==F.col('min_distance'))\
        .select(
            F.col('event'),
#             F.col('event_type'),
            F.col('lat'),
            F.col('lon'),
            F.col('city_id'),
            F.col('city'),
            F.col('city_lat'),
            F.col('city_lon'),
            F.col('timezone'),
            F.col('distance')
        )

def get_act_city(df):
    return df.withColumn('act_city', 
                    F.first(F.col('city')).over(Window.partitionBy('user_id').orderBy(F.desc('date'))))\
        .select(F.col('user_id'),
               F.col('act_city'))\
        .distinct()

def get_home_city(df):
    return df.withColumn('lag_city', 
                       F.lag('city').over(Window.partitionBy('user_id').orderBy('date')))\
            .filter((F.col('city')!= F.col('lag_city'))|(F.col('lag_city').isNull()))\
            .withColumn('lag_date',
                       F.lag('date').over(Window.partitionBy('user_id').orderBy('date')))\
            .withColumn('diff_date', F.datediff( F.col('date'), F.col('lag_date')))\
            .filter(F.col('diff_date')>27)\
            .select(F.col('user_id').alias('h_user_id'), 
                    F.col('lag_city').alias('home_city'))


def add_cities(df):
    df = df.select(F.col('event.message_from').alias('user_id'),
                 F.col('event.message_ts').alias('date'),
                 F.col('city'))
    
    df_act_city = get_act_city(df)
    df_home_city = get_home_city(df)
    
    df = df_act_city.join(df_home_city, df_act_city.user_id==df_home_city.h_user_id, 'left')\
            .drop(F.col('h_user_id'))\
            .select(F.col('user_id'), F.col('act_city'), F.col('home_city'))
    return df

def add_travel_data(df):
    df = df.select(F.col('event.message_from').alias('user_id'),
                 F.col('event.message_ts').alias('date'),
                 F.col('city'))
    
    return df.withColumn('lag_city', 
                       F.lag('city').over(Window.partitionBy('user_id').orderBy('date')))\
            .filter((F.col('city')!= F.col('lag_city'))|(F.col('lag_city').isNull()))\
            .groupBy('user_id')\
            .agg(F.count('city').alias('travel_count'), F.collect_list('city').alias('travel_array'))\
            .select(F.col('user_id').alias('t_user_id'),
                   F.col('travel_count'),
                   F.col('travel_array'))
    
def add_local_time(df):
    df = df.select(F.col('event.message_from').alias('user_id'),
                 F.col('event.message_ts').alias('date'),
                 F.col('timezone'))\
            .withColumn('max_date', F.max('date').over(Window.partitionBy('user_id')))\
            .filter(F.col('date')==F.col('max_date'))\
            .withColumn('time_utc', F.date_format('max_date', "HH:mm:ss"))\
            .withColumn('local_time', F.from_utc_timestamp(F.col("time_utc"),F.coalesce(F.col('timezone'), F.lit('Australia/Sydney'))))\
            .select(F.col('user_id').alias('lt_user_id')
                    , F.col('local_time'))

    return df

def main():
    
    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("Leaning") \
                    .getOrCreate()
    
#     base_path = "/user/damirkalin/data/geo/events/event_type=message"
# # "/user/damirkalin/data/geo/events/event_type=message"
# # "/user/master/data/geo/events"
#     date = "2022-06-21"
#     depth = int("28")
#     geo_path = "/user/damirkalin/data/geo/geo.csv"
#     timezone_path = "/user/damirkalin/data/geo/timezone.csv"
#     output_path = "/user/damirkalin/analytics/project_sp7_users_d28"
    
    base_path = sys.argv[1]
    date = sys.argv[2]
    depth = int(sys.argv[3])
    geo_path = sys.argv[4]
    timezone_path = sys.argv[5]
    output_path = sys.argv[6]
    
    
    events = spark.read.parquet(*input_event_paths(base_path, date, depth))\
        .where(F.col('event.message_from').isNotNull() & F.col('event.message_ts').isNotNull())
    
    timezone = spark.read\
        .csv(timezone_path,
          sep=',',
          header=True,
          inferSchema=True)\
        .select(F.col('city').alias('t_city'), F.col('timezone'))
    
    city = spark.read\
        .csv(geo_path,
          sep=';',
          header=True,
          inferSchema=True)\
        .withColumn("city_lat", F.regexp_replace("lat", ",", ".").cast("double"))\
        .withColumn("city_lon", F.regexp_replace("lng", ",", ".").cast("double"))\
        .drop('lat', 'lng')\
        .select(F.col('id').alias('city_id'),
               F.col('city'),
               F.col('city_lat'),
               F.col('city_lon'))
    
    city_with_tz = city.join(timezone, city.city==timezone.t_city, 'left').drop('t_city')
                        

    
    df_event_city = add_city_in_events(events, city_with_tz)
    
    df_cities = add_cities(df_event_city)
    df_travel = add_travel_data(df_event_city)
    df_local_time = add_local_time(df_event_city)
    
    df = df_cities.join(df_travel, df_cities.user_id==df_travel.t_user_id, 'left')\
            .drop(df_travel.t_user_id)\
            .join(df_local_time, df_cities.user_id==df_local_time.lt_user_id, 'left')\
            .drop(df_local_time.lt_user_id)
    
#     df.show(20, truncate=False)
#     df.printSchema()

    df.write.mode("overwrite").parquet(f"{output_path}/date={date}")
    
    
    
    
    
    

if __name__ == '__main__':
    main()