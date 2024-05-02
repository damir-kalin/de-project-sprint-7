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

def input_event_paths(base_path,event_type, date, depth):
    dt = datetime.strptime(date, '%Y-%m-%d')
#     for event_type in ['message', 'reaction', 'subscription']:
    paths = [f"{base_path}/event_type={event_type}/date={(dt - timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]
    paths = [x for x in paths if check_path(x)]
    return paths

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
                                        'lat',
                                        'lon')\
                    .orderBy(F.asc('distance'))))\
        .where((F.col('distance')==F.col('min_distance')) | (F.col('distance').isNull()))
# \
#         .select(
#             F.col('event'),
#             F.col('lat'),
#             F.col('lon'),
#             F.col('city_id'),
#             F.col('city'),
#             F.col('city_lat'),
#             F.col('city_lon'),
#             F.col('event_type'),
#             F.col('date')
#         )

def add_local_time(df):
    df = df.select(F.col('event.message_from').alias('user_id'),
                 F.col('event.message_ts').alias('date'),
                 F.col('timezone'))\
            .withColumn('max_date', F.max('date').over(Window.partitionBy('user_id')))\
            .filter(F.col('date')==F.col('max_date'))\
            .withColumn('time_utc', F.date_format('max_date', "HH:mm:ss"))\
            .withColumn('local_time', F.from_utc_timestamp(F.col("time_utc"),F.coalesce(F.col('timezone'), F.lit('Australia/Sydney'))))\

    return df

def main():
    
    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("Leaning") \
                    .getOrCreate()
    
#     base_path = "/user/damirkalin/data/geo/events"
#     date = "2022-02-28"
#     depth = int("30")
#     geo_path = "/user/damirkalin/data/geo/geo.csv"
#     timezone_path = "/user/damirkalin/data/geo/timezone.csv"
#     output_path = "/user/damirkalin/analytics/project_sp7_recomendation_d30"
    
    base_path = sys.argv[1]
    date = sys.argv[2]
    depth = int(sys.argv[3])
    geo_path = sys.argv[4]
    timezone_path = sys.argv[5]
    output_path = sys.argv[6]
    
    
    subscriptions = spark\
            .read\
            .option("basePath", base_path)\
            .parquet(*input_event_paths(base_path, 'subscription', date, depth))\

    
    
    messages = spark\
            .read\
            .option("basePath", base_path)\
            .parquet(*input_event_paths(base_path, 'message', date, depth))\

#             .parquet("/user/damirkalin/data/geo/events/event_type=message/date=2022-02-27")\
    
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
    
    subscriptions = subscriptions.select(F.col('event.subscription_channel'), 
                              F.col('event.user').alias('user_left'))
    
    subscriptions = subscriptions.crossJoin(subscriptions.select(F.col('subscription_channel'), 
                                                          F.col('user_left').alias('user_right')))\
                                .filter(F.col('user_left')!=F.col('user_right'))\
                                .select(F.col('user_left'), F.col('user_right'))
    
    messages = add_city_in_events(messages, city_with_tz)
    
    df = messages.select(F.col('event.message_from').alias('from'), 
                         F.col('event.message_to').alias('to'), 
                         F.col('event.message_ts').alias('ts'),
                         F.col('lat'),
                         F.col('lon'),
                         F.col('date'),
                         F.col('city_id'),
                         F.col('timezone')
                        ).filter(F.col('to').isNotNull()).alias('orig')
    
    df_copy = df.alias('copy')
    
    df = df.join(df_copy, 
                 (F.col('orig.from')==F.col('copy.to'))\
                 & (F.col('orig.to')==F.col('copy.from')), 
                 'left_anti').alias('orig')
    
    df_copy = df.alias('copy')
    
    df = df.crossJoin(df_copy)\
            .filter((F.col('orig.from')!=F.col('copy.from'))\
                    & (F.col('orig.to')!=F.col('copy.to'))\
                    & (F.col('orig.date')==F.col('copy.date')))\
            .join(subscriptions, 
                  (F.col('orig.from')==F.col('user_left')) & (F.col('copy.from')==F.col('user_right')),
                  'inner'
                 )\
            .withColumn('distance_users', F.lit(2) 
                   * F.lit(6371) 
                   * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('orig.lat'))
                                               - F.radians(F.col('copy.lat')))/F.lit(2)),2)
                                   + (F.cos(F.radians(F.col('copy.lat')))
                                      * F.cos(F.radians(F.col('orig.lat')))
                                      * F.pow(F.sin((F.radians(F.col('orig.lon'))
                                                   - F.radians(F.col('copy.lon')))/F.lit(2)),2)))))\
            .filter((F.col('distance_users')<=1) & (F.col('orig.ts')>F.col('copy.ts')))\
            .withColumn('time_utc', F.date_format(F.col('orig.ts'), "HH:mm:ss"))\
            .withColumn('local_time', F.from_utc_timestamp(F.col("time_utc"),F.coalesce(F.col('orig.timezone'), F.lit('Australia/Sydney'))))\
            .withColumn('processed_dttm', F.current_timestamp())\
            .select(F.col('user_left'),
                    F.col('user_right'),
                    F.col('processed_dttm'),
                    F.col('orig.city_id').alias('zone_id'),
                    F.col('local_time')
                   )
    
    df.write.mode("overwrite").parquet(f"{output_path}/date={date}")
    
#     df.show(10,truncate=False)
    
#     df.printSchema()


    

if __name__ == '__main__':
    main()