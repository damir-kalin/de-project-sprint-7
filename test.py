import os
import sys
import subprocess
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.window  import Window
import pyspark.sql.functions as F

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

check_path = lambda x: True if subprocess.run(['hdfs', 'dfs', '-ls', x], capture_output=True, text=True).stdout else False

def input_event_paths(base_path, event_type, date, depth):
    dt = datetime.strptime(date, '%Y-%m-%d')
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
        .where((F.col('distance')==F.col('min_distance')) )

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
#     date = "2022-05-31"
#     depth = int("7")
#     geo_path = "/user/damirkalin/data/geo/geo.csv"
#     timezone_path = "/user/damirkalin/data/geo/timezone.csv"
#     output_path = "/user/damirkalin/analytics/project_sp7_recomendation_d30"
    
    base_path = sys.argv[1]
    date = sys.argv[2]
    depth = int(sys.argv[3])
    geo_path = sys.argv[4]
    timezone_path = sys.argv[5]
    output_path = sys.argv[6]
    

    
    
    messages = spark\
            .read\
            .option("basePath", base_path)\
            .parquet(*input_event_paths(base_path, 'message', date, depth))
    
    subscriptions = spark\
            .read\
            .option("basePath", base_path)\
            .parquet(*input_event_paths(base_path, 'subscription', date, depth))
    
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
    
#     По некоторым городам невозможно определить временную зону.
#     Поэтому добавляю к городам файл который я нашел в интеренете.
    city_with_tz = city.join(timezone, city.city==timezone.t_city, 'left').drop('t_city')
    
#     Определяю город и временную зону, где было отправлено сообщение.
    messages_with_tz = add_city_in_events(messages, city_with_tz)
#     Выбираем необходимые нам данные
    messages_with_tz = messages_with_tz.select(F.col('event.message_from').alias('from'),
                                               F.col('event.message_to').alias('to'),
                                               F.coalesce(F.col('event.message_ts'), F.col('event.datetime')).alias('ts'),
                                               F.col('lat'),
                                               F.col('lon'),
                                               F.col('date'),
                                               F.col('city_id'),
                                               F.col('city_lat'),
                                               F.col('city_lon'),
                                               F.col('timezone')
                                              )


#     Определяю какие пользователи на какие каналы подписаны
    subscriptions = subscriptions.select(F.col('event.subscription_channel'), 
                                                F.col('event.user'))

#     Определяю какие пользователи могли бы написать пользователям при условии, если они подписаны на один канал
    subscriptions = subscriptions.join(subscriptions.select(F.col("user").alias("user_right"), 
                                                            "subscription_channel"), 
                                       "subscription_channel"
                                      )
#     Определяю пользователей которые общались
    users_connect = messages_with_tz.filter((F.col('from').isNotNull()) & (F.col('to').isNotNull()))\
        .select(
            F.col("from").alias("user"),
            F.col("to").alias("user_right"),
        ).distinct()\
        
    users_connect = users_connect.union(
        users_connect.select(F.col("user").alias("user_right"), 
                             F.col("user_right").alias("user"))
    ).distinct()
    
#     Определяю местоположение пользователей (откуда было отправлено последнее сообщение)
    window_place = Window().partitionBy('from').orderBy(F.desc('ts'))
    location_users = messages_with_tz.withColumn('rn', F.row_number().over(window_place))\
                        .filter(F.col('rn')==1)   
    
#     Добавляю локальное время
    location_with_time = location_users.filter(F.col('timezone').isNotNull())\
                        .withColumn('time_utc', F.date_format(F.col('ts'), "HH:mm:ss"))\
                        .withColumn('local_time', F.from_utc_timestamp(F.col("time_utc"),
                                                           F.col('timezone')))\
                        .select(F.col('from').alias('user'), F.col('local_time'))
    
#     Находим пользователей, расстояние между которыми меньше 1 км.
    messages_left = messages_with_tz.select(
                        F.col('from').alias('user'),
                        F.col('lat'), 
                        F.col('lon'),
                        F.col('date'),
                        F.col('city_id')
                        )
    messages_right = messages_with_tz.select(
                        F.col('from').alias('user_right'),
                        F.col('lat').alias('lat_right'), 
                        F.col('lon').alias('lon_right'),
                        F.col('date'),
                        F.col('city_id').alias('city_id_right')
                        )
    minimum_distance = messages_left.join(messages_right, 'date')\
            .withColumn('distance_users', F.lit(2) 
                   * F.lit(6371) 
                   * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat'))
                                               - F.radians(F.col('lat_right')))/F.lit(2)),2)
                                   + (F.cos(F.radians(F.col('lat_right')))
                                      * F.cos(F.radians(F.col('lat')))
                                      * F.pow(F.sin((F.radians(F.col('lon'))
                                                   - F.radians(F.col('lon_right')))/F.lit(2)),2)))))\
            .filter((F.col('distance_users')<=1) & (F.col('user')<F.col('user_right')))
    
# Выводим результат
    
    df = minimum_distance\
            .join(subscriptions, on=["user", "user_right"], how="leftsemi")\
            .join(users_connect, on=["user", "user_right"], how="leftsemi")\
            .join(location_with_time, on="user", how="left")\
            .withColumn('processed_dttm', F.current_timestamp())\
            .select(
                F.col("local_time"),
                F.col("user").alias("user_left"),
                "user_right",
                F.col("processed_dttm"),
                F.col("city_id").alias("zone_id"),
            )
                    
    
    df.write.mode("overwrite").parquet(f"{output_path}/date={date}")
    


    

if __name__ == '__main__':
    main()