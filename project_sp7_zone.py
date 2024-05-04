import os
import sys
import subprocess
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.window  import Window
import pyspark.sql.functions as F

RADIUS=6371

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

check_path = lambda x: True if subprocess.run(['hdfs', 'dfs', '-ls', x], capture_output=True, text=True).stdout else False

def input_event_paths(base_path, date, depth):
    dt = datetime.strptime(date, '%Y-%m-%d')
    result = []
    for event_type in ['message', 'reaction', 'subscription']:
        paths = []
        paths = [f"{base_path}/event_type={event_type}/date={(dt - timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]
        paths = [x for x in paths if check_path(x)]
        result.extend(paths)
    return result

def add_city_in_events(events, city):

    df = events.crossJoin(city)

    df = df.withColumn('distance', F.lit(2) 
                   * F.lit(RADIUS) 
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
        .where((F.col('distance')==F.col('min_distance')) | (F.col('distance').isNull()))\
        .select(
            F.col('event'),
            F.col('lat'),
            F.col('lon'),
            F.col('city_id'),
            F.col('city'),
            F.col('city_lat'),
            F.col('city_lon'),
            F.col('event_type'),
            F.col('date')
        )

def main():
    
    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("Leaning") \
                    .getOrCreate()
    
    
    base_path = sys.argv[1]
    date = sys.argv[2]
    depth = int(sys.argv[3])
    geo_path = sys.argv[4]
    output_path = sys.argv[5]
    
    
    events = spark\
            .read\
            .option("basePath", base_path)\
            .parquet(*input_event_paths(base_path, date, depth))\
            .sample(0.3)
    
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
    
    df_event_city = add_city_in_events(events, city)
    
    window_week = Window().partitionBy('week')
    window_month = Window().partitionBy('month')
    window_week_tz = Window().partitionBy('week', 'city_id')
    window_month_tz = Window().partitionBy('month', 'city_id')
    window = Window().partitionBy('event.message_from').orderBy(F.col('date'))
    
    df = df_event_city \
        .withColumn("month",F.trunc(F.col("date"), "month"))\
        .withColumn("week",F.trunc(F.col("date"), "week"))\
        .withColumn("rn",F.row_number().over(window))\
        .withColumn("week_message",F.sum(F.when(df_event_city.event_type == "message",1).otherwise(0)).over(window_week_tz))\
        .withColumn("week_reaction", F.sum(F.when(df_event_city.event_type=="reaction", 1).otherwise(0)).over(window_week))\
        .withColumn("week_subscription", F.sum(F.when(df_event_city.event_type=="subscription", 1).otherwise(0)).over(window_week))\
        .withColumn("week_user", F.sum(F.when(F.col("rn")==1, 1).otherwise(0)).over(window_week_tz))\
        .withColumn("month_message",F.sum(F.when(df_event_city.event_type == "message",1).otherwise(0)).over(window_month_tz))\
        .withColumn("month_reaction", F.sum(F.when(df_event_city.event_type=="reaction", 1).otherwise(0)).over(window_month))\
        .withColumn("month_subscription", F.sum(F.when(df_event_city.event_type=="subscription", 1).otherwise(0)).over(window_month))\
        .withColumn("month_user", F.sum(F.when(F.col("rn")==1, 1).otherwise(0)).over(window_month_tz))\
        .select(F.col("month"),
               F.col("week"),
               F.col("city_id").alias("zone_id"),
               F.col("week_message"),
               F.col("week_reaction"),
               F.col("week_subscription"),
               F.col("week_user"),
               F.col("month_message"),
               F.col("month_reaction"),
               F.col("month_subscription"),
               F.col("month_user"))\
        .distinct()
    
    df.write.mode("overwrite").parquet(f"{output_path}/date={date}")
    

if __name__ == '__main__':
    main()