import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEY']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEY']['AWS_SECRET_ACCESS_KEY']


def timer(func):
    """
    Print the runtime of the decorated function
    :param func: function that we want to be timed
    :return: value of function, but prints string of how long function ran
    """
    import functools
    import time
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.time()
        value = func(*args, **kwargs)
        end_time = time.time()
        run_time = end_time - start_time
        if run_time > 3659:
            hours = int(run_time/3600)
            print(f"Finished {func.__name__} in {hours:.0f} hours, {(run_time-hours*3600)/60:.0f} minutes and {run_time%60:.0f} secs")
        elif run_time > 59:
            print(f"Finished {func.__name__} in {run_time/60:.0f} minutes and {run_time%60:.0f} secs")
        else:
            print(f"Finished {func.__name__} in {run_time:.2f} secs")
        return value

    return wrapper_timer


def create_spark_session():
    """
    Creates the session we use to interact with spark
    :return: the session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.getenv('AWS_ACCESS_KEY_ID'))
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.getenv('AWS_SECRET_ACCESS_KEY'))
    
    return spark


@timer
def process_song_data(spark, input_data, output_data):
    """
    Reads the json song data and creates/saves the songs and artist tables
    :param spark: the current spark session
    :param input_data: STR path where we pull raw json files
    :param output_data: STR path where we save the parquets
    :return:
    """
    # get filepath to song data file
    song_data = 'song_data/A/A/C/*.json'
    
    # read song data file and create schema
    song_schema = StructType([
        StructField('num_songs', IntegerType()),
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', DoubleType()),
        StructField('year', IntegerType())
        ])
    print('Reading song data')
    df = spark.read.schema(song_schema).json(os.path.join(input_data,song_data))

    # extract columns to create songs table
    songs_table = df.select(df.song_id, df.title, df.artist_id, df.year, df.duration).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print('Writing song table')
    songs_table.write.parquet(os.path.join(output_data,"songs/"), mode="overwrite", partitionBy=["year","artist_id"])

    
    # extract columns to create artists table
    artists_table = df.select(df.artist_id, 
                              df.artist_name.alias('name'), 
                              df.artist_location.alias('location'), 
                              df.artist_latitude.alias('latitude'), 
                              df.artist_longitude.alias('longitude')).dropDuplicates()
    
    # write artists table to parquet files
    print('Writing artist table')
    artists_table.write.parquet(os.path.join(output_data,"artists/"), mode="overwrite")
    
    return
    

@timer
def process_log_data(spark, input_data, output_data):
    """
    Reads the json log data and creates/saves the users, time and songplay tables
    :param spark: the current spark session
    :param input_data: STR path where we pull raw json files
    :param output_data: STR path where we save the parquets
    :return:
    """
    # get filepath to log data file
    log_data = 'log_data/*/*/*.json'

    # read log data file
    log_schema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('itemInSession', LongType()),
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', DoubleType()),
        StructField('sessionId', LongType()),
        StructField('song', StringType()),
        StructField('status', LongType()),
        StructField('ts', LongType()),
        StructField('userAgent', StringType()),
        StructField('userId', StringType())
        ])
    print('Reading log data')
    df = spark.read.schema(log_schema).json(os.path.join(input_data, log_data)).dropDuplicates()
    #df = spark.read.json(os.path.join(input_data, log_data))

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    
    # extract columns for users table  
    users_table = df.select(
        df.userId.alias('user_id'),
        df.firstName.alias('first_name'),
        df.lastName.alias('last_name'),
        df.gender,
        df.level
    ).dropDuplicates()
    
    # write users table to parquet files
    print('Writing user table')
    users_table.write.parquet(os.path.join(output_data,"users/"), mode="overwrite")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    
    # extract columns to create time table
    time_table = df.select(
        df.start_time,
        hour(df.start_time).alias("hour"),
        dayofmonth(df.start_time).alias("day"),
        weekofyear(df.start_time).alias("week"),
        month(df.start_time).alias("month"),
        year(df.start_time).alias("year"),
        date_format(df.start_time, "E").alias("weekday")
                ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    print('Writing time table')
    time_table.write.parquet(os.path.join(output_data,"time/"), mode="overwrite", partitionBy=["year","month"])
    
    # read in song data to use for songplays table
    print('Reading back in song table')
    song_df = spark.read.format("parquet").load(os.path.join(output_data,"songs/"))
    
    # extract columns from joined song and log datasets to create songplays table
    columns = [monotonically_increasing_id().alias('songplay_id'), df.start_time,
               df.userId.alias('user_id'), df.level, song_df.song_id, song_df.artist_id,
               df.sessionId.alias('session_id'), df.location, df.userAgent.alias('user_agent')]
    condish = [df.song == song_df.title, df.length == song_df.duration]
    songplays_table = df.join(song_df, condish, 'inner').select(columns).distinct()
    
    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays table')
    songplays_table.write.parquet(os.path.join(output_data,"songplays/"), mode="overwrite", partitionBy=["year","month"])
    
    return


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-de-tamayo/spark-parquet/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    print('Complete')
    
    return


if __name__ == "__main__":
    main()
