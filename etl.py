import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function create a spark session
    """
    spark = SparkSession.builder \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function extract data from song_data dataset and extract columns and write data into parquet files that are loaded in S3
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration')\
                         .dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.patitionedBy('year','artist_id')\
                     .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table =  song_df.select('artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude') \
                            .withColumnRenamed('artist_name', 'name') \
                            .withColumnRenamed('artist_location', 'location') \
                            .withColumnRenamed('artist_latitude', 'latitude') \
                            .withColumnRenamed('artist_longitude', 'longitude') \
                            .dropDuplicates(['artist_id'])
    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This function extract data from log_data dataset and extract columns and write data into parquet files that are loaded in S3
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong') \
                   .select('ts', 'userId', 'level', 'song', 'artist','sessionId', 'location', 'userAgent')

    # extract columns for users table    
    user_table = log_df.select('userId', 'first_name', 'last_name', 'gender', 'level').drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))
    
    # extract columns to create time table
    time_table =  log_df.select('datetime') \
                        .withColumn('start_time', log_df.datetime) \
                        .withColumn('hour', hour('datetime')) \
                        .withColumn('day', dayofmonth('datetime')) \
                        .withColumn('week', weekofyear('datetime')) \
                        .withColumn('month', month('datetime')) \
                        .withColumn('year', year('datetime')) \
                        .withColumn('weekday', dayofweek('datetime')) \
                        .dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, col('log_df.artist') == col('song_df.artist_name'), 'inner')
    songplays_table = songplays_table.select(col('log_df.datetime').alias('start_time'),
                                             col('log_df.userId').alias('userId'),
                                             col('log_df.level').alias('level'),
                                             col('song_df.song_id').alias('song_id'),
                                             col('song_df.artist_id').alias('artist_id'),
                                             col('log_df.sessionId').alias('session_id'),
                                             col('log_df.location').alias('location'), 
                                             col('log_df.userAgent').alias('user_agent'),
                                             year('log_df.datetime').alias('year'),
                                             month('log_df.datetime').alias('month')) \
                                             .withColumn('songplay_id', monotonically_increasing_id())\
                                             .dropDuplicates()
    

    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_d.dropDuplicates()
    ata,'songplays/songplays.parquet'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
