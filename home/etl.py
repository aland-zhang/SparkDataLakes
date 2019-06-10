import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col, to_timestamp, unix_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    song_data = input_data + 'song_data/*/*/*/*'
    df = spark.read.json(song_data)
    
    songs_table = df.select('song_id','title','artist_id','year', 'duration').dropDuplicates()
    
    try:
        songs_table.write.partitionBy("year", 'artist_id').parquet(output_data + 'songs/songs.parquet')
    except Exception:
        print(repr(Exception))

    artists_table =  df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates()
    
    try:
        artists_table.write.parquet(output_data + 'artists/artists.parquet')
    except Exception:
        print(repr(Exception))


def process_log_data(spark, input_data, output_data):
    log_data = input_data + 'log_data/*'

    df = spark.read.json(log_data)
    
    df = df.filter('page in ("NextSong")')

    users_table = df.select('userId','firstName','lastName','gender','level')
    
    try:
        users_table.write.parquet(output_data + 'users/users.parquet')
    except Exception:
        print(repr(Exception))

    def get_timestamp(ts):
        import time
        return time.ctime(ts//1000)
    get_timestamp_udf = udf(get_timestamp)
    df = df.withColumn('timestamp', get_timestamp_udf('ts'))
    
    
    def get_datetime(ts):
        import datetime
        return datetime.datetime.fromtimestamp(ts//1000)
    get_datetime_udf = udf(get_datetime)
    df = df.withColumn('datetime', get_timestamp_udf('ts')).dropDuplicates()
    
        
    def get_time_vars(t):
        t = datetime.strptime(str(t.datetime), '%c')
        return Row(start_time=t.time().strftime("%H:%M:%S"), hour=t.hour, day=t.day, week=int(t.strftime("%V")), month=t.month, year=t.year, weekday=t.weekday())
    time_table = df.select('datetime').rdd.map(lambda t: get_time_vars(t))
    time_table = time_table.toDF().dropDuplicates()
    
    try:
        time_table.write.partitionBy("year", 'month').parquet(output_data + 'time/time.parquet')
    except Exception:
        print(repr(Exception))

    song_df = spark.read.parquet(output_data + 'songs/songs.parquet')
    song_df.toDF('song_id', 'title', 'artist_id', 'year', 'duration')
    song_df.createTempView('songs')
    df.createTempView('logs')

    songplays_table = spark.sql('SELECT logs.ts as start_time, logs.userId as user_id, logs.level, songs.song_id, songs.artist_id, logs.sessionId as session_id, logs.location, logs.userAgent as user_agent FROM logs JOIN songs ON logs.length = songs.duration')
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    try:
        songplays_table.write.parquet(output_data + 'songplays/songplays.parquet')
    except Exception:
        print(repr(Exception))

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = './data/'
    output_data = "s3a://analytics-sparkify/"
    #output_data = './'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
