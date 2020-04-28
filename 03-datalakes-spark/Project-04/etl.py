import configparser
import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, \
    monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates the Spark Session with the necessary packages.
    :return: Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data dataset and creates the song table and artist table
    :param spark: SparkSession
    :param input_data: path/to/files to process
    :param output_data: path/to/files to write the results Datasets
    """
    # get filepath to song data file
    song_data = "{}*/*/*/*.json".format(input_data)

    # read song data file
    df = spark.read.json(song_data).dropDuplicates().cache()

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"), col("artist_id"), col("year"), col("duration")).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet("{}songs/songs_table.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name"), col("artist_location"), \
                              col("artist_latitude"), col("artist_longitude")).distinct()

    # write artists table to parquet files
    artists_table.write.parquet("{}artists/artists_table.parquet".format(output_data))

    df.createOrReplaceTempView("song_df_table")


def process_log_data(spark, input_data, output_data):
    """
    Process the log data dataset and creates the user table, time table and songsplay table
    :param spark: SparkSession
    :param input_data: path/to/files to process
    :param output_data: path/to/files to write the results Datasets
    """
    # get filepath to log data file
    log_data = "{}*/*/*events.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong").cache()

    # extract columns for users table
    user_table = df.select(col("firstName"), col("lastName"), col("gender"), col("level"), col("userId")).distinct()

    # write users table to parquet files
    user_table.write.parquet("{}users/users_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))

    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))

    time_table = df.select(col("start_time"), col("hour"), col("day"), col("week"), \
                           col("month"), col("year"), col("weekday")).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet("{}time/time_table.parquet".format(output_data))

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_df_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner") \
        .distinct() \
        .select(col("start_time"), col("userId"), col("level"), col("sessionId"), \
                col("location"), col("userAgent"), col("song_id"), col("artist_id")) \
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month") \
        .parquet("{}songplays/songplays_table.parquet".format(output_data))


def main():
    """
    Main Function
    """
    spark = create_spark_session()
    input_data_song = "s3://udacity-dend/song-data/"
    input_data_log = "s3: // udacity-dend/log-data/"

    output_data = "s3://aws-logs-596951707262-us-west-2/elasticmapreduce/"

    process_song_data(spark, input_data_song, output_data)
    process_log_data(spark, input_data_log, output_data)


if __name__ == "__main__":
    main()
