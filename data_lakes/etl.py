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
    '''Create SparkSession for programming using Spark
    Parameters
    ----------

    Returns
    -------
    spark (SparkSession) : session for Spark DataFrames
    '''

    spark = SparkSession.builder
    spark = spark.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    )
    spark = spark.getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """ Process song data files into parquet files using Spark.

    Parameters
    ----------
    spark (SparkSession) : Spark session for processing files
    input_data (str) : path to input data to process
    output_data (str) : path to write parquet files

    Returns
    -------
    None
    """

    # get filepath to song data file
    song_data = None
    
    # read song data file
    df = None

    # extract columns to create songs table
    songs_table = None
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = None

    # extract columns to create artists table
    artists_table = None
    
    # write artists table to parquet files
    artists_table = None


def process_log_data(spark, input_data, output_data):
    """ Process log data files into parquet files using Spark.

    Parameters
    ----------
    spark (SparkSession) : Spark session for processing files
    input_data (str) : path to input data to process
    output_data (str) : path to write parquet files

    Returns
    -------
    None
    """

    # get filepath to log data file
    log_data = None

    # read log data file
    df = None
    
    # filter by actions for song plays
    df = None

    # extract columns for users table    
    artists_table = None
    
    # write users table to parquet files
    artists_table = None

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = None
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = None
    
    # extract columns to create time table
    time_table = None
    
    # write time table to parquet files partitioned by year and month
    time_table = None

    # read in song data to use for songplays table
    song_df = None

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = None

    # write songplays table to parquet files partitioned by year and month
    songplays_table = None


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "/output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
