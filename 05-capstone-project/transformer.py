from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType


class Transformer:
    """
    Realize transformations necessaries in order to create the model.
    """

    @staticmethod
    def transform_demographics(demographics):
        """
        Transform demographics dataset grouping by state an calculate all the totals and ratios for every race
         in every state.
        :param demographics: demographics dataset
        :return: demographics dataset transformed
        """
        demo = demographics \
            .groupBy(col("State Code").alias("State_code"), col("State")).agg(
            sum("Total Population").alias("Total_Population")\
            , sum("Male Population").alias("Male_Population"), sum("Female Population").alias("Female_Population")\
            , sum("American Indian and Alaska Native").alias("American_Indian_and_Alaska_Native")\
            , sum("Asian").alias("Asian"), sum("Black or African-American").alias("Black_or_African-American")\
            , sum("Hispanic or Latino").alias("Hispanic_or_Latino")\
            , sum("White").alias("White")) \
            .withColumn("Male_Population_Ratio", round(col("Male_Population") / col("Total_Population"), 2))\
            .withColumn("Female_Population_Ratio", round(col("Female_Population") / col("Total_Population"), 2))\
            .withColumn("American_Indian_and_Alaska_Native_Ratio",
                        round(col("American_Indian_and_Alaska_Native") / col("Total_Population"), 2))\
            .withColumn("Asian_Ratio", round(col("Asian") / col("Total_Population"), 2))\
            .withColumn("Black_or_African-American_Ratio",
                        round(col("Black_or_African-American") / col("Total_Population"), 2))\
            .withColumn("Hispanic_or_Latino_Ratio", round(col("Hispanic_or_Latino") / col("Total_Population"), 2))\
            .withColumn("White_Ratio", round(col("White") / col("Total_Population"), 2))

        return demo

    @staticmethod
    def transform_inmigrants(inmigrants):
        """
        Transform inmigration dataset on order to get arrival date in different columns (year, month, day) 
        for partitioning the dataset.
        :param inmigrants: inmigration dataset
        :return: inmigration dataset transformed
        """
        inmigrants = inmigrants \
            .withColumn("arrival_date-split", split(col("arrival_date"), "-")) \
            .withColumn("arrival_year", col("arrival_date-split")[0]) \
            .withColumn("arrival_month", col("arrival_date-split")[1]) \
            .withColumn("arrival_day", col("arrival_date-split")[2]) \
            .drop("arrival_date-split")

        return inmigrants