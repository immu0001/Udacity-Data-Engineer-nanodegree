from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Source:
    """
    Get the sources and return dataframes
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def _get_standard_csv(self, filepath, delimiter=","):
        """
        Get sources in CSV format
        :param filepath: csv file path
        :param delimiter: delimiter
        :return: dataframe
        """
        return self.spark.read.format("csv").option("header", "true").option("delimiter", delimiter).load(filepath)

    def get_cities_demographics_raw(self):
        """
        Get demographics dataset
        :return: demographics dataset
        """
        return self._get_standard_csv(filepath=self.paths["demographics"], delimiter=";")

    def get_airports_raw(self):
        """
        Get airports dataset
        :return: airports dataset
        """
        return self._get_standard_csv(self.paths["airports"])

    def get_inmigration_raw(self):
        """
        Get inmigration dataset.
        :return: inmigration dataset
        """
        return self.spark.read.parquet(self.paths["sas_data"])

    def get_countries_raw(self):
        """
        Get countries dataset
        :return: countries dataset
        """
        return self.spark.read.json(self.paths["countries"], multiLine=True)

    def get_visa_raw(self):
        """
        Get visa dataset
        :return: visa dataset
        """
        return self._get_standard_csv(self.paths["visa"])

    def get_mode_raw(self):
        """
        Get modes dataset
        :return: modes dataset
        """
        return self._get_standard_csv(self.paths["mode"])

    def get_airlines(self):
        """
        Get airlines dataset
        :return: airlines dataset
        """
        schema = StructType([
            StructField("Airline_ID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Alias", StringType(), True),
            StructField("IATA", StringType(), True),
            StructField("ICAO", StringType(), True),
            StructField("Callsign", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Active", StringType(), True)])

        return self.spark.read.csv(self.paths["airlines"], header=False, schema=schema)
