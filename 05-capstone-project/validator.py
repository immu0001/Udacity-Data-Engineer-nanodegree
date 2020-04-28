from pyspark.sql.functions import *


class Validator:
    """
    Validate and checks the model and data.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def _get_demographics(self):
        """
        Get demographics dimension
        :return: demographics dimension
        """
        return self.spark.read.parquet(self.paths["demographics"])

    def _get_airports(self):
        """
        Get airports dimension
        :return: airports dimension
        """
        return self.spark.read.parquet(self.paths["airports"])

    def _get_airlines(self):
        """
        Get airlines dimension
        :return: airlines dimension
        """
        return self.spark.read.parquet(self.paths["airlines"])

    def _get_countries(self):
        """
        Get countries dimension
        :return: countries dimension
        """
        return self.spark.read.parquet(self.paths["countries"])

    def _get_visa(self):
        """
        Get visa dimension
        :return: visa dimension
        """
        return self.spark.read.parquet(self.paths["visa"])

    def _get_mode(self):
        """
        Get mode dimension
        :return: mode dimension
        """
        return self.spark.read.parquet(self.paths["mode"])

    def get_facts(self):
        """
        Get facts table
        :return: facts table
        """
        return self.spark.read.parquet(self.paths["facts"])

    def get_dimensions(self):
        """
        Get all dimensions of the model
        :return: all dimensions
        """
        return self._get_demographics(), self._get_airports(), self._get_airlines() \
            , self._get_countries(), self._get_visa(), self._get_mode()

    def exists_rows(self, dataframe):
        """
        Checks if there is any data in a dataframe
        :param dataframe: dataframe
        :return: true or false if the dataset has any row
        """
        return dataframe.count() > 0

    def check_integrity(self, fact, dim_demographics, dim_airports, dim_airlines, dim_countries, dim_visa, dim_mode):
        """
        Check the integrity of the model. Checks if all the facts columns joined with the dimensions has correct values 
        :param fact: fact table
        :param dim_demographics: demographics dimension
        :param dim_airports: airports dimension
        :param dim_airlines: airlines dimension
        :param dim_countries: countries dimension
        :param dim_visa: visa dimension
        :param dim_mode: mode dimension
        :return: true or false if integrity is correct.
        """
        integrity_demo = fact.select(col("cod_state")).distinct() \
                             .join(dim_demographics, fact["cod_state"] == dim_demographics["State_Code"], "left_anti") \
                             .count() == 0

        integrity_airports = fact.select(col("cod_port")).distinct() \
                                 .join(dim_airports, fact["cod_port"] == dim_airports["local_code"], "left_anti") \
                                 .count() == 0

        integrity_airlines = fact.select(col("airline")).distinct() \
                                 .join(dim_airlines, fact["airline"] == dim_airlines["IATA"], "left_anti") \
                                 .count() == 0

        integrity_countries = fact.select(col("cod_country_origin")).distinct() \
                                  .join(dim_countries, fact["cod_country_origin"] == dim_countries["cod_country"],
                                        "left_anti") \
                                  .count() == 0

        integrity_visa = fact.select(col("cod_visa")).distinct() \
                             .join(dim_visa, fact["cod_visa"] == dim_visa["cod_visa"], "left_anti") \
                             .count() == 0

        integrity_mode = fact.select(col("cod_mode")).distinct() \
                             .join(dim_mode, fact["cod_mode"] == dim_mode["cod_mode"], "left_anti") \
                             .count() == 0

        return integrity_demo & integrity_airports & integrity_airlines & integrity_countries\
               & integrity_visa & integrity_mode