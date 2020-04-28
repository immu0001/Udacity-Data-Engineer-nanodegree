from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType

from pyspark.sql.functions import *


class Modelizer:
    """
    Modelizes the datawarehouse (star schema) from datasets. Creating the facts table and dimension tables.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def _modelize_demographics(self, demographics):
        """
        Create de demographics dimension table in parquet.
        :param demographics: demographics dataset.
        """
        demographics.write.mode('overwrite').parquet(self.paths["demographics"])

    def _modelize_airports(self, airports):
        """
        Create de airports dimension table in parquet.
        :param airports: airports dataset
        """
        airports.write.mode('overwrite').parquet(self.paths["airports"])

    def _modelize_airlines(self, airlines):
        """
        Create de airlines dimension table in parquet.
        :param airlines: airlines dataset
        """
        airlines.write.mode('overwrite').parquet(self.paths["airlines"])

    def _modelize_countries(self, countries):
        """
        Create countries dimension table in parquet
        :param countries: countries dataset
        """
        countries.write.mode('overwrite').parquet(self.paths["countries"])

    def _modelize_visa(self, visa):
        """
        Create visa dimension table in parquet
        :param visa: visa dataset
        """
        visa.write.mode('overwrite').parquet(self.paths["visa"])

    def _modelize_mode(self, mode):
        """
        Create modes dimension table in parquet
        :param mode: modes dataset
        """
        mode.write.mode('overwrite').parquet(self.paths["mode"])

    def _modelize_facts(self, facts):
        """
        Create facts table from inmigration in parquet particioned by arrival_year, arrival_month and arrival_day
        :param facts: inmigration dataset
        """
        facts.write.partitionBy("arrival_year", "arrival_month", "arrival_day").mode('overwrite').parquet(
            self.paths["facts"])

    def modelize(self, facts, dim_demographics, dim_airports, dim_airlines, dim_countries, dim_visa, dim_mode):
        """
        Create the Star Schema for the Data Warwhouse
        :param facts: facts table, inmigration dataset
        :param dim_demographics: dimension demographics
        :param dim_airports: dimension airports
        :param dim_airlines: dimension airlines
        :param dim_countries: dimension countries
        :param dim_visa: dimension visa
        :param dim_mode: dimension mode
        """
        facts = facts \
            .join(dim_demographics, facts["cod_state"] == dim_demographics["State_Code"], "left_semi") \
            .join(dim_airports, facts["cod_port"] == dim_airports["local_code"], "left_semi") \
            .join(dim_airlines, facts["airline"] == dim_airlines["IATA"], "left_semi") \
            .join(dim_countries, facts["cod_country_origin"] == dim_countries["cod_country"], "left_semi") \
            .join(dim_visa, facts["cod_visa"] == dim_visa["cod_visa"], "left_semi") \
            .join(dim_mode, facts["cod_mode"] == dim_mode["cod_mode"], "left_semi")

        self._modelize_demographics(dim_demographics)
        self._modelize_airports(dim_airports)
        self._modelize_airlines(dim_airlines)
        self._modelize_countries(dim_countries)
        self._modelize_visa(dim_visa)
        self._modelize_mode(dim_mode)

        self._modelize_facts(facts)