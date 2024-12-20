from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, translate, trim
from datetime import datetime
from config.config import FILE_FORMAT, TIMEZONE
from include.datalake.datalake_factory import get_datalake
import unicodedata
import pytz
import sys

class SilverTransformDataFromBronze:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE) #Adjust to your timezone
        self.datalake = get_datalake()
        self.spark = SparkSession.builder.appName("SilverLayer").getOrCreate()
    

    # Remove punctuation and normalize special characters in a DataFrame column.
    def normalize_and_clean_text(self, column):
        special_chars = ""
        normalized_chars = ""

        for i in range(ord(" "), sys.maxunicode):
            name = unicodedata.name(chr(i), "")
            if "WITH" in name:
                try:
                    base = unicodedata.lookup(name.split(" WITH")[0])
                    special_chars += chr(i)
                    normalized_chars += base
                except KeyError:
                    pass

        return translate(
            regexp_replace(column, "\p{Pc}", ""), 
            special_chars, normalized_chars
        ).alias(column)



    # Transform breweries data from bronze layer and persist into the datalake silver layer
    def transform_and_persist_breweries_data(self, **kwargs):
        execution_date = kwargs.get('execution_date')
        if execution_date:
            execution_date = execution_date.astimezone(self.timezone).strftime('%Y-%m-%d')
        else:
            execution_date = datetime.now().strftime('%Y-%m-%d')
        bronze_df = self.datalake.read_files(layer="bronze", execution_date=execution_date, format=FILE_FORMAT)

        selected_columns = ['id', 'name', 'brewery_type', 'country',
                             'state', 'city', 'longitude', 'latitude']
        
        bronze_transformation_df = bronze_df.select(*selected_columns)

        bronze_transformation_df = bronze_transformation_df.withColumn(colName='city', col=regexp_replace(lower(col=trim(col('city').cast('string'))), ' ', '-')) \
                                                        .withColumn(colName='state', col=regexp_replace(lower(col=trim(col('state').cast('string'))), ' ', '-')) \
                                                        .withColumn(colName='country', col=regexp_replace(lower(col=trim(col('country').cast('string'))), ' ', '-'))
        
        bronze_transformation_df.show(10)

        bronze_transformation_df = bronze_transformation_df.withColumn('name', regexp_replace('name', 'Anheuser-Busch Inc ̢���� Williamsburg',
                                                                                    'Anheuser-Busch/Inbev Williamsburg Brewery'))
        bronze_transformation_df = bronze_transformation_df.withColumn('name', regexp_replace('name', 'Wimitzbr�u','Wimitzbrau'))
        bronze_transformation_df = bronze_transformation_df.withColumn('name', regexp_replace('name', 'Caf� Okei','Cafe Okei'))
        bronze_transformation_df = bronze_transformation_df.withColumn('name', regexp_replace('name', 'â','-'))
        bronze_transformation_df = bronze_transformation_df.withColumn('city', regexp_replace('city', 'klagenfurt-am-w�rthersee',
                                                                                    'klagenfurt-am-worthersee'))
        bronze_transformation_df = bronze_transformation_df.withColumn('state', regexp_replace('state', 'nieder�sterreich','niederosterreich'))
        bronze_transformation_df = bronze_transformation_df.withColumn('state', regexp_replace('state', 'k�rnten','karnten'))

        bronze_transformation_df = bronze_transformation_df.withColumn('city', self.normalize_and_clean_text('city')) \
                                                        .withColumn('state', self.normalize_and_clean_text('state')) \
                                                        .withColumn('country', self.normalize_and_clean_text('country'))
        
        bronze_transformation_df.show(10)

        self.datalake.write_file(df=bronze_transformation_df, layer="silver", execution_date=execution_date, format=FILE_FORMAT, partitions=['country', 'state'])