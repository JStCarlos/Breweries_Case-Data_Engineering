import requests
from include.datalake.datalake_factory import get_datalake
from datetime import datetime
from pyspark.sql import SparkSession
import pytz
from config.config import BREWERY_API_URL, FILE_FORMAT, TIMEZONE
from pyspark.sql.types import StructType, StructField, StringType

class BonzeFetchDataBreweriesApi:
    def __init__(self):
        self.api_url = BREWERY_API_URL
        self.timezone = pytz.timezone(TIMEZONE) #Adjust to your timezone
        self.per_page = 200
        self.datalake = get_datalake()
        self.spark = SparkSession.builder.appName("BreweriesAPI").getOrCreate()

    # Get metadata about the total number of breweries to evaluate the number of pages to fetch
    def get_total_breweries_metadata(self):
        metadata = requests.get(self.api_url + '/meta')
        metadata.raise_for_status()
        total_breweries = int(metadata.json()['total'])
        print(f"Total breweries: {total_breweries}")
        pages_to_fetch = total_breweries // self.per_page + (total_breweries % self.per_page > 0)
        return pages_to_fetch
        


    # Fetch breweries data from the breweries API and persist into the datalake
    def fetch_and_persist_breweries_data(self, pages_to_fetch, layer="bronze", **kwargs):
        execution_date = kwargs.get('execution_date')

        if execution_date:
            execution_date = execution_date.astimezone(self.timezone).strftime('%Y-%m-%d')
        else:
            execution_date = datetime.now().strftime('%Y-%m-%d')

        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("state", StringType(), True),
            StructField("street", StringType(), True),
        ])

        page = 1       
        print(f"Pages to fetch: {pages_to_fetch}")

        while page <= pages_to_fetch:
            url = self.api_url + f"?page={page}&per_page={self.per_page}"
            response = requests.get(url)
            response.raise_for_status()
            breweries_data = response.json()

            df_spark = self.spark.createDataFrame(breweries_data, schema=schema)
            filename = f"breweries_page_{page}"

            self.datalake.write_file(df_spark, layer, execution_date, filename, FILE_FORMAT)

            print(f"Page {page} fetched and saved as {FILE_FORMAT} in {layer} layer")
            page += 1

        print(f"Total pages fetched: {pages_to_fetch}")