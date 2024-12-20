from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from datetime import datetime
from include.datalake.datalake_factory import get_datalake
from config.config import FILE_FORMAT, TIMEZONE
import pytz

class GoldAggregateDataFromSilver:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.datalake = get_datalake()
        self.spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

    # Aggregates the data to get the number of breweries by type and location
    def aggregate_breweries_data(self, **kwargs):
        execution_date = kwargs.get('execution_date')
        
        if execution_date:
            execution_date = execution_date.astimezone(self.timezone).strftime('%Y-%m-%d')
        else:
            execution_date = datetime.now().strftime('%Y-%m-%d')

        silver_df = self.datalake.read_files(layer="silver", execution_date=execution_date, partitioned=True)
        silver_df.show(30)

        aggregated_df = (
            silver_df
            .groupBy("brewery_type",col("country"), col("state"))
            .agg(count("*").alias("brewery_count"))
        )

        aggregated_df.createOrReplaceGlobalTempView("brewery_counts")
        spark_catalog = self.spark.catalog.listTables("global_temp")
        print("Generated aggregated view 'brewery_counts' containing brewery counts categorized by type and location (country, state).")
        print(f'Displaying the catalog in global_temp: {spark_catalog}')

        aggregated_df.show(truncate=False)

        self.datalake.write_file(
            df=aggregated_df,
            layer="gold",
            execution_date=execution_date,
            filename="aggregated_breweries_data",
            format=FILE_FORMAT,
            partitions=['country']
        )

        print(f"Aggregated data saved successfully in gold layer for execution date: {execution_date}")
