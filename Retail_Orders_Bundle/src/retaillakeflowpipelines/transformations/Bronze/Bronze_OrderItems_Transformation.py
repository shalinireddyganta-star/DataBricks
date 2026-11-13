from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)

job_id: str = "101"
job_name: str = "RetailLakeFlowPipelines"

source_root_path = "/Volumes/hidi/raw/hidi/Retail/Orders/"
incremental_load_path = f"{source_root_path}/*"

# Resolve catalog/schema from pipeline configuration
#Bronzecatalog = spark.conf.get("Bronzecatalog")
#Bronzeschema = spark.conf.get("Bronzeschema")
Bronzecatalog = "retailrawdev"
Bronzeschema = "bronze"
# Create streaming table
@dp.table(
    name=f"{Bronzecatalog}.{Bronzeschema}.RawOrderItems",
    comment="Raw stagingorderitems",
      table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "parquet",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "overwriteMode" : "true"
    }
)
def ingest():

    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("modifiedAfter", "2025-01-01T00:00:00.000+00:00")
        .load(incremental_load_path)

        # Custom metadata columns
        .withColumn("RawIngestionTimestamp", current_timestamp())
        .withColumn("RawSourceFileName", col("_metadata.file_path"))
        .withColumn("ETL_Job_ID", lit(job_id))
        .withColumn("ETL_Job_Name", lit(job_name))
    )