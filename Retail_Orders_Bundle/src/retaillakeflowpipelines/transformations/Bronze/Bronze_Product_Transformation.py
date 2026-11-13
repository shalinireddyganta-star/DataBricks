from pyspark import pipelines as dp
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)

w = WorkspaceClient()

# Provide Lakeflow Pipeline Name
pipeline_name = "RetailLakeFlowPipelines"

# Find pipeline by name (matches deployed pipeline names that carry a
# bundle prefix, e.g. "[dev <user>] RetailLakeFlowPipelines")
pipelines = w.pipelines.list_pipelines()

pipeline = next(
    (p for p in pipelines if pipeline_name in p.name),
    None
)

PipelineID = pipeline.pipeline_id
PipelineName = pipeline.name
#Bronzecatalog = spark.conf.get("Bronzecatalog")
#Bronzeschema = spark.conf.get("Bronzeschema")
#PipelineID = spark.conf.get("spark.config.PipelineID", "7c4db94a-f5d3-460c-8e6c-bd5b09dd84e1")
#PipelineID =   pipeline.pipeline_id
#PipelineName = spark.conf.get("spark.config.pipeline.name", "RetailLakeFlowPipelines")
#ipelineName = spark.conf.get("PipelineName")
#source_table = spark.conf.get("source_table")
source_root_path = "/Volumes/hidi/raw/hidi/Retail/Product/"
incremental_load_path = f"{source_root_path}/*"
Bronzecatalog = "retailrawdev"
Bronzeschema = "bronze"
# Create streaming table
@dp.table(
  name=f"{Bronzecatalog}.{Bronzeschema}.Product",
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
        .withColumn("ETL_Job_ID",  lit(PipelineID))
        .withColumn("ETL_Job_Name", lit(PipelineName))
    )