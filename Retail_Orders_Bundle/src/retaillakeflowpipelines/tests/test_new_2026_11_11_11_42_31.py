from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)

bronze_catalog = spark.conf.get("Bronzecatalog","retailraw")
print(bronze_catalog)