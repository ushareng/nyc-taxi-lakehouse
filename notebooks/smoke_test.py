import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

bucket = os.environ["BUCKET"]
builder = (
    SparkSession.builder.appName("smoke_test")
    .config("spark.jars", "jars/gcs-connector-hadoop3-latest.jar")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = f"gs://{bucket}/bronze/_smoke_test"
spark.range(5).write.format("delta").mode("overwrite").save(path)
print("Read back rows:", spark.read.format("delta").load(path).count())
spark.stop()
