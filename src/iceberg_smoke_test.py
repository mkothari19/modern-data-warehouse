from pyspark.sql import SparkSession


ICEBERG_JAR = (
    "/Users/mac/Documents/modern-data-warehouse/"
    "jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar"
)

spark = (
    SparkSession.builder
    .appName("ModernDataWarehouse-Iceberg-SmokeTest")
    .config("spark.jars", ICEBERG_JAR)
    .config(
        "spark.sql.catalog.iceberg",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.iceberg.type",
        "rest",
    )
    .config(
        "spark.sql.catalog.iceberg.uri",
        "http://localhost:8181",
    )
    .getOrCreate()
)

print("\n=== Spark ===")
print("Spark version:", spark.version)

print("\n=== Iceberg Namespaces ===")
spark.sql("SHOW NAMESPACES IN iceberg").show(truncate=False)

print("\n=== SUCCESS ===")

spark.stop()