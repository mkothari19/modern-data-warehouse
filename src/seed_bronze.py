from pyspark.sql import SparkSession


ICEBERG_JAR = (
    "/Users/mac/Documents/modern-data-warehouse/"
    "jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar"
)

ICEBERG_AWS_BUNDLE_JAR = (
    "/Users/mac/Documents/modern-data-warehouse/"
    "jars/iceberg-aws-bundle-1.6.0.jar"
)
spark = (
    SparkSession.builder
    .appName("ModernDataWarehouse-SeedBronze")
    .config("spark.jars", f"{ICEBERG_JAR},{ICEBERG_AWS_BUNDLE_JAR}")

    .config(
        "spark.sql.catalog.warehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.warehouse.type",
        "rest"
    )
    .config(
        "spark.sql.catalog.warehouse.uri",
        "http://localhost:8181"
    ).config(
        "spark.sql.catalog.warehouse.s3.path-style-access",
        "true"
    )
    .config(
        "spark.sql.catalog.warehouse.s3.endpoint",
        "http://localhost:9000"
    )
    .config(
        "spark.sql.catalog.warehouse.s3.region",
        "us-east-1"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint.region",
        "us-east-1"
    )
    .config(
        "spark.driver.extraJavaOptions",
        "-Daws.region=us-east-1 "
        "-Daws.accessKeyId=minioadmin "
        "-Daws.secretAccessKey=minioadmin123"
    ).config(
        "spark.executor.extraJavaOptions",
        "-Daws.region=us-east-1 "
        "-Daws.accessKeyId=minioadmin "
        "-Daws.secretAccessKey=minioadmin123"
    )

    .getOrCreate()
)


# ============================================================
# CUSTOMERS
# ============================================================

spark.sql("""
INSERT INTO warehouse.bronze.customers VALUES
(1, 'Amit Sharma', 'amit@example.com', 'Delhi', 'India', TIMESTAMP '2026-01-05 10:00:00'),
(2, 'Priya Mehta', 'priya@example.com', 'Mumbai', 'India', TIMESTAMP '2026-01-06 11:00:00'),
(3, 'Rahul Verma', 'rahul@example.com', 'Bangalore', 'India', TIMESTAMP '2026-01-07 09:30:00'),
(4, 'Neha Singh', 'neha@example.com', 'Pune', 'India', TIMESTAMP '2026-01-08 14:00:00'),
(5, 'Arjun Patel', 'arjun@example.com', 'Ahmedabad', 'India', TIMESTAMP '2026-01-09 12:15:00'),
(6, 'Sneha Kapoor', 'sneha@example.com', 'Chennai', 'India', TIMESTAMP '2026-01-10 16:20:00'),
(7, 'Vikram Joshi', 'vikram@example.com', 'Hyderabad', 'India', TIMESTAMP '2026-01-11 10:45:00'),
(8, 'Riya Nair', 'riya@example.com', 'Kochi', 'India', TIMESTAMP '2026-01-12 13:30:00')
""")


# ============================================================
# PRODUCTS
# ============================================================

spark.sql("""
INSERT INTO warehouse.bronze.products VALUES
(101, 'Laptop', 'Electronics', 75000.00, TIMESTAMP '2026-01-01 09:00:00'),
(102, 'Smartphone', 'Electronics', 45000.00, TIMESTAMP '2026-01-01 09:00:00'),
(103, 'Headphones', 'Accessories', 5000.00, TIMESTAMP '2026-01-02 10:00:00'),
(104, 'Keyboard', 'Accessories', 3000.00, TIMESTAMP '2026-01-02 10:00:00'),
(105, 'Monitor', 'Electronics', 18000.00, TIMESTAMP '2026-01-03 11:00:00'),
(106, 'Mouse', 'Accessories', 1500.00, TIMESTAMP '2026-01-03 11:00:00')
""")


# ============================================================
# ORDERS
# ============================================================

spark.sql("""
INSERT INTO warehouse.bronze.orders VALUES
(1001, 1, 101, 1, 75000.00, 'COMPLETED', TIMESTAMP '2026-02-01 10:15:00'),
(1002, 2, 102, 1, 45000.00, 'COMPLETED', TIMESTAMP '2026-02-02 11:20:00'),
(1003, 3, 103, 2, 10000.00, 'COMPLETED', TIMESTAMP '2026-02-03 12:10:00'),
(1004, 1, 104, 2, 6000.00, 'COMPLETED', TIMESTAMP '2026-02-04 13:30:00'),
(1005, 4, 105, 1, 18000.00, 'PENDING', TIMESTAMP '2026-02-05 14:00:00'),
(1006, 5, 106, 3, 4500.00, 'COMPLETED', TIMESTAMP '2026-02-06 15:10:00'),
(1007, 2, 101, 1, 75000.00, 'COMPLETED', TIMESTAMP '2026-02-07 16:20:00'),
(1008, 6, 102, 2, 90000.00, 'COMPLETED', TIMESTAMP '2026-02-08 10:40:00'),
(1009, 7, 103, 1, 5000.00, 'CANCELLED', TIMESTAMP '2026-02-09 11:50:00'),
(1010, 8, 104, 1, 3000.00, 'COMPLETED', TIMESTAMP '2026-02-10 12:30:00'),
(1011, 3, 105, 2, 36000.00, 'COMPLETED', TIMESTAMP '2026-02-11 13:45:00'),
(1012, 4, 106, 2, 3000.00, 'COMPLETED', TIMESTAMP '2026-02-12 14:15:00'),
(1013, 5, 102, 1, 45000.00, 'COMPLETED', TIMESTAMP '2026-02-13 15:25:00'),
(1014, 1, 103, 3, 15000.00, 'COMPLETED', TIMESTAMP '2026-02-14 16:35:00'),
(1015, 6, 101, 1, 75000.00, 'PENDING', TIMESTAMP '2026-02-15 17:00:00')
""")


# ============================================================
# VALIDATION
# ============================================================

print("\n==============================")
print("BRONZE COUNTS")
print("==============================")

for table in ["customers", "products", "orders"]:
    print(f"\n{table}")
    spark.sql(
        f"SELECT COUNT(*) AS count FROM warehouse.bronze.{table}"
    ).show()


print("\n==============================")
print("BRONZE SEED SUCCESSFUL")
print("==============================")

spark.stop()