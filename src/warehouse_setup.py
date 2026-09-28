from pyspark.sql import SparkSession


# ============================================================
# CONFIGURATION
# ============================================================

ICEBERG_JAR = (
    "/Users/mac/Documents/modern-data-warehouse/"
    "jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar"
)

spark = (
    SparkSession.builder
    .appName("ModernDataWarehouse-Setup")

    # Iceberg runtime
    .config("spark.jars", ICEBERG_JAR)

    # --------------------------------------------------------
    # Iceberg REST Catalog
    # --------------------------------------------------------
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
    )

    .getOrCreate()
)


# ============================================================
# SPARK INFO
# ============================================================

print("\n==============================")
print("Spark Information")
print("==============================")
print("Spark Version:", spark.version)


# ============================================================
# CHECK CATALOG
# ============================================================

print("\n==============================")
print("Warehouse Catalog")
print("==============================")

spark.sql(
    "SHOW NAMESPACES IN warehouse"
).show(truncate=False)


# ============================================================
# CREATE MEDALLION NAMESPACES
# ============================================================

print("\n==============================")
print("Creating Medallion Namespaces")
print("==============================")

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS warehouse.bronze"
)

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS warehouse.silver"
)

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS warehouse.gold"
)

print("Bronze, Silver and Gold namespaces created.")


# ============================================================
# BRONZE - CUSTOMERS
# ============================================================

print("\n==============================")
print("Creating Bronze Customers")
print("==============================")

spark.sql("""
CREATE TABLE IF NOT EXISTS warehouse.bronze.customers (
    customer_id BIGINT,
    customer_name STRING,
    email STRING,
    city STRING,
    country STRING,
    created_at TIMESTAMP
)
USING iceberg
""")


# ============================================================
# BRONZE - PRODUCTS
# ============================================================

print("\n==============================")
print("Creating Bronze Products")
print("==============================")

spark.sql("""
CREATE TABLE IF NOT EXISTS warehouse.bronze.products (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    created_at TIMESTAMP
)
USING iceberg
""")


# ============================================================
# BRONZE - ORDERS
# ============================================================

print("\n==============================")
print("Creating Bronze Orders")
print("==============================")

spark.sql("""
CREATE TABLE IF NOT EXISTS warehouse.bronze.orders (
    order_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    quantity INT,
    order_amount DECIMAL(10, 2),
    order_status STRING,
    order_timestamp TIMESTAMP
)
USING iceberg
""")


# ============================================================
# VERIFY NAMESPACES
# ============================================================

print("\n==============================")
print("Medallion Namespaces")
print("==============================")

spark.sql(
    "SHOW NAMESPACES IN warehouse"
).show(truncate=False)


# ============================================================
# VERIFY BRONZE TABLES
# ============================================================

print("\n==============================")
print("Bronze Tables")
print("==============================")

spark.sql(
    "SHOW TABLES IN warehouse.bronze"
).show(truncate=False)


# ============================================================
# DESCRIBE CUSTOMERS
# ============================================================

print("\n==============================")
print("Customers Schema")
print("==============================")

spark.sql(
    "DESCRIBE TABLE warehouse.bronze.customers"
).show(truncate=False)


# ============================================================
# FINISHED
# ============================================================

print("\n==============================")
print("SETUP SUCCESSFUL")
print("==============================")

print("""
Catalog : warehouse

Medallion Architecture:

warehouse
├── bronze
│   ├── customers
│   ├── products
│   └── orders
│
├── silver
│
└── gold
""")

spark.stop()

