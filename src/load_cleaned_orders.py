from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder. \
    appName('Ecommerce Cleaned Orders'). \
    getOrCreate()

last_watermark = spark.sql("""select last_watermark from warehouse.ecommerce.silver.ingestion_control
          where source_table='bronze.orders' AND is_active=true 
          AND target_table='silver.cleaned_orders'""").first()["last_watermark"]

print(f"LATEST WATER MARK : {last_watermark}")

extraction_end = spark.sql("""
    SELECT current_timestamp() AS extraction_end
""").first()["extraction_end"]

print(f"Extraction Time End: {extraction_end}")

if last_watermark is None:
    where_condition = f"""
        updated_at <= TIMESTAMP '{extraction_end}'
    """
else:
    where_condition = f"""
        updated_at > TIMESTAMP '{last_watermark}'
        AND updated_at <= TIMESTAMP '{extraction_end}'
    """

print("\n LOAD FROM BRONZE ORDERS TABLE WITH WATER MARK \n")
order_df = spark.sql(f'SELECT * FROM warehouse.ecommerce.bronze.orders where {where_condition} ').cache()

filter_orders = order_df.filter("""
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity > 0
    AND order_amount > 0
    AND (order_status IS NOT NULL
    AND lower(order_status) IN ('placed', 'pending', 'completed','cancelled'))
    AND order_timestamp IS NOT NULL
    AND updated_at IS NOT NULL
""").withColumn(
    "order_status",
    F.lower(F.col("order_status"))
).cache()

window_spec = Window.partitionBy('order_id') \
    .orderBy(F.col('updated_at').desc())

dedup_df = filter_orders.withColumn('rn', F.row_number().over(window_spec)) \
    .filter(F.col('rn') == 1) \
    .drop(F.col('rn')).persist(StorageLevel.MEMORY_AND_DISK)

dedup_df.createOrReplaceTempView("clean_records")

merge_query = """ 
 MERGE INTO warehouse.ecommerce.silver.cleaned_orders as target
     USING clean_records AS source
        ON target.order_id=source.order_id
        WHEN MATCHED THEN UPDATE SET 
                target.customer_id=source.customer_id,
                target.product_id=source.product_id,
                target.quantity=source.quantity,
               target.order_amount=source.order_amount,
               target.order_status=source.order_status,
                target.order_timestamp=source.order_timestamp,
                target.discount=source.discount,
                target.updated_at=current_timestamp()
        
          WHEN NOT MATCHED THEN INSERT (
                order_id,
                customer_id,
                product_id,
                quantity,
                order_amount,
               order_status,
                order_timestamp,
                discount,
                updated_at
          )
          VALUES(
          source.order_id,
          source.customer_id,
          source.product_id,
          source.quantity,
          source.order_amount,
          source.order_status,
          source.order_timestamp,
          source.discount,
          current_timestamp()
          )
"""
total_records = order_df.count()
filter_records = filter_orders.count()
valid_records = dedup_df.count()
duplicate_records = filter_records - valid_records

invalid_records = total_records-valid_records
print(f"total_records={total_records}, valid_records={valid_records},invalid_records={invalid_records}")
try:
    spark.sql(merge_query)
    update_last_watermark = f"""UPDATE warehouse.ecommerce.silver.ingestion_control
    SET last_watermark=TIMESTAMP'{extraction_end}'
    WHERE source_table = 'bronze.orders'
    AND is_active = true
    AND target_table= 'silver.cleaned_orders'"""

    spark.sql(update_last_watermark)
    print("UPDATE LAST WATERMARK")

except Exception as e:
    print(f"FAIL TO LOAD {e}")
    raise

try:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    pass_percentage = round(
     (valid_records / total_records) * 100, 2
    ) if total_records > 0 else 100.0

    status = "PASS" if invalid_records == 0 else "FAIL"
    quality_check_query = f"""
    INSERT INTO warehouse.ecommerce.gold.data_quality_results (
        run_id,
        source_table_name,
        target_table_name,
        layer,
        check_name,
        total_records,
        passed_records,
        failed_records,
        duplicate_records,
        pass_percentage,
        status,
        execution_time
    )
    VALUES (
            '{run_id}',
            'bronze.orders',
            'silver.cleaned_orders',
             'bronze/silver',
            'cleaning of records',
             {total_records},
             {valid_records},
             {invalid_records},
             {duplicate_records},
             {pass_percentage},
            '{status}',
            current_timestamp()
)
"""
    spark.sql(quality_check_query)
except Exception as e:
    print(f"FAIL TO LOAD {e}")
    raise

spark.stop()
