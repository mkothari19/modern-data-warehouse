from pyspark.sql import SparkSession
from datetime import datetime

"""
1. Read last_watermark
2. Capture extraction_end
3. MERGE → fact_orders
4. MERGE successful → update watermark
5. DQ insert 
"""
spark = SparkSession.builder. \
    appName('Ecommerce-FactOrders-Incremental').getOrCreate()

last_watermark = spark.sql("""select last_watermark from warehouse.ecommerce.silver.ingestion_control
          where source_table='silver.cleaned_orders' AND is_active=true 
          AND target_table='silver.fact_orders'""").first()["last_watermark"]

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

fact_order_query = f""" SELECT
                            o.order_id,
                            c.customer_key,
                            p.product_key,
                            p.category_key,
                            d.date_key,
                            o.quantity,
                            o.order_amount,
                            o.order_status,
                            o.discount,
                            o.order_timestamp
                        FROM warehouse.ecommerce.silver.cleaned_orders AS o
                        
                        LEFT JOIN warehouse.ecommerce.silver.dim_customer AS c
                            ON o.customer_id = c.customer_id
                           AND c.is_current = true
                        
                       LEFT JOIN warehouse.ecommerce.silver.dim_products AS p
                            ON o.product_id = p.product_id
                           AND p.is_current = true
                      
                        
                       LEFT JOIN warehouse.ecommerce.silver.dim_date AS d
                            ON CAST(o.order_timestamp AS DATE) = d.full_date
                        
                        WHERE {where_condition}
"""

fact_orders_df = spark.sql(fact_order_query).cache()
fact_orders_df.createOrReplaceTempView('fact_orders_records_temp')
merge_query = """ MERGE INTO warehouse.ecommerce.silver.fact_orders AS target

                    USING fact_orders_records_temp AS source

                    ON target.order_id = source.order_id
                    
                    WHEN MATCHED THEN UPDATE SET
                        target.customer_key = source.customer_key,
                        target.product_key = source.product_key,
                        target.date_key = source.date_key,
                        target.category_key=source.category_key,
                        target.quantity = source.quantity,
                        target.order_amount = source.order_amount,
                        target.order_status = source.order_status,
                        target.discount=source.discount,
                        target.order_timestamp = source.order_timestamp

                    WHEN NOT MATCHED THEN INSERT (
                        order_id,
                        customer_key,
                        product_key,
                        category_key,
                        date_key,
                        quantity,
                        order_amount,
                        order_status,
                        discount,
                        order_timestamp
                    )
                    VALUES (
                        source.order_id,
                        source.customer_key,
                        source.product_key,
                        source.category_key,
                        source.date_key,
                        source.quantity,
                        source.order_amount,
                        source.order_status,
                        source.discount,
                        source.order_timestamp
                    );
"""
print("\n QUERY EXECUTED FOR INCREMENTAL LOAD \n")
print(merge_query)
try:
    spark.sql(merge_query)
    print("MERGE SUCCESSFUL")
    update_last_watermark = f"""
    UPDATE warehouse.ecommerce.silver.ingestion_control 
    SET last_watermark=TIMESTAMP'{extraction_end}'
    WHERE source_table = 'silver.cleaned_orders'
      AND is_active = true
      AND target_table= 'silver.fact_orders'
  """
    spark.sql(update_last_watermark)
    print("UPDATE LAST WATERMARK")

except Exception as e:
    print("MERGE UNSUCCESSFUL")
    raise

try:
    print("\nCHECK DATA QUALITY\n")

    dq_result = spark.sql("""
        SELECT
            COUNT(*) AS total_records,

            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)
                AS order_id_failed,

            SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END)
                AS customer_mapping_failed,

            SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END)
                AS product_mapping_failed,

            SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END)
                AS date_mapping_failed

        FROM fact_orders_records_temp
    """).first()

    total_records = dq_result["total_records"]

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    checks = [
        ("order_id_not_null", dq_result["order_id_failed"]),
        ("customer_mapping", dq_result["customer_mapping_failed"]),
        ("product_mapping", dq_result["product_mapping_failed"]),
        ("date_mapping", dq_result["date_mapping_failed"])
    ]

    values = []
    duplicate_records = 0
    for check_name, failed_records in checks:

        passed_records = total_records - failed_records

        pass_percentage = round(
            (passed_records / total_records) * 100, 2
        ) if total_records > 0 else 100.0

        status = "PASS" if failed_records == 0 else "FAIL"

        values.append(
            f"""
            (
                '{run_id}',
                'silver.cleaned_orders',
                'silver.fact_orders',
                'silver',
                '{check_name}',
                {total_records},
                {passed_records},
                {failed_records},
                {duplicate_records},
                {pass_percentage},
                '{status}',
                current_timestamp()
            )
            """
        )

    dq_insert_query = f"""
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
        VALUES
        {','.join(values)}
    """

    spark.sql(dq_insert_query)

    print("ALL DQ RESULTS INSERTED IN ONE BATCH")

except Exception as e:
    print("FAIL TO LOAD INTO gold.data_quality_results")
    raise
spark.stop()
