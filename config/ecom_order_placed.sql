DESCRIBE warehouse.ecommerce.silver.dim_date;
DESCRIBE warehouse.ecommerce.silver.dim_products;
DESCRIBE warehouse.ecommerce.silver.dim_category;
DESCRIBE warehouse.ecommerce.silver.dim_customer;
DESCRIBE warehouse.ecommerce.silver.fact_orders;

# VERIFY NEW ENTRY IN Bronze order Table
SELECT o.* from warehouse.ecommerce.bronze.orders o LEFT ANTI JOIN warehouse.ecommerce.silver.fact_orders f ON o.order_id=f.order_id;

# dimension lookup verify because fact table doest not have source_id like customer_id. Fact table store surrogate key like customer-key
SELECT * from warehouse.ecommerce.silver.dim_customer  where customer_id=1 and is_current=true
SELECT * from warehouse.ecommerce.silver.dim_product  where product_id=103 and is_current=true
SELECT * from warehouse.ecommerce.silver.dim_date  where full_date=DATE '2026-03-17 10:00:00';

### INSERT NEW RECORDS warehouse.ecommerce.silver.fact_orders  THIS WILL NOT PREDICT IF OLD RECORD IS UPDATED SO MERGE UPSERT USEFUL IN SUCH SCENARIO
INSERT INTO warehouse.ecommerce.silver.fact_orders
 SELECT
   o.order_id,
   c.customer_key,
   p.product_key,
   d.date_key,
   o.quantity,
   o.order_amount,
   o.order_status,
   o.order_timestamp FROM(SELECT o.* from warehouse.ecommerce.bronze.orders o
   LEFT ANTI JOIN warehouse.ecommerce.silver.fact_orders f ON o.order_id=f.order_id) o
   JOIN warehouse.ecommerce.silver.dim_customer c ON o.customer_id=c.customer_id AND c.is_current=true
   JOIN warehouse.ecommerce.silver.dim_products p ON  o.product_id=p.product_id AND p.is_current=true
   JOIN warehouse.ecommerce.silver.dim_date d ON CAST(o.order_timestamp AS DATE)=d.full_date

#MERGE BASED=UPSERT

UPDATE warehouse.ecommerce.bronze.orders
SET order_status = 'COMPLETED'
WHERE order_id = 39;

MERGE INTO warehouse.ecommerce.silver.fact_orders AS target

USING (
    SELECT
        o.order_id,
        c.customer_key,
        p.product_key,
        d.date_key,
        o.quantity,
        o.order_amount,
        o.order_status,
        o.order_timestamp
    FROM warehouse.ecommerce.bronze.orders AS o

    JOIN warehouse.ecommerce.silver.dim_customer AS c
        ON o.customer_id = c.customer_id
       AND c.is_current = true

    JOIN warehouse.ecommerce.silver.dim_products AS p
        ON o.product_id = p.product_id
       AND p.is_current = true

    JOIN warehouse.ecommerce.silver.dim_date AS d
        ON CAST(o.order_timestamp AS DATE) = d.full_date
) AS source

ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
    target.customer_key = source.customer_key,
    target.product_key = source.product_key,
    target.date_key = source.date_key,
    target.quantity = source.quantity,
    target.order_amount = source.order_amount,
    target.order_status = source.order_status,
    target.order_timestamp = source.order_timestamp

WHEN NOT MATCHED THEN INSERT (
    order_id,
    customer_key,
    product_key,
    date_key,
    quantity,
    order_amount,
    order_status,
    order_timestamp
)
VALUES (
    source.order_id,
    source.customer_key,
    source.product_key,
    source.date_key,
    source.quantity,
    source.order_amount,
    source.order_status,
    source.order_timestamp
);

## INCREMENTAL MERGE

ALTER TABLE warehouse.ecommerce.bronze.orders
ADD COLUMNS (updated_at TIMESTAMP);

UPDATE warehouse.ecommerce.bronze.orders
SET updated_at = order_timestamp;

UPDATE warehouse.ecommerce.bronze.orders
SET
    order_status = 'COMPLETED',
    updated_at = current_timestamp()
WHERE order_id = 39;

SELECT
    order_id,
    order_status,
    updated_at
FROM warehouse.ecommerce.bronze.orders
WHERE order_id = 39;

SELECT
    order_id,
    order_status,
    updated_at
FROM warehouse.ecommerce.bronze.orders
WHERE updated_at > TIMESTAMP '2026-09-30 17:30:00'
ORDER BY updated_at;

# HERE WE DOING WATER MARKING STRATEGY because very time we can not complete record

## BELOW FILTER APPLY WITH MERGE
SELECT
    o.order_id,
    c.customer_key,
    p.product_key,
    d.date_key,
    o.quantity,
    o.order_amount,
    o.order_status,
    o.order_timestamp
FROM warehouse.ecommerce.bronze.orders AS o

JOIN warehouse.ecommerce.silver.dim_customer AS c
    ON o.customer_id = c.customer_id
   AND c.is_current = true

JOIN warehouse.ecommerce.silver.dim_products AS p
    ON o.product_id = p.product_id
   AND p.is_current = true

JOIN warehouse.ecommerce.silver.dim_date AS d
    ON CAST(o.order_timestamp AS DATE) = d.full_date

WHERE o.updated_at > TIMESTAMP '2026-09-30 17:30:00';

MERGE INTO warehouse.ecommerce.silver.fact_orders AS target

USING (
    SELECT
        o.order_id,
        c.customer_key,
        p.product_key,
        d.date_key,
        o.quantity,
        o.order_amount,
        o.order_status,
        o.order_timestamp
    FROM warehouse.ecommerce.bronze.orders AS o

    JOIN warehouse.ecommerce.silver.dim_customer AS c
        ON o.customer_id = c.customer_id
       AND c.is_current = true

    JOIN warehouse.ecommerce.silver.dim_products AS p
        ON o.product_id = p.product_id
       AND p.is_current = true

    JOIN warehouse.ecommerce.silver.dim_date AS d
        ON CAST(o.order_timestamp AS DATE) = d.full_date

    WHERE o.updated_at > TIMESTAMP '2026-09-30 17:30:00'
) AS source

ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
    target.customer_key = source.customer_key,
    target.product_key = source.product_key,
    target.date_key = source.date_key,
    target.quantity = source.quantity,
    target.order_amount = source.order_amount,
    target.order_status = source.order_status,
    target.order_timestamp = source.order_timestamp

WHEN NOT MATCHED THEN INSERT (
    order_id,
    customer_key,
    product_key,
    date_key,
    quantity,
    order_amount,
    order_status,
    order_timestamp
)
VALUES (
    source.order_id,
    source.customer_key,
    source.product_key,
    source.date_key,
    source.quantity,
    source.order_amount,
    source.order_status,
    source.order_timestamp
);

## IF WE SEE UPDATED DATE is Hard Coded so now we apply water mark incremental load.
1. CREATE ingestion_control control table to hold the information regarding last processed record
CREATE TABLE warehouse.ecommerce.silver.ingestion_control (
    source_table STRING,
    watermark_column STRING,
    last_watermark TIMESTAMP,
    target_table STRING,
    is_active BOOLEAN
)
USING iceberg;

2. ORDER WATERMARK INSERT

INSERT INTO warehouse.ecommerce.silver.ingestion_control
VALUES (
    'orders',
    'updated_at',
     NULL,
    'silver.cleaned_orders',
    true
);

3. INSERT NEW RECORDS
INSERT INTO warehouse.ecommerce.bronze.orders
VALUES (
    40,
    1,
    103,
    1,
    5000.00,
    'PENDING',
    TIMESTAMP '2026-03-18 10:00:00',
    250.00,
    current_timestamp()
);

4. CHECK WATERMARK PICK THE LATEST INSERT RECORD
 select order_id,order_status,updated_at
    from warehouse.ecommerce.bronze.orders
        where updated_at >
        (select last_watermark from warehouse.ecommerce.silver.ingestion_control
            where source_table='orders' AND is_active=true
            ) order by updated_at
5. NOW  actual incremental MERGE

MERGE INTO warehouse.ecommerce.silver.fact_orders AS target

USING (
    SELECT
        o.order_id,
        c.customer_key,
        p.product_key,
        d.date_key,
        o.quantity,
        o.order_amount,
        o.order_status,
        o.order_timestamp
    FROM warehouse.ecommerce.bronze.orders AS o

    JOIN warehouse.ecommerce.silver.dim_customer AS c
        ON o.customer_id = c.customer_id
       AND c.is_current = true

    JOIN warehouse.ecommerce.silver.dim_products AS p
        ON o.product_id = p.product_id
       AND p.is_current = true

    JOIN warehouse.ecommerce.silver.dim_date AS d
        ON CAST(o.order_timestamp AS DATE) = d.full_date

    WHERE o.updated_at >  (select last_watermark from warehouse.ecommerce.silver.ingestion_control
                                     where source_table='orders' AND is_active=true
                                     )
) AS source

ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
    target.customer_key = source.customer_key,
    target.product_key = source.product_key,
    target.date_key = source.date_key,
    target.quantity = source.quantity,
    target.order_amount = source.order_amount,
    target.order_status = source.order_status,
    target.order_timestamp = source.order_timestamp

WHEN NOT MATCHED THEN INSERT (
    order_id,
    customer_key,
    product_key,
    date_key,
    quantity,
    order_amount,
    order_status,
    order_timestamp
)
VALUES (
    source.order_id,
    source.customer_key,
    source.product_key,
    source.date_key,
    source.quantity,
    source.order_amount,
    source.order_status,
    source.order_timestamp
);

# This temporary update of last_watermark ingestion_control table

UPDATE warehouse.ecommerce.silver.ingestion_control
SET last_watermark = (
    SELECT MAX(updated_at)
    FROM warehouse.ecommerce.bronze.orders
    WHERE updated_at > last_watermark
)
WHERE source_table = 'orders'
  AND is_active = true;

  # Now New records > Merge Fail > No watermark update > same records picks in next run > Merge > watermark update

  1. VERIFY any new records
  SELECT COUNT(*) AS incremental_count
  FROM warehouse.ecommerce.bronze.orders
  WHERE updated_at > (
      SELECT last_watermark
      FROM warehouse.ecommerce.silver.ingestion_control
      WHERE source_table = 'orders'
        AND is_active = true
  );
  2. INSERT 3 new record
  INSERT INTO warehouse.ecommerce.bronze.orders
  VALUES
  (
      41,
      1,
      103,
      1,
      5000.00,
      'PENDING',
      TIMESTAMP '2026-03-19 10:00:00',
      250.00,
      current_timestamp()
  ),
  (
      42,
      2,
      105,
      2,
      36000.00,
      'PENDING',
      TIMESTAMP '2026-03-19 11:00:00',
      1000.00,
      current_timestamp()
  ),
  (
      43,
      3,
      104,
      1,
      3000.00,
      'PENDING',
      TIMESTAMP '2026-03-19 12:00:00',
      150.00,
      current_timestamp()
  );
  SELECT
      order_id,
      order_status,
      updated_at
  FROM warehouse.ecommerce.bronze.orders
  WHERE updated_at > (
      SELECT last_watermark
      FROM warehouse.ecommerce.silver.ingestion_control
      WHERE source_table = 'orders'
        AND is_active = true
  )
  ORDER BY order_id;

  # NOTE Iceberg MERGE is atomic(transaction is either completed fully or
  not completed at all; it does not leave a partial committed transaction.)
  #save in iceberg so lets update the records pipeline fail after merge and last_watermark is not update state

  ## CHECK NEW RECORDS ENTER IN fact_orders without updating the ingestion_control last_watermark watermark_column
  SELECT
      order_id,
      order_status,
      order_amount
  FROM warehouse.ecommerce.silver.fact_orders
  WHERE order_id IN (41, 42, 43)
  ORDER BY order_id;

  ### So old water mark

  SELECT last_watermark
  FROM warehouse.ecommerce.silver.ingestion_control
  WHERE source_table = 'orders'
    AND is_active = true;

    ## 3 Records in incremental batch

    SELECT
        order_id,
        order_status,
        updated_at
    FROM warehouse.ecommerce.bronze.orders
    WHERE updated_at > (
        SELECT last_watermark
        FROM warehouse.ecommerce.silver.ingestion_control
        WHERE source_table = 'orders'
          AND is_active = true
    )
    ORDER BY order_id;

   SELECT  o.order_id,
           c.customer_key,
           p.product_key,
           d.date_key,
           o.quantity,
           o.order_amount,
           o.order_status,
           o.discount,
           o.order_timestamp
           FROM (
              SELECT *
             FROM(
               SELECT
               order_id,
               customer_id,
               product_id,
               quantity,
               order_amount,
               order_status,
               order_timestamp,
               discount,
               updated_at,ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY updated_at DESC) as rn
           FROM warehouse.ecommerce.bronze.orders

            ) AS x
             where x.rn=1
    )AS o
       JOIN warehouse.ecommerce.silver.dim_customer AS c
           ON o.customer_id = c.customer_id
          AND c.is_current = true

       JOIN warehouse.ecommerce.silver.dim_products AS p
           ON o.product_id = p.product_id
          AND p.is_current = true

       JOIN warehouse.ecommerce.silver.dim_date AS d
           ON CAST(o.order_timestamp AS DATE) = d.full_date;




