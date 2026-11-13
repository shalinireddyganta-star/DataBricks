-- Databricks notebook source
DROP TABLE hidi.raw.product

-- COMMAND ----------

SELECT name, id FROM retailraw.bronze.Pipelines

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC # Initialize Databricks client
-- MAGIC w = WorkspaceClient()
-- MAGIC
-- MAGIC # Get all Lakeflow pipelines
-- MAGIC pipelines = w.pipelines.list_pipelines()
-- MAGIC
-- MAGIC # Print Pipeline ID and Name
-- MAGIC for pipeline in pipelines:
-- MAGIC     print("Pipeline ID:", pipeline.pipeline_id)
-- MAGIC     print("Pipeline Name:", pipeline.name)
-- MAGIC     print("-" * 50)
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC w = WorkspaceClient()
-- MAGIC
-- MAGIC # Provide Lakeflow Pipeline Name
-- MAGIC pipeline_name = "RetailLakeFlowPipelines"
-- MAGIC
-- MAGIC # Find pipeline by name
-- MAGIC pipelines = w.pipelines.list_pipelines()
-- MAGIC
-- MAGIC pipeline = next(
-- MAGIC     (p for p in pipelines if p.name == pipeline_name),
-- MAGIC     None
-- MAGIC )
-- MAGIC
-- MAGIC if pipeline:
-- MAGIC     print("Pipeline Name:", pipeline.name)
-- MAGIC     print("Pipeline ID:", pipeline.pipeline_id)
-- MAGIC else:
-- MAGIC     print(f"Pipeline not found: {pipeline_name}")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC w = WorkspaceClient()
-- MAGIC
-- MAGIC pipeline_id = "7c4db94a-f5d3-460c-8e6c-bd5b09dd84e1"
-- MAGIC
-- MAGIC # Get pipeline details
-- MAGIC pipeline = w.pipelines.get(pipeline_id=pipeline_id)
-- MAGIC
-- MAGIC # Get pipeline name
-- MAGIC pipeline_name = pipeline.name
-- MAGIC
-- MAGIC print("Lakeflow Pipeline Name:", pipeline_name)

-- COMMAND ----------

SELECT
    pipeline_id,
    name,
    workspace_id,
    create_time
FROM system.lakeflow.pipelines
where name = 'RetailLakeFlowPipelines'


-- COMMAND ----------

-- DBTITLE 1,Cell 3
-- MAGIC %python
-- MAGIC # pyspark.pipelines exists only inside pipeline runs, and custom pipeline
-- MAGIC # configuration keys are not readable on serverless notebook compute
-- MAGIC # (CONFIG_NOT_AVAILABLE) — hardcode the value here instead.
-- MAGIC PipelineID = "7c4db94a-f5d3-460c-8e6c-bd5b09dd84e1"

-- COMMAND ----------

DROP TABLE hidi.raw.product 

-- COMMAND ----------

select * from hidi.raw.product LIMIT 2

-- COMMAND ----------

-- DBTITLE 1,Cell 1
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp, lit
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC from pyspark.sql.functions import (
-- MAGIC     current_timestamp,
-- MAGIC     input_file_name
-- MAGIC )
-- MAGIC
-- MAGIC # 'Bronzecatalog' is pipeline configuration — readable only inside pipeline runs.
-- MAGIC # On serverless notebook compute spark.conf.get blocks custom keys (CONFIG_NOT_AVAILABLE),
-- MAGIC # so set the value directly here (matches pipeline settings).
-- MAGIC bronze_catalog = "hidi"  # hardcode: pipeline config keys are unreadable here
-- MAGIC print(bronze_catalog)

-- COMMAND ----------

-- DBTITLE 1,Cell 1
CREATE OR REPLACE STREAMING TABLE retailgold.retail.SilverPayment_Source
COMMENT 'Payment source transformation'
TBLPROPERTIES (
    'quality' = 'GOLD',
    'layer' = 'GOLD',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT 
    pay.payment_id AS Paymentkey,
    pay.order_id AS OrderID,
    pay.method AS PaymentMethod,
    pay.amount AS PaymentAmount,
    pay.paid_at AS PaidAT,

    CAST(
        DATE_FORMAT(TO_DATE(pay.paid_at), 'yyyyMMdd')
        AS INT
    ) AS paid_at_key,
    CASE WHEN pay.status = 'captured' THEN 'sucesss' ELSE 'Failed'
    END AS PayementStatus,
    pay.failure_reason

FROM STREAM(retailraw.bronze.payment) pay;

-- COMMAND ----------

select * from retailraw.bronze.payment pay

-- COMMAND ----------

select Paymentkey  from retailgold.retail.factpayment
group by Paymentkey having count(*) >1

-- COMMAND ----------

SELECT * FROM retailgold.retail.SilverPayment_Source

-- COMMAND ----------

-- DBTITLE 1,Cell 4
-- SilverPayment_Source is owned by the RetailLakeFlowPipelines pipeline.
-- Its definition lives in transformations/Gold/FactPaymentTransformation.sql
-- and cannot be created/replaced/refreshed from a notebook
-- (STREAMING_TABLE_OPERATION_NOT_ALLOWED). Use this cell to inspect the result:
SELECT * FROM retailgold.retail.SilverPayment_Source;

-- COMMAND ----------

DROP TABLE retailgold.retail.FactPayment 

-- COMMAND ----------

Select count(1) from retailgold.retail.FactPayment 
union all
select count(1) from retailraw.bronze.payment

-- COMMAND ----------

describe  retailraw.bronze.payment 

-- COMMAND ----------

describe retailgold.retail.FactPayment

-- COMMAND ----------

select distinct PayementStatus from retailgold.retail.FactPayment

-- COMMAND ----------

CREATE OR REPLACE STREAMING  TABLE retailgold.retail.SilverPayment_Source
COMMENT 'Payment source transformation'
TBLPROPERTIES (
    'quality' = 'GOLD',
    'layer' = 'GOLD',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT 
    pay.payment_id AS Paymentkey,
    orditem.order_id AS OrderID,
    pay.method AS PaymentMethod,
    pay.amount AS PaymentAmount,
    pay.paid_at AS PaidAT,

    CAST(
        DATE_FORMAT(TO_DATE(pay.paid_at), 'yyyyMMdd')
        AS INT
    ) AS paid_at_key,

    pay.status,
    pay.failure_reason

FROM STREAM(retailraw.bronze.payment) pay
INNER JOIN STREAM(retailraw.bronze.raworderitems) orditem
    ON pay.order_id = orditem.order_id;


-- COMMAND ----------

select * from retailgold.retail.SilverPayment_Source

-- COMMAND ----------

select Paymentkey from V_Payment
group by Paymentkey having count(*) > 1

-- COMMAND ----------

select count(1)
from (select distinct  * from V_Payment)


-- COMMAND ----------

select count(1) from retailgold.retail.SilverPayment_Source

-- COMMAND ----------

select count(1) from V_Payment --93687

-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE retailgold.retail.SilverPayment_Source
COMMENT 'Payment source transformation'
TBLPROPERTIES (
    'quality' = 'GOLD',
    'layer' = 'GOLD',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT 
    pay.payment_id AS Paymentkey,
    pay.order_id AS OrderID,
    pay.method AS PaymentMethod,
    pay.amount AS PaymentAmount,
    pay.paid_at AS PaidAT,

    CAST(
        DATE_FORMAT(TO_DATE(pay.paid_at), 'yyyyMMdd')
        AS INT
    ) AS paid_at_key,
    pay.status,
    CASE  WHEN    pay.status = 'captured' then ' Payement'
    when  pay.status <> 'captured' then 'Failed'
    END PayementStatus,
    pay.failure_reason

FROM STREAM(retailraw.bronze.payment) pay;


-- COMMAND ----------

select distinct status from retailraw.bronze.payment pay

-- COMMAND ----------

select * from  retailraw.bronze.payment pay
where Order_id = '6199'

-- COMMAND ----------

select * from retailraw.bronze.raworderitems orditem
where Order_id = '6199'

-- COMMAND ----------


CREATE OR REPLACE STREAMING TABLE retailgold.retail.SilverPayment_Source_Test
COMMENT 'Payment source transformation'
TBLPROPERTIES (
    'quality' = 'GOLD',
    'layer' = 'GOLD',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT distinct
    pay.payment_id AS Paymentkey,
    pay.order_id AS OrderID,
    pay.method AS PaymentMethod,
    pay.amount AS PaymentAmount,
    pay.paid_at AS PaidAT,

    CAST(
        DATE_FORMAT(TO_DATE(pay.paid_at), 'yyyyMMdd')
        AS INT
    ) AS paid_at_key,

    pay.status,
    pay.failure_reason

FROM STREAM(retailraw.bronze.payment) pay;

-- COMMAND ----------

create or replace temporary view V_Payment 
as
select
pay.payment_id as Paymentkey,
pay.order_id   As OrderID,
pay.method AS PaymentMethod,
pay.amount as PaymentAmount,
pay.paid_at as PaidAT,
 -- Date dimension keys
    CAST(date_format(
        TO_DATE(pay.paid_at),
        'yyyyMMdd'
    ) AS INT) AS paid_at_key,
pay.status,
pay.failure_reason

from retailraw.bronze.payment pay
--inner join retailraw.bronze.raworderitems orditem on pay.order_id = orditem.order_id;

-- COMMAND ----------

select * from retailgold.retail.FactOrderItem

-- COMMAND ----------

select * from retailraw.bronze.customer 

-- COMMAND ----------

describe retailraw.bronze.raworderitems 

-- COMMAND ----------

select * from retailgold.retail.factcustomerHistory
where first_name = 'Delphine'
and last_name  = 'Rossi'
and `__END_AT` is  null

-- COMMAND ----------

describe retailraw.bronze.OrdersMain

-- COMMAND ----------

select * from retailraw.bronze.OrdersMain 

-- COMMAND ----------

describe retailraw.bronze.OrdersMain

-- COMMAND ----------

select distinct status from retailraw.bronze.OrdersMain 

-- COMMAND ----------

select * from retailraw.bronze.raworderitems
where order_id =  33445

-- COMMAND ----------

describe retailraw.bronze.raworderitems

-- COMMAND ----------

select * from retailgold.retail.dimSilverProduct
where PRODUCT_ID = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

DROP MATERIALIZED VIEW  retailgold.retail.FOrderItemCDC

-- COMMAND ----------

SELECT * FROM retailgold.retail.FOrderItemCDC

-- COMMAND ----------

select distinct status from retailraw.bronze.OrdersMain 

-- COMMAND ----------

select * from retailgold.retail.factorderitem
where OrderID = 2


-- COMMAND ----------

-- DBTITLE 1,Cell 14
update retailraw.bronze.OrdersMain 
set status = 'returned'
where order_id = 2

-- COMMAND ----------

select * from retailraw.bronze.OrdersMain 
where order_id = 2

-- COMMAND ----------

select count(1) from  retailgold.retail.FactOrderItem

-- COMMAND ----------

SELECT * from retailgold.retail.FactOrderItem

-- COMMAND ----------

create or refresh MATERIALIZED VIEW
 retailgold.retail.FOrderItemCDC
AS
select DISTINCT
ord.order_id As OrderID,
cast(ord.ordered_at as date) as ordereddate,
cast(ord.shipped_at as date) as shippeddate,
 -- Date dimension keys
    CAST(date_format(
        TO_DATE(ord.ordered_at),
        'yyyyMMdd'
    ) AS INT) AS order_date_key,

    CAST(date_format(
        TO_DATE(ord.shipped_at),
        'yyyyMMdd'
    ) AS INT) AS shipped_date_key,

ord.status as orderstatus,
case when ord.status = 'delivered' or ord.status = 'shipped' then true else false end OrderDelivered,
ord.channel,
ord.shipping_country,
ord.coupon_code,
--cust.last_name || cust.first_name as customerfullname,
cust.CustomerID,
prod.PRODUCT_ID as ProductID,
orditem.order_item_id AS OrderItemID,
orditem.quantity,
orditem.unit_price,
orditem.discount_amount,
-- Calculated measures
    ROUND(
    orditem.quantity * orditem.unit_price,
        2
    ) AS Gross_Amount,

    ROUND(
        COALESCE(orditem.discount_amount, 0),
        2
    ) AS Total_Discount,

    ROUND(
        (orditem.quantity * orditem.unit_price)
        - COALESCE(orditem.discount_amount, 0),
        2
    ) AS Net_Amount,

    ROUND(
        orditem.quantity * orditem.unit_price,
        2
    ) AS Total_Price,

    ROUND(
        (
            (orditem.quantity * orditem.unit_price)
            - COALESCE(orditem.discount_amount, 0)
        ) / NULLIF(orditem.quantity, 0),
        2
    ) AS Net_Unit_Price,
       CURRENT_TIMESTAMP() AS change_timestamp

from  retailraw.bronze.OrdersMain ord
left join retailgold.retail.factcustomerHistory  cust on ord.customer_id = cust.CustomerID and cust.`__END_AT` is  null
left join retailraw.bronze.raworderitems orditem on ord.order_id = orditem.order_id

left join retailgold.retail.dimsilverproduct prod on orditem.product_id = prod.PRODUCT_ID;

-- COMMAND ----------

select order_id,order_item_id,product_id
from retailgold.retail.FOrderItemCDC
group by order_id,order_item_id,product_id
having count(*) >1

-- COMMAND ----------

select * from retailgold.retail.FOrderItemCDC

-- COMMAND ----------

SELECT COUNT(1) FROM retailgold.retail.FOrderItemCDC

-- COMMAND ----------

SELECT COUNT(1) FROM TEST

-- COMMAND ----------

SELECT COUNT(1) FROM TEST --431242

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TEST 
AS
select  DISTINCT 
ord.order_id,
cast(ord.ordered_at as date) as ordereddate,
cast(ord.shipped_at as date) as shippeddate,
 -- Date dimension keys
    CAST(date_format(
        TO_DATE(ord.ordered_at),
        'yyyyMMdd'
    ) AS INT) AS order_date_key,

    CAST(date_format(
        TO_DATE(ord.shipped_at),
        'yyyyMMdd'
    ) AS INT) AS shipped_date_key,

ord.status as orderstatus,
case when ord.status = 'delivered' or ord.status = 'shipped' then true else false end OrderDelivered,
ord.channel,
ord.shipping_country,
ord.coupon_code,
--cust.last_name || cust.first_name as customerfullname,
cust.CustomerID,
prod.PRODUCT_ID,
orditem.order_item_id,
orditem.quantity,
orditem.unit_price,
orditem.discount_amount,
-- Calculated measures
    ROUND(
    orditem.quantity * orditem.unit_price,
        2
    ) AS Gross_Amount,

    ROUND(
        COALESCE(orditem.discount_amount, 0),
        2
    ) AS Total_Discount,

    ROUND(
        (orditem.quantity * orditem.unit_price)
        - COALESCE(orditem.discount_amount, 0),
        2
    ) AS Net_Amount,

    ROUND(
        orditem.quantity * orditem.unit_price,
        2
    ) AS Total_Price,

    ROUND(
        (
            (orditem.quantity * orditem.unit_price)
            - COALESCE(orditem.discount_amount, 0)
        ) / NULLIF(orditem.quantity, 0),
        2
    ) AS Net_Unit_Price

from  retailraw.bronze.OrdersMain ord
left join retailgold.retail.factcustomerHistory  cust on ord.customer_id = cust.CustomerID and cust.`__END_AT` is  null
left join retailraw.bronze.raworderitems orditem on ord.order_id = orditem.order_id

left join retailgold.retail.dimsilverproduct prod on orditem.product_id = prod.PRODUCT_ID


-- COMMAND ----------

select 
ord.order_id,
cast(ord.ordered_at as date) as ordereddate,
cast(ord.shipped_at as date) as shippeddate,
 -- Date dimension keys
    CAST(date_format(
        TO_DATE(ord.ordered_at),
        'yyyyMMdd'
    ) AS INT) AS order_date_key,

    CAST(date_format(
        TO_DATE(ord.shipped_at),
        'yyyyMMdd'
    ) AS INT) AS shipped_date_key,

ord.status as orderstatus,
case when ord.status = 'delivered' or ord.status = 'shipped' then true else false end OrderDelivered,
ord.channel,
ord.shipping_country,
ord.coupon_code,
--cust.last_name || cust.first_name as customerfullname,
cust.CustomerID,
prod.PRODUCT_ID,
orditem.order_item_id,
orditem.quantity,
orditem.unit_price,
orditem.discount_amount,
-- Calculated measures
    ROUND(
    orditem.quantity * orditem.unit_price,
        2
    ) AS Gross_Amount,

    ROUND(
        COALESCE(orditem.discount_amount, 0),
        2
    ) AS Total_Discount,

    ROUND(
        (orditem.quantity * orditem.unit_price)
        - COALESCE(orditem.discount_amount, 0),
        2
    ) AS Net_Amount,

    ROUND(
        orditem.quantity * orditem.unit_price,
        2
    ) AS Total_Price,

    ROUND(
        (
            (orditem.quantity * orditem.unit_price)
            - COALESCE(orditem.discount_amount, 0)
        ) / NULLIF(orditem.quantity, 0),
        2
    ) AS Net_Unit_Price

from  retailraw.bronze.OrdersMain ord
left join retailgold.retail.factcustomerHistory  cust on ord.customer_id = cust.CustomerID and cust.`__END_AT` is  null
left join retailraw.bronze.raworderitems orditem on ord.order_id = orditem.order_id

left join retailgold.retail.dimsilverproduct prod on orditem.product_id = prod.PRODUCT_ID;

-- COMMAND ----------

select * from retailgold.retail.dimDate

-- COMMAND ----------

select * from retailraw.bronze.RawOrderItems
where order_id = 25


-- COMMAND ----------

select * from retailraw.bronze.OrdersMain
where order_id = 25

-- COMMAND ----------

select * from retailraw.bronze.payment

-- COMMAND ----------

describe history retailraw.bronze.Customer

-- COMMAND ----------

select * from retailgold.retail.factcustomerHistory
where first_name = 'Delphine'
and last_name  = 'Rossi'
and `__END_AT` is  null
--ORDER BY CustomerID,EFFECTIVE_START_DATE asc

-- COMMAND ----------

update retailraw.bronze.Customer
set marketing_opt_in = 'false'
where first_name = 'Delphine'
and last_name  = 'Rossi'

-- COMMAND ----------

select * from retailraw.bronze.Customer
where first_name = 'Delphine'
and last_name  = 'Rossi'

-- COMMAND ----------

select distinct SIZE from retailgold.retail.dimSilverProduct
--where Sku = 'SKU-000002'

-- COMMAND ----------

select * from retailraw.bronze.product 
where Sku = 'SKU-000002'

-- COMMAND ----------

delete from retailraw.bronze.product 
where Sku = 'SKU-000002'

-- COMMAND ----------

UPDATE retailraw.bronze.product 
set unit_price = 0.02
where  Sku = 'SKU-000002'

-- COMMAND ----------

select * from retailraw.bronze.product 
where Sku = 'SKU-000002'

-- COMMAND ----------

SELECT * FROM retailraw.bronze.product_cdc_source_mv
where Sku = 'SKU-000002'


-- COMMAND ----------

use catalog retailgold;
use schema retail;
list tables;

-- COMMAND ----------

select * from retailgold.retail.dimSilverProduct
where Sku = 'SKU-000002'

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE trips_silver_staging
(
    CONSTRAINT valid_date EXPECT (YEAR(business_date) >= 2020),
    CONSTRAINT valid_driver_rating EXPECT (driver_rating BETWEEN 1 AND 10),
    CONSTRAINT valid_passenger_rating EXPECT (passenger_rating BETWEEN 1 AND 10)
)
COMMENT 'Transformed trips data ready for CDC upsert';

AS
SELECT
    trip_id AS id,
    CAST(date AS DATE) AS business_date,
    city_id,
    LOWER(passenger_type) AS passenger_category,
    distance_travelled_km AS distance_kms,
    fare_amount AS sales_amt,
    passenger_rating,
    driver_rating,
    ingest_datetime AS bronze_ingest_timestamp,
    CURRENT_TIMESTAMP() AS silver_processed_timestamp
FROM STREAM transportation.bronze.trips;