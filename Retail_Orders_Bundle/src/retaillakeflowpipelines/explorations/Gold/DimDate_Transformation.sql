CREATE OR REPLACE MATERIALIZED VIEW
retailgold.retail.dimDate
AS
WITH date_range AS (
    SELECT explode(
        sequence(
            DATE('2024-01-01'),
            DATE('2027-12-31'),
            INTERVAL 1 DAY
        )
    ) AS full_date
)
SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    date_format(full_date, 'MMMM') AS month_name,
    DAY(full_date) AS day,
    DAYOFWEEK(full_date) AS day_of_week,
    date_format(full_date, 'EEEE') AS day_name,
    WEEKOFYEAR(full_date) AS week_of_year,
    CASE
        WHEN DAYOFWEEK(full_date) IN (1, 7)
        THEN TRUE ELSE FALSE
    END AS is_weekend
FROM date_range;

create or replace MATERIALIZED VIEW
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

from  ${Bronzecatalog}.${Bronzeschema}.OrdersMain ord
left join retailgold.retail.factcustomerHistory  cust on ord.customer_id = cust.CustomerID and cust.`__END_AT` is  null
left join ${Bronzecatalog}.${Bronzeschema}raworderitems orditem on ord.order_id = orditem.order_id

left join retailgold.retail.dimsilverproduct prod on orditem.product_id = prod.PRODUCT_ID;

CREATE OR REFRESH STREAMING TABLE retailgold.retail.FactOrderItem
COMMENT 'Silver FactOrderItem with SCD Type 1 CDC'
TBLPROPERTIES (
    'quality' = 'GOLD',
    'layer' = 'GOLD',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

CREATE OR REFRESH FLOW FOrderItemCDC_flow
AS AUTO CDC INTO retailgold.retail.FactOrderItem
FROM SNAPSHOT (SELECT * FROM  retailgold.retail.FOrderItemCDC)
WITH VERSION (
    SELECT version FROM (
        SELECT MAX(change_timestamp) AS version FROM retailgold.retail.FOrderItemCDC
    )
    WHERE (
        NOT EXISTS (SELECT 1 FROM last_snapshot_version())
        OR version > (SELECT version FROM last_snapshot_version())
    )
)
KEYS (OrderID,OrderItemID,ProductID)
STORED AS SCD TYPE 1;