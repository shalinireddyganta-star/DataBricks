CREATE OR REFRESH MATERIALIZED VIEW product_cdc_source_mv
COMMENT 'Transformed Product data with category information'
AS
SELECT
    prod.product_id AS PRODUCT_ID,
    prod.sku AS SKU,
    prod.product_name AS ProductName,
    prodcat.category_name AS CategoryName,
    prodcat.parent_category_id AS ParentCategory,
    prod.category_id AS Category,
    prod.unit_price AS UnitPrice,
    prod.unit_cost AS UnitCost,
    (prod.unit_price + prod.unit_cost) AS TotalCost,
    prod.discontinued_at AS DiscontinueDate,
    9 AS SIZE,
    CASE
        WHEN prod.discontinued_at IS NULL THEN 'N'
        ELSE 'Y'
    END AS DiscontinuedFlag,
    '123' AS ETL_JOB_ID,
    'RetailLakeFlowPipelines' AS ETL_JOB_NAME,
    CURRENT_TIMESTAMP() AS change_timestamp
FROM ${Bronzecatalog}.${Bronzeschema}.product prod
LEFT JOIN ${Bronzecatalog}.${Bronzeschema}.productcategories prodcat
    ON prod.category_id = prodcat.category_id;

CREATE OR REFRESH STREAMING TABLE retailgold.retail.dimSilverProduct
COMMENT 'Silver Product table with SCD Type 1 CDC'
TBLPROPERTIES (
    'quality' = 'silver',
    'layer' = 'silver',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

CREATE FLOW product_silver_cdc_flow
AS AUTO CDC INTO retailgold.retail.dimSilverProduct
FROM SNAPSHOT (SELECT * FROM product_cdc_source_mv)
WITH VERSION (
    SELECT version FROM (
        SELECT MAX(change_timestamp) AS version FROM product_cdc_source_mv
    )
    WHERE (
        NOT EXISTS (SELECT 1 FROM last_snapshot_version())
        OR version > (SELECT version FROM last_snapshot_version())
    )
)
KEYS (PRODUCT_ID)
STORED AS SCD TYPE 1;