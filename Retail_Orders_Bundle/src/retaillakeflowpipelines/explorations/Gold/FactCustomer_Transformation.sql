CREATE OR REFRESH MATERIALIZED VIEW customer_cdc_source_mv
COMMENT 'Customer MATERIALIZED VIEW  '
AS SELECT customer_id as CustomerID,
first_name as FIRST_NAME,
last_name AS Last_Name,
email as  Email,
city as City,
country as Country,
signup_date as SignupDate,
loyalty_tier as LoyalityTier,
marketing_opt_in as MarketingOptIn,
current_timestamp() AS EFFECTIVE_START_DATE,
 CAST('9999-12-31 23:59:59' AS TIMESTAMP)  AS EFFECTIVE_END_DATE,
'123' AS ETL_JOB_ID,
'RetailLakeFlowPipelines' AS  ETL_JOB_NAME,
current_timestamp() AS change_timestamp
FROM ${Bronzecatalog}.${Bronzeschema}.customer;



CREATE OR REFRESH STREAMING TABLE retailgold.retail.factcustomerHistory
COMMENT 'Gold Customer table with SCD Type 2 CDC'
TBLPROPERTIES (
    'quality' = 'gold',
    'layer' = 'gold',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

CREATE OR REFRESH FLOW target_snapshot_flow AS
AUTO CDC INTO retailgold.retail.factcustomerHistory
FROM SNAPSHOT (
  SELECT CustomerID, FIRST_NAME, Last_Name, Email, City, Country, SignupDate, LoyalityTier, MarketingOptIn, ETL_JOB_ID, ETL_JOB_NAME
  FROM customer_cdc_source_mv
)
WITH VERSION (
   SELECT version FROM (
        SELECT MAX(change_timestamp) AS version FROM customer_cdc_source_mv
    )
  WHERE (
    NOT EXISTS (SELECT 1 FROM last_snapshot_version())
    OR version > (SELECT version FROM last_snapshot_version())
  )
)
KEYS (CustomerID)
STORED AS SCD TYPE 2;
