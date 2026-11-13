
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

FROM STREAM(${Bronzecatalog}.${Bronzeschema}) pay;


CREATE OR REPLACE STREAMING TABLE retailgold.retail.FactPayment
COMMENT 'Silver FactPayment with SCD Type 1 CDC'
TBLPROPERTIES (
    'quality' = 'GOLD',
    'layer' = 'GOLD',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

CREATE OR  REFRESH  FLOW FactPayment_cdc_flow AS

AUTO CDC INTO retailgold.retail.FactPayment

FROM STREAM(retailgold.retail.SilverPayment_Source)

KEYS (Paymentkey)

SEQUENCE BY PaidAT

COLUMNS * EXCEPT (PaidAT)

STORED AS SCD TYPE 1;