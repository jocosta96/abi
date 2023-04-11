-- Databricks notebook source
WITH cte_volume AS (

  SELECT

        C.bottler_origin_name
      , B.trade_group_desc
      , SUM(volume) AS volume

    FROM 30_gold.fact_montly_beverage_sales A

    INNER JOIN 30_gold.dim_trade_channel B
    ON A.trade_channel_id = B.trade_channel_id

    INNER JOIN 30_gold.dim_bottler_origin C
    ON A.bottler_origin_id  = C.bottler_origin_id

    GROUP BY
        C.bottler_origin_name
      , B.trade_group_desc
      
)

SELECT

  bottler_origin_name, trade_group_desc, ROUND(volume, 2) AS volume
  
FROM (
  SELECT
  *
  , ROW_NUMBER() OVER(PARTITION BY bottler_origin_name ORDER BY volume DESC) AS position

  FROM cte_volume
  ORDER BY bottler_origin_name, trade_group_desc, volume  DESC
)

WHERE position <= 3

-- COMMAND ----------

SELECT
    
    C.calendar_year
  , C.calendar_month_of_year
  , B.brand_name
  , ROUND(SUM(A.volume), 2) AS volume

FROM 30_gold.fact_montly_beverage_sales A

INNER JOIN 30_gold.dim_brand B
ON A.brand_id = B.brand_id

INNER JOIN 30_gold.dim_montly_calendar C
ON A.montly_calendar_id = C.montly_calendar_id

GROUP BY calendar_year, calendar_month_of_year, brand_name

ORDER BY calendar_year, calendar_month_of_year, brand_name


-- COMMAND ----------

WITH cte_low_brand AS (

  SELECT

      brand_id
    , bottler_origin_id
    , SUM(volume) AS volume

  FROM 30_gold.fact_montly_beverage_sales

  GROUP BY 1, 2

  ORDER BY 1, 2, 3
)


SELECT

     C.bottler_origin_name
   , B.brand_name

FROM (
  SELECT * , ROW_NUMBER() OVER(PARTITION BY bottler_origin_id ORDER BY volume) rn
  FROM cte_low_brand
) A

INNER JOIN 30_gold.dim_brand B
ON A.brand_id = B.brand_id

INNER JOIN 30_gold.dim_bottler_origin C
ON A.bottler_origin_id  = C.bottler_origin_id

WHERE A.rn = 1
