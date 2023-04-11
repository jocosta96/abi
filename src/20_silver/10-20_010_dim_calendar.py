# Databricks notebook source
from pyspark.sql.functions import explode, sequence, to_date

beginDate = '2006-01-01'
endDate = '2100-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table 20_silver.dim_calendar
# MAGIC using delta
# MAGIC location '/mnt/20_silver/dim_calendar'
# MAGIC as select
# MAGIC 
# MAGIC   date_format(calendarDate, 'yyyyMMdd') as calendar_id,
# MAGIC   
# MAGIC   date_format(calendarDate, 'yyyyMM') as montly_calendar_id,
# MAGIC   
# MAGIC   calendarDate as calendar_date,
# MAGIC   
# MAGIC   year(calendarDate) AS calendar_year,
# MAGIC   
# MAGIC   date_format(calendarDate, 'MMMM') as calendar_month,
# MAGIC   
# MAGIC   month(calendarDate) as calendar_month_of_year,
# MAGIC   
# MAGIC   date_format(calendarDate, 'EEEE') as calendar_day,
# MAGIC   
# MAGIC   dayofweek(calendarDate) as calendar_day_of_week,
# MAGIC   
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as calendar_is_week_day,
# MAGIC   
# MAGIC   dayofmonth(calendarDate) as calendar_day_of_month,
# MAGIC   
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as calendar_is_last_day_of_month,
# MAGIC   
# MAGIC   dayofyear(calendarDate) as calendar_day_of_year,
# MAGIC   
# MAGIC   weekofyear(calendarDate + 1) as calendar_week_of_year,
# MAGIC   
# MAGIC   dense_rank() over(partition by year(calendarDate), month(calendarDate) order by weekofyear(calendarDate + 1)) as calendar_period,
# MAGIC   
# MAGIC   quarter(calendarDate) as calendar_quarter_of_year
# MAGIC      
# MAGIC from
# MAGIC   dates

# COMMAND ----------

(

  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 month)) as calendarDate")

    .createOrReplaceTempView('montly_dates')

)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table 20_silver.dim_montly_calendar
# MAGIC using delta
# MAGIC location '/mnt/20_silver/dim_montly_calendar'
# MAGIC as select
# MAGIC 
# MAGIC   date_format(calendarDate, 'yyyyMM') as montly_calendar_id,
# MAGIC   
# MAGIC   year(calendarDate) AS calendar_year,
# MAGIC   
# MAGIC   date_format(calendarDate, 'MMMM') as calendar_month,
# MAGIC   
# MAGIC   month(calendarDate) as calendar_month_of_year,
# MAGIC   
# MAGIC   quarter(calendarDate) as calendar_quarter_of_year
# MAGIC   
# MAGIC from montly_dates

# COMMAND ----------


