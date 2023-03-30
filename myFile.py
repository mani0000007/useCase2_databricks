df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Sheet1'!") \
    .load('dbfs:/FileStore/tables/btd.xlsx')

 df.write.format('delta').saveAsTable('btd')   

=========================================================================
Problem: 1
=========================================================================

%sql
select count(*) as total_terminations from btd where terminationdate_key like '%2020%';

===========================================================================
Problem:2
===========================================================================

%sql
select count(*)+count(*)*(7/100) as Projection_termination_of_2021 from btd where year(terminationdate_key)=2020;


======================================================================
Problem: 3
======================================================================

from pyspark.sql.types import DecimalType, StructType, StructField
from decimal import Decimal

schema = StructType([StructField("Rate", DecimalType(4, 1), True)])

seq = []
for i in range(4, 11):
    if i < 10:
        seq.append((Decimal(i + 0.0),))
        seq.append((Decimal(i + 0.5),))
    else:
        seq.append((Decimal(i + 0.0),))

spark_df = spark.createDataFrame(seq, schema)
spark_df.write.format('delta').saveAsTable('rates')


%sql
DROP TABLE IF EXISTS projected_terminations;
CREATE TABLE projected_terminations (
  Rate DECIMAL(4,1), 
  Expected_Termination_in_2021 DECIMAL(10,3)
);


INSERT INTO projected_terminations (Rate, Expected_Termination_in_2021)
SELECT r.Rate, COUNT(*) + COUNT(*) * (r.Rate / 100) AS Expected_Termination_in_2021
FROM btd
CROSS JOIN rates r
WHERE YEAR(terminationdate_key) = 2020
GROUP BY r.Rate;

==============================================================================
D)	What would be the projected terminations if it is expected to grow by 4.5%? 
================================================================================

%sql
select count(*)+count(*)*4.5/100 as projected_terminations from btd where YEAR(terminationdate_key) = 2020


# =====================================================================================================================
E)	Create a summary table to show the total termination for each termination reason (Retirement, Resignation, Layoff),       
•	Which one is the highest? 
==========================================================================================================================

%sql
select termreason_desc, count(*) as Total_Terminations from btd group by termreason_desc order by Total_Terminations desc limit 1;

============================================================================================
F)	Brightness is thinking of introducing a categorization of contract terminations, if the termination was a retirement or a resignation then it is a “Voluntary” termination, if it was a layoff then it is an “Involuntary” termination.
a.	Based on the above description Fill out the termtype_desc column.	
b.	What percentage of total terminations were voluntary?		________
===========================================================================================

%sql
SELECT (voluntary_count * 100 / total_count) AS Percentage_Voluntary_Terminations
FROM (
  SELECT COUNT(*) AS total_count, 
         COUNT(*) FILTER (WHERE termtype_desc = 'Voluntary') AS voluntary_count
  FROM btd
)


=====================================================================================================
G)	You are to study the experience level of terminated employees based on the following rough classification.									
•	If the employee has served for 5 years or less then he/she is a “Newcomer”
•	If the employee has served between 6 to 10 years, then he/she is a “Proficient”
•	If the employee has served for more than 10 years, the he/she is an “Expert’

======================================================================================================



%sql
SELECT 
  CASE 
    WHEN DATEDIFF(terminationdate_key, hiredate_key)/365 <= 5 THEN 'Newcomer'
    WHEN DATEDIFF(terminationdate_key, hiredate_key)/365 BETWEEN 6 AND 10 THEN 'Proficient'
    ELSE 'Expert'
  END AS Experience_Level,
  COUNT(*) AS Total_Terminations
FROM btd
GROUP BY Experience_Level;


