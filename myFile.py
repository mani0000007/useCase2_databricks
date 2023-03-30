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




