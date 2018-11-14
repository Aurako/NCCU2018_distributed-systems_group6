# -*- coding: utf8 -*-
import datetime
import pyspark
from pyspark.sql.types import IntegerType


if __name__ == '__main__':
	spark = pyspark.SparkContext()
	sqlContext = pyspark.SQLContext(spark)
	CSV = "gs://for_ds_test100/demo_1G.csv"
	data = spark.textFile(CSV, use_unicode=False).map(lambda x:x.replace('"', "")).map(lambda x:x.split(","))
	DT = sqlContext.createDataFrame(data = data.filter(lambda x:x[0]!='DTime'), schema=data.filter(lambda x:x[0]=='DTime').collect()[0])
	DT.persist()

	DT = DT.filter(DT['DTime'] != "2018-07-31")
	result = DT.groupBy("DTime").count().sort("DTime")
	DT = DT.withColumn('outputs_output_satoshis', DT['outputs_output_satoshis'].cast(IntegerType()))

	sumDT = DT.groupBy("DTime").sum('outputs_output_satoshis').sort("DTime").withColumnRenamed('sum(outputs_output_satoshis)', 'Sum_satoshis')

	finalresult = result.join(sumDT ,'DTime', 'inner')
	finalresult = finalresult.withColumn("Sum_BTC", finalresult.Sum_satoshis / 100000000)

	finalresult = finalresult.drop('Sum_satoshis').sort("DTime")
	finalresult.show()

	spark.stop()
	print('Done!')



