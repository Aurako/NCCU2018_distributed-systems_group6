# -*- coding: utf8 -*-
import datetime
import pyspark
import pandas as pd
import os
from pyspark.sql.types import IntegerType
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType

if __name__ == '__main__':
	spark = pyspark.SparkContext()
	sqlContext = pyspark.SQLContext(spark)
	#sqlContext = SQLContext(sc)
	
	CSV = "gs://test1111bucket/*.csv"
	
	datas = spark.textFile(CSV, use_unicode=False).map(lambda x:x.replace('"', "")).map(lambda x:x.split(","))
	
	#DT = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(["gs://test1111bucket/*.csv"])
	

	DT = sqlContext.createDataFrame(data = datas.filter(lambda x:x[0]!='DTime'), schema=datas.filter(lambda x:x[0]=='DTime').collect()[0])

	func =  udf (lambda x: datetime.strptime(x, '%d/%m/%Y'), DateType())
	df = DT.withColumn('date', func(col('DTime')))
	df.show()

	df = df.filter(df['date'] != "2018-07-31")
	result = df.groupBy("date").count().sort("date")
	df = df.withColumn('outputs_output_satoshis', df['outputs_output_satoshis'].cast(IntegerType()))

	reg=df.groupBy("date").max('outputs_output_satoshis').sort("date")
	reg.show()

	df.persist()


	sumDT = df.groupBy("date").sum('outputs_output_satoshis').sort("date").withColumnRenamed('sum(outputs_output_satoshis)', 'Sum_satoshis')

	sumDT.show() #

	finalresult = result.join(sumDT ,'date', 'inner')
	finalresult = finalresult.withColumn("Sum_BTC", finalresult.Sum_satoshis / 100000000)

	finalresult = finalresult.drop('Sum_satoshis').sort("date")
	finalresult.show()

	spark.stop()
	print('Done!')



