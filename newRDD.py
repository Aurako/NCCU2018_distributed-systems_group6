# -*- coding: utf8 -*-
import datetime
import pyspark
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum

if __name__ == '__main__':
	spark = pyspark.SparkContext()
	sqlContext = pyspark.SQLContext(spark)
	CSV = "gs://test1111bucket/demo1G.csv"
	data = spark.textFile(CSV, use_unicode=False).map(lambda x:x.replace('"', "")).map(lambda x:x.split(","))
	DT = sqlContext.createDataFrame(data = data.filter(lambda x:x[0]!='DTime'), schema=data.filter(lambda x:x[0]=='DTime').collect()[0])
	DT = DT.withColumn('outputs_output_satoshis', DT['outputs_output_satoshis'].cast(IntegerType()))
	DT.persist()

	numin = DT.groupBy("inputs_input_pubkey_base58").count().sort(desc("count")) #count user's transaction number
	numin.show()
	numinavg=numin.select(func.avg("count")).collect()
	print(numinavg)
	
	numout = DT.groupBy("outputs_output_pubkey_base58").count().sort(desc("count")) #count user's transaction number
	numout.show()
	numoutavg=numout.select(func.avg("count")).collect()
	print(numoutavg)

	volume = DT.groupBy("inputs_input_pubkey_base58").agg(func.max("outputs_output_satoshis"),func.min("outputs_output_satoshis"),func.avg("outputs_output_satoshis"), func.sum("outputs_output_satoshis")).sort(desc("sum(outputs_output_satoshis)"))	#count user's total amount
	volume.show()
	volumeavg=DT.select(func.avg("outputs_output_satoshis")).collect()
	print(volumeavg)




	spark.stop()
	print('Done!')



