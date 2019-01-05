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

	choosevolume=DT.select("inputs_input_pubkey_base58","outputs_output_pubkey_base58","outputs_output_satoshis").sort(desc("outputs_output_satoshis"))# find out each record of satoshis<56839628
	choosevolume=choosevolume.where("outputs_output_satoshis<=56839628")
	choosevolume=choosevolume.selectExpr("inputs_input_pubkey_base58 as in_address","outputs_output_pubkey_base58 as out_address","outputs_output_satoshis as out_satoshis")
	choosevolume.show()

	inavg = DT.groupBy("inputs_input_pubkey_base58").count().sort(desc("count"))
	numinavg=inavg.select(func.avg("count")).collect()
	print(numinavg)
	
	numout = DT.groupBy("outputs_output_pubkey_base58").count().sort(desc("count"))
	numoutavg=numout.select(func.avg("count")).collect()
	print(numoutavg)

	numin = choosevolume.groupBy("in_address").count().sort(desc("count")) #count user's in transaction number
	numin = numin.where("count<=6")
	numin = numin.selectExpr("in_address as address","count as incount")
	numin.show()
	
	numout = choosevolume.groupBy("out_address").count().sort(desc("count")) #count user's out transaction number
	numout = numout.where("count<=4")
	numout = numout.selectExpr("out_address as address","count as outcount")
	numout.show()
	

	#volume = DT.groupBy("inputs_input_pubkey_base58").agg(func.max("outputs_output_satoshis"),func.min("outputs_output_satoshis"),func.avg("outputs_output_satoshis"), func.sum("outputs_output_satoshis")).sort(desc("sum(outputs_output_satoshis)"))	#count user's total amount
	#volume = DT.groupBy("inputs_input_pubkey_base58").sum("outputs_output_satoshis").sort(desc("sum(outputs_output_satoshis)")) #sum satashis per in_address
	#volume.show()

	volumeavg=DT.select(func.avg("outputs_output_satoshis")).collect()
	print(volumeavg)

	personal=numin.select('address').intersect(numout.select('address')) #in<=4 and out<=6 and satoshi<=5683962
	personal.show()
	numofpersonal=personal.count()
	print(numofpersonal)

	spark.stop()
	print('Done!')



