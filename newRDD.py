# -*- coding: utf8 -*-
import datetime
import pyspark
import pyspark.sql.functions as func

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum
from pyspark.sql.functions import col,size,isnan
from pyspark.sql.types import *
from pyspark.sql.functions import lower

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
	choosevolume=choosevolume.selectExpr("inputs_input_pubkey_base58 as pay_address","outputs_output_pubkey_base58 as receipt_address","outputs_output_satoshis as satoshis")
	#choosevolume.show()

	inavg = DT.groupBy("inputs_input_pubkey_base58").count().sort(desc("count"))
	numinavg=inavg.select(func.avg("count")).collect()
	#print(numinavg)
	
	numout = DT.groupBy("outputs_output_pubkey_base58").count().sort(desc("count"))
	numoutavg=numout.select(func.avg("count")).collect()
	#print(numoutavg)

	numin = choosevolume.groupBy("pay_address").count().sort(desc("count")) #count user's pay times
	numin = numin.where("count<=6")
	numin = numin.selectExpr("pay_address as address","count as incount")
	#numin.show()
	
	numout = choosevolume.groupBy("receipt_address").count().sort(desc("count")) #count user's receipt times
	numout = numout.where("count<=4")
	numout = numout.selectExpr("receipt_address as address","count as outcount")
	#numout.show()
	

	###volume = DT.groupBy("inputs_input_pubkey_base58").agg(func.max("outputs_output_satoshis"),func.min("outputs_output_satoshis"),func.avg("outputs_output_satoshis"), func.sum("outputs_output_satoshis")).sort(desc("sum(outputs_output_satoshis)"))	#count user's total amount
	###volume = DT.groupBy("inputs_input_pubkey_base58").sum("outputs_output_satoshis").sort(desc("sum(outputs_output_satoshis)")) #sum satashis per pay_address
	###volume.show()

	volumeavg=DT.select(func.avg("outputs_output_satoshis")).collect()
	#print(volumeavg)

	personal=numin.select('address').intersect(numout.select('address')) #pay<=4 and receipt<=6 and satoshi<=5683962
	personal.show() #table of personal address
	numofpersonal=personal.count()
	print(numofpersonal) #numbers of personal address


	pool=DT.filter((DT["outputs_output_pubkey_base58"] == "") | DT["outputs_output_pubkey_base58"].isNull() | isnan(DT["outputs_output_pubkey_base58"])).sort(desc("outputs_output_satoshis"))
	pool=DT.filter((DT["inputs_input_pubkey_base58"] == "") | DT["inputs_input_pubkey_base58"].isNull() | isnan(DT["inputs_input_pubkey_base58"])).sort(desc("outputs_output_satoshis"))
	pool=pool.filter((DT["outputs_output_satoshis"] >= 1250000000)&(DT["outputs_output_satoshis"] <= 1450000000))
	pool=pool.groupBy("outputs_output_pubkey_base58").count().sort(desc("count")) #table of mining pool address and count
	pool=pool.selectExpr("outputs_output_pubkey_base58 as address")
	pool.show() #only address
	numofpool=pool.count()
	print(numofpool) #numbers of mining pool address

	casinoout=DT.where((lower(DT["outputs_output_pubkey_base58"]).like('1dice%')) | (lower(DT["outputs_output_pubkey_base58"]).like('1lucky%'))) #|(DT.inputs_input_pubkey_base58.like('1dice%')))
	casinoout=casinoout.groupBy("outputs_output_pubkey_base58").count().sort(desc("count")) #table of casino address and count
	casinoout=casinoout.selectExpr("outputs_output_pubkey_base58 as address")
	casinoin=DT.where((lower(DT["inputs_input_pubkey_base58"]).like('1dice%')) | (lower(DT["inputs_input_pubkey_base58"]).like('1lucky%'))) #|(DT.inputs_input_pubkey_base58.like('1dice%')))
	casinoin=casinoin.groupBy("inputs_input_pubkey_base58").count().sort(desc("count"))
	casinoin=casinoin.selectExpr("inputs_input_pubkey_base58 as address")
	casino=casinoin.union(casinoout)
	casino=casino.distinct()
	casino.show() #only address
	numofcasino=casino.count()
	print(numofcasino) #numbers of casino address


	alladdress=DT.select('outputs_output_pubkey_base58').union(DT.select('inputs_input_pubkey_base58'))
	alladdress=alladdress.distinct()

	servicesandexchange=personal.union(pool).union(casino)
	servicesandexchange=servicesandexchange.distinct()
	servicesandexchange=alladdress.subtract(servicesandexchange)
	servicesandexchange=servicesandexchange.selectExpr("outputs_output_pubkey_base58 as address")
	servicesandexchange.show()
	numeofsande=servicesandexchange.count()
	print(numeofsande) #numbers of service and exchange

	spark.stop()
	print('Done!')



