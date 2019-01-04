# -*- coding: utf8 -*-
import datetime
import pyspark
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

	num = DT.groupBy("inputs_input_pubkey_base58").count().sort(desc("count")) #count user's transaction number
	num.show()
	
	volume = DT.groupBy("inputs_input_pubkey_base58").sum('outputs_output_satoshis').sort(desc("sum(outputs_output_satoshis)"))	
	volume.show()

	spark.stop()
	print('Done!')



