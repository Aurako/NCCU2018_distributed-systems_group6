
spark-submit newRDD.py

## GraphFrame安裝
gsutil cp gs://fen_san_shi_xi_tong/graphframes-0.7.0-spark2.3-s_2.11.jar ./
jar xf graphframes-0.7.0-spark2.3-s_2.11.jar
cd graphframes
zip graphframes.zip -r *
cp graphframes.zip /home/gekkarei/
sudo vi /etc/spark/conf/spark-env.sh
>>export PYTHONPATH=$PYTHONPATH:/home/gekkarei/graphframes.zip:.
pyspark --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11

## 建立GraphFrame
from graphframe import *
df = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('gs://fen_san_shi_xi_tong/btc201808-*.csv')
df2 = df.withColumnRenamed('inputs_input_pubkey_base58','input').withColumnRenamed('outputs_output_satoshis','satoshis').withColumnRenamed('outputs_output_pubkey_base58','output')
df2 = df2.filter(df2['input']!='null')
df2 = df2.filter(df2['output']!='null')
relation = df2.select(df2['input'], df2['output'], df2['satoshis'])

pubkey1 = relation.select(relation['input']).dropDuplicates(['input']).withColumnRenamed('input','id')
pubkey2 = df2.select(df2['output']).dropDuplicates(['output']).withColumnRenamed('output','id')
newpubkey = pubkey1.union(pubkey2)

relation = relation.withColumnRenamed('input', 'src').withColumnRenamed('output', 'dst').withColumnRenamed('satoshis', 'relationship')
g = GraphFrame(newpubkey, relation)

## 交談型分析
g.inDegrees.sort('inDegree', ascending=False).show()
g.outDegrees.sort('outDegree', ascending=False).show()
g.vertices.count()
g.edges.sort('relationship', ascending=False).show()
g2 = g.filterEdges("relationship >= 100000000").dropIsolatedVertices()
df_ginout = g.inDegrees.join(g.outDegrees, g.inDegrees['id']==g.outDegrees['id'])

