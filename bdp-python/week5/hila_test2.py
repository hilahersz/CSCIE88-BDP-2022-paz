from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("HilaTest").getOrCreate()

df = ss.read.csv('sample_data')
df2 = df.rdd.map(lambda s: (s[1][0:13], s[2], 1))
df3 = df2.distinct()
df4 = df3.map(lambda s: (s[0], 1))
final = df4.reduceByKey(lambda a, b: a + b)

dataColl = final.collect()
for row in dataColl[:10]:
    print(row[0] + "," + str(row[1]))

