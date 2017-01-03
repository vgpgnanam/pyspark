# pyspark

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

dataRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/departments")
for i in dataRDD.take(10): print(i)

dataRDD.saveAsTextFile("/user/gnanaprakasam/pyspark/departmentTesting")

spark-submit --master local saveFile.py

spark-submit --master yarn saveFile.py

# save as sequencefile

dataRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/departments")
dataRDD.map(lambda rec: (None, rec)).saveAsSequenceFile("/user/gnanaprakasam/pyspark/departmentSeq")
dataRDDSeq = dataRDD.map(lambda rec: (None, rec))
for i in dataRDDSeq.take(10): print(i)


dataRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/departments")
dataRDD.map(lambda rec: tuple(rec.split("|", 1))).saveAsSequenceFile("/user/gnanaprakasam/pyspark/departmentSeqKey")

# saveasNewAPIHadoopFile

path="/user/gnanaprakasam/pyspark/departmentSeqNewAPI"
dataRDD.map(lambda x: tuple(x.split("|", 1))).saveAsNewAPIHadoopFile(
path,"org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",keyClass="org.apache.hadoop.io.Text",valueClass="org.apache.hadoop.io.Text")

# reading sequenceFile
dataRDD = sc.sequenceFile("/user/gnanaprakasam/pyspark/departmentSeq")
for i in dataRDD.take(10): print(i)

dataRDD = sc.sequenceFile("/user/gnanaprakasam/pyspark/departmentSeqNewAPI","org.apache.hadoop.io.IntWritable","org.apache.hadoop.io.Text")
for i in dataRDD.take(10): print(i)

# Code snippet to read data from hive tables in hive context.

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
depts = sqlContext.sql("select * from departments")
for i in depts.take(10): print(i)







#Pyspark

Filter
#Get all the orders with status COMPLETE

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
ordersCompleted = ordersRDD.filter(lambda x: x.split(",")[3] == 'COMPLETE')

for i in ordersCompleted.take(10): print(i)

# Get all the orders where status contains the word PENDING

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
ordersPending = ordersRDD.filter(lambda x: x.split(",")[3] in 'PENDING')
for i in ordersPending.take(10): print(i)

# Get all the orders where order_id is greater than 100

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
orderGT100 = ordersRDD.filter(lambda x: int(x.split(",")[0]) > 100)
for i in orderGT100.take(10): print(i)

#Boolean operation - or
#Get all the orders where order_id > 100 or order_status is in one of the pending states

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
ordersGT100RPending = ordersRDD.filter(lambda x: int(x.split(",")[0]) > 100 or x.split(",")[3] in 'PENDING')
for i in ordersGT100RPending.take(100): print(i)

# Get order id > 1000 and (Status PENDING or cancelled)

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
ordersGT100PenRCan = ordersRDD.filter(lambda x: (int(x.split(",")[0]) > 100 and
  (x.split(",")[3] in 'PENDING' or x.split(",")[3] == 'CANCELED')))
for i in ordersGT100PenRCan.take(100): print(i)

# Get distinct order status from orders table
ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
orderstatus = ordersRDD.filter(lambda x: x.split(",")[3]).distinct()
for i in orderstatus.take(10): print(i)

# Orders > 1000 and staus other than COMPLETE

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
orderGT100Notcomplete = ordersRDD.filter(lambda x: int(x.split(",")[0]) > 1000 and
  x.split(",")[3] != 'COMPLETE')
for i in orderGT100Notcomplete.take(10): print(i)

# Check if there are cancelled orders with amount greater than 1000$

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
orderItemsRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/order_items")

ordersCancelled = ordersRDD.filter(lambda x: x.split(",")[3] == 'CANCELED')
ordersParsed = ordersCancelled.map(lambda x: (int(x.split(",")[0]), x))

orderItemsParsed = orderItemsRDD.map(lambda x: (int(x.split(",")[1]), x.split(",")[4]))

orderItemsJoinorders = orderItemsParsed.join(ordersParsed)

revenuePerOrder = orderItemsJoinorders.map(lambda x: (int(x[0]), float(x[1][0].split(",")[0])))

revenuePerOrderreduceByKey = revenuePerOrder.reduceByKey(lambda x, y: x + y)

revenuePerOrderFilter = revenuePerOrderreduceByKey.filter(lambda x: x[1] > 1000)

for i in revenuePerOrderFilter.take(10): print(i)


# Sorting and Ranking using Global Key

ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
ordersMap = ordersRDD.map(lambda x: (int(x.split(",")[0]), x)).sortByKey()
for i in ordersMap.take(10): print(i)

for i in ordersRDD.map(lambda x: (int(x.split(",")[0]), x)).sortByKey().take(5): print(i)
for i in ordersRDD.map(lambda x: (int(x.split(",")[0]), x)).sortByKey(False).take(5): print(i)

for i in ordersRDD.map(lambda x: (int(x.split(",")[0]), x)).takeOrdered(5, lambda x: x[0]): print(i)
for i in ordersRDD.map(lambda x: (int(x.split(",")[0]), x)).takeOrdered(5, lambda x: -x[0]): print(i)

for i in ordersRDD.takeOrdered(5, lambda x: int(x.split(",")[0])): print(i)
for i in ordersRDD.takeOrdered(5, lambda x: -int(x.split(",")[0])): print(i)

# Sort products by price with in each category

productsRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/products")
productsMap = productsRDD.map(lambda x: (x.split(",")[1], x))
productsGroupBy = productsMap.groupByKey()
for i in productsGroupBy.map(lambda x: sorted(x[1], key=lambda k: float(k.split(",")[4]))).take(10): print(i)

for i in productsGroupBy.map(lambda x: sorted(x[1], key=lambda k: float(k.split(",")[4]), reverse=True)).take(10): print(i)

def getTopDenseN(rec, topN):
  x = [ ]
  topNPrices = [ ]
  prodPrices = [ ]
  prodPricesDec = [ ]
  for i in rec[1]:
    prodPrices.append(float(i.split(",")[4]))
  prodPricesDesc = list(sorted(set(prodPrices), reverse=True))
  import itertools
  topNprices = list(itertools.islice(prodPricesDesc, 0, topN))
  for j in sorted(rec[1], key=lambda K: float(split(",")[4]), reverse=True):
    if (float(j.split(",")[4]) in topNPrices):
      x.append(j)
  return (y for y in x)


products = sc.textFile("/user/gnanaprakasam/sqoop_import/products")
productsMap = products.map(lambda x: (x.split(",")[1], x))

for i in productsMap.groupByKey().flatMap(lambda x: getTopDenseN(x, 2)).take(10): print(i)


def getTopDenseN(rec, topN):
  x = [ ]
  topNPrices = [ ]
  prodPrices = [ ]
  prodPricesDesc = [ ]
  for i in rec[1]:
    prodPrices.append(float(i.split(",")[4]))
  prodPricesDesc = list(sorted(set(prodPrices), reverse=True))
  import itertools
  topNPrices = list(itertools.islice(prodPricesDesc, 0, topN))
  for j in sorted(rec[1], key=lambda k: float(k.split(",")[4]), reverse=True):
    if(float(j.split(",")[4]) in topNPrices):
      x.append(j)
  return (y for y in x)


products = sc.textFile("/user/gnanaprakasam/sqoop_import/products")
productsMap = products.map(lambda rec: (rec.split(",")[1], rec))

for i in productsMap.groupByKey().flatMap(lambda x: getTopDenseN(x, 2)).collect(): print(i)
