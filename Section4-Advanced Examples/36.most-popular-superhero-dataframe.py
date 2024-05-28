from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("36.Marvel-Names.txt")

lines = spark.read.text("36.Marvel-Graph.txt")
print('*'*100)
print(lines.take(10)) #registros com "value" como nome da coluna.
#output: [Row(value='5988 748 1722 3752 4655 5743 1872 3413 5527 6368 6085 4319 4728 1636 2397 3364 4001 1614 1819 1585 732 2660 3952 2507 3891 2070 2239 2602 612 1352 5447 4548 1596 5488 1605 5517 11 479 2554 2043 17 865 4292 6312 473 534 1479 6375 4456 '), Row(value='5989 4080 4264 4446 3779 2430 2297 6169 3530 3272 4282 6432 2548 4140 185 105 3878 2429 1334 4595 2767 3956 3877 4776 4946 3407 128 269 5775 5121 481 5516 4758 4053 1044 1602 3889 1535 6038 533 3986 '), Row(value='5982 217 595 1194 3308 2940 1815 794 1503 5197 859 5096 6039 2664 651 2244 528 284 1449 1097 1172 1092 108 3405 5204 387 4607 4545 3705 4930 1805 4712 4404 247 4754 4427 1845 536 5795 5978 533 3984 6056 '), Row(value='5983 1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485 '), Row(value='5980 2731 3712 1587 6084 2472 2546 6313 875 859 323 2664 1469 522 2506 2919 2423 3624 5736 5046 1787 5776 3245 3840 2399 '), Row(value='5981 3569 5353 4087 2653 2058 2218 5354 5306 3135 4088 4869 2958 2959 5732 4076 4155 291 '), Row(value='5986 2658 3712 2650 1265 133 4024 6313 3120 6066 3546 403 545 4860 4337 2295 5467 128 2399 5999 5516 5309 4731 2557 5013 4132 5306 5615 2397 945 533 5694 824 1383 3771 592 5017 704 3778 1127 1480 274 5768 6148 4204 5250 4804 1715 2069 2548 525 2664 520 522 4978 6306 1259 5002 449 2449 1231 3662 3950 2603 2931 3319 3955 3210 5776 5088 2273 5576 1649 518 1535 3356 5874 5973 1660 4359 4188 2614 2613 3594 3805 3750 331 3757 1347 4366 66 2199 3296 3008 1425 3454 1638 1587 731 183 2 2689 505 5021 2629 5834 4441 2184 4607 4603 5716 969 867 6196 604 2438 155 2430 3632 5446 5696 4454 3233 6227 1116 1177 563 2728 5736 4898 859 5535 5046 2971 1805 1602 1289 3220 4589 3989 5931 3986 1369 '), Row(value='5987 2614 5716 1765 1818 2909 6436 1587 6451 5661 4069 1962 66 6084 6148 2051 4045 6005 6419 6432 3626 2119 6417 5707 2987 3388 6438 2666 1380 6066 725 1469 2508 5905 5332 6059 2107 197 6057 3762 2467 2723 2810 5099 3935 208 2422 611 713 1331 1330 2183 4621 5736 1420 5323 2449 2506 3724 2452 2350 58 1645 1647 1554 3354 1766 2896 4070 53 5815 4480 4898 260 4978 5388 875 827 729 2028 820 2373 '), Row(value='5984 590 4898 745 3805 2650 2430 3757 4756 5744 4650 2093 3079 491 2201 2204 6112 4827 874 859 2664 2623 1314 3956 2589 6066 4629 4839 1805 4933 2557 5716 6295 2310 5306 2311 6313 4454 6296 6030 2354 2399 '), Row(value='5985 3233 2254 212 2023 2728 2557 3805 5716 2650 1265 3594 4654 6359 4454 2249 5874 2184 4236 505 ')]
print('*'*100)
# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
#cria  a coluna ID a partir do primeiro campo do split 
#função size faz a contagem de ocorrências - 1 (despreza a primeira que é o id)
#soma as conexões agrupadas por id
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
print(connections.take(10))    
#ouput: [Row(id='691', connections=6), Row(id='1159', connections=11), Row(id='3959', connections=142), Row(id='1572', connections=35), Row(id='2294', connections=14), Row(id='1090', connections=4), Row(id='3606', connections=171), Row(id='3414', connections=7), Row(id='296', connections=17), Row(id='4821', connections=16)]

print('*'*100)
mostPopular = connections.sort(func.col("connections").desc()).first()
print(mostPopular)    

print('*'*100)
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()
#ouput: Row(id='859', connections=1933)

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

