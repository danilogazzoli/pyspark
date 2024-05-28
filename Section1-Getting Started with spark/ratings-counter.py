from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

sc = SparkContext(conf = conf)

sc.setLogLevel('ERROR')
print('*'*100)
print(type(sc))
print('*'*100)

lines = sc.textFile("file:///home/danilo/projetos/pyspark/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
