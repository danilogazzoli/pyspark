import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

input = sc.textFile("19.Book.txt")
words = input.flatMap(normalizeWords)

# o map abaixo acrescenta o inteiro 1 para cada palavra e o reducebykey mantém a palavra porém faz a soma destes inteiros
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

#finalmente chama a ordenação
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(False)
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
