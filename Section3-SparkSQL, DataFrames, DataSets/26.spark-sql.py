from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("26.fakefriends.csv")
# faz o mapeamento do arquivo csv com ID, nome, idade e número de amigos
people = lines.map(mapper)
#output: [Row(ID=0, name="b'Will'", age=33, numFriends=385), Row(ID=1, name="b'Jean-Luc'", age=26, numFriends=2), Row(ID=2, name="b'Hugh'", age=55, numFriends=221), Row(ID=3, name="b'Deanna'", age=40, numFriends=465), Row(ID=4, name="b'Quark'", age=68, numFriends=21), Row(ID=5, name="b'Weyoun'", age=59, numFriends=318), Row(ID=6, name="b'Gowron'", age=37, numFriends=220), Row(ID=7, name="b'Will'", age=54, numFriends=307), Row(ID=8, name="b'Jadzia'", age=38, numFriends=380), Row(ID=9, name="b'Hugh'", age=27, numFriends=181)]
print(people.take(10))
print('*'*10)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age BETWEEN 13 AND 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)


# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

#o mesmo acima com sql group by e filtro de idade
count_teens = spark.sql('SELECT age, count(*) as count FROM people WHERE age BETWEEN 13 AND 19 GROUP BY age ORDER BY age')
for teen in count_teens.collect():
    dict_teen = teen.asDict()
    print(f"Idade: {dict_teen['age']}, count: {dict_teen['count']}")

spark.stop()
