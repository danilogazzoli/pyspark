from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("36.Marvel-Names.txt")

lines = spark.read.text("36.Marvel-Graph.txt")

# Small tweak vs. what's shown in the video: we trim whitespace from each line as this
# could throw the counts off by one.

#cria  a coluna ID a partir do primeiro campo do split 
#função size faz a contagem de ocorrências - 1 (despreza a primeira que é o id)
#soma as conexões agrupadas por id
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
# agrega pela função, pegando o primeiro.    
minConnectionCount = connections.agg(func.min("connections")).first()[0]

#filtra as conexoes que forem iguais à mínima
minConnections = connections.filter(func.col("connections") == minConnectionCount)

#faz o join pelo id para buscar os nomes
minConnectionsWithNames = minConnections.join(names, "id")

print("The following characters have only " + str(minConnectionCount) + " connection(s):")

minConnectionsWithNames.select("name").show()