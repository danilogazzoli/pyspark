# -*- coding: utf-8 -*-
"""
Created on Mon Sep  7 15:28:00 2020

@author: Frank

Find the most popular movie
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("../ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)


# Infer the schema, and register the DataFrame as a table.
moviesDF.createOrReplaceTempView("movies")

print('*'*10)
movies = spark.sql("SELECT movieID, count(*) as count FROM movies GROUP BY movieID ORDER BY count DESC")
# The results of SQL queries are RDDs and support all the normal RDD operations.
for movie in movies.collect():
  print(movie)



# Stop the session
spark.stop()
