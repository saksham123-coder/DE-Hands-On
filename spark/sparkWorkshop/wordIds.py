docs = [
    (1, "one,two,three"),
    (2, "four,one,five"),
    (3, "seven,nine,one,two"),
    (4, "two,three,five"),
    (5, "six,five,one")
]
df = spark.createDataFrame(docs, ["id","words"])

search_words = [("one",), ("six",), ("eight",), ("five",), ("seven",)]
lookup = spark.createDataFrame(search_words, ["word"])
from pyspark.sql import functions as F
df = spark.createDataFrame(docs, ["id","words"])
df = df.withColumn("arr", F.split("words",","))
df=df.withColumn("token", F.explode("arr")).drop("arr","words")
final = df.join(lookup,df.token==lookup.word,"left").where("word is not null").groupBy("word").agg(F.count("id").alias("count"),F.collect_list("id").alias("ids"))
final.show()
