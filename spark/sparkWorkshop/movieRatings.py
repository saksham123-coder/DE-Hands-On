from pyspark.sql import Row
from pyspark.sql import functions as F
MovieRating = Row("movieName","rating")
data = [("Manuel", [MovieRating("Logan",1.5), MovieRating("Zoolander",3.0), MovieRating("John Wick",2.5)]),("John",   [MovieRating("Logan",2.0), MovieRating("Zoolander",3.5), MovieRating("John Wick",3.0)])]
df = spark.createDataFrame(data, "name string, movieRatings array<struct<movieName:string,rating:double>>")
expl = df.select("name", F.explode("movieRatings").alias("mr"))
expl.select("name",F.col("mr.movieName").alias("movieName"),F.col("mr.rating").alias("rating")).groupBy("name").pivot("movieName").agg(F.first("rating")).show(20,False)
