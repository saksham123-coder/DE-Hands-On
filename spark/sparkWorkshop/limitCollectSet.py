from pyspark.sql import functions as F
df = spark.range(50).withColumn("key", F.col("id") % 5)
df.groupBy("key").agg(F.collect_set("id").alias("all")).withColumn("only_first_three", F.slice("all", 1, 3)).show(20,False)
