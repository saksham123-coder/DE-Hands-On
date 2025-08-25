data = [("alice",),("Bob",)]
df = spark.createDataFrame(data, ["col"])

ans = df.withColumn("upper_col", F.upper("col"))
