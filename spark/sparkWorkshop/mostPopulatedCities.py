data = [("US","NYC", 8800000), ("US","LA", 3890000),("IN","Mumbai", 12442373), ("IN","Delhi", 11007835)]
df = spark.createDataFrame(data, ["country","city","population"])
w = Window.partitionBy("country").orderBy(F.desc("population"))
df.withColumn("rn", F.row_number().over(w)).where("rn=1").select("country","city","population").show(20,False)
