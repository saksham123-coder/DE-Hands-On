data = [("2025-08-10","2025-08-15")]
df = spark.createDataFrame(data, ["start","end"])

ans = df.select(F.datediff(F.to_date("end"), F.to_date("start")).alias("days"))
