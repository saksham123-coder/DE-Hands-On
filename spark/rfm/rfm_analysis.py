from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

bucket_udf = F.udf(lambda p: 5 if p > 90 else 4 if p > 75 else 3 if p > 50 else 2 if p > 20 else 1, IntegerType())

spark = SparkSession.builder.appName("RFMAnalysis").getOrCreate()

# Adjust this path to your local CSV file
csv_path = r"C:\Users\mashu\saksham\spark-assignment-RFM\RFM\data.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)


spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


df_with_ts = df.withColumn("parsed_ts", to_timestamp("InvoiceDate", "MM/d/yyyy H:mm")).drop("InvoiceDate")
df_with_date = df_with_ts.withColumn("InvoiceDate", to_date("parsed_ts")).drop("parsed_ts")
df = df_with_date
df = df.withColumn("InvoiceDate", F.to_date("InvoiceDate"))

max_date = df.select(F.max("InvoiceDate")).first()[0]

rfm = df.groupBy("CustomerID").agg(
    F.datediff(F.lit(max_date), F.max("InvoiceDate")).alias("Recency"),
    F.countDistinct("InvoiceNo").alias("Frequency"),
    F.sum("UnitPrice").alias("Monetary")
)

w_r = Window.partitionBy("CustomerID").orderBy("Recency")
w_f = Window.partitionBy("CustomerID").orderBy(F.col("Frequency").desc())
w_m = Window.partitionBy("CustomerID").orderBy(F.col("Monetary").desc())

rfm = rfm.withColumn("Recency_pct", F.percent_rank().over(w_r) * 100) \
         .withColumn("Frequency_pct", F.percent_rank().over(w_f) * 100) \
         .withColumn("Monetary_pct", F.percent_rank().over(w_m) * 100) \
         .withColumn("R_score", bucket_udf(F.lit(100) - F.col("Recency_pct"))) \
         .withColumn("F_score", bucket_udf(F.col("Frequency_pct"))) \
         .withColumn("M_score", bucket_udf(F.col("Monetary_pct"))) \
         .withColumn("RFM_score", F.col("R_score") + F.col("F_score") + F.col("M_score"))

final_score = rfm.select("CustomerID", "Recency", "Frequency", "Monetary", "R_score", "F_score", "M_score", "RFM_score") \
   .orderBy(F.col("RFM_score").desc())

spark.stop()
