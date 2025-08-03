##Assuming spark session is already created
##Couldn't download the dataset so have manulay created through spark

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

schema = StructType([StructField("DISH", StringType(), True),StructField("ORDERID", StringType(), True),StructField("STOREID", StringType(), True),StructField("PRICE", DoubleType(), True),StructField("TOTALAMOUNT", DoubleType(), True),StructField("QUANTITY", DoubleType(), True),StructField("JUSTANOTHERFEATURE", DoubleType(), True)])

data = [("Biryani", "O1", "S1", 100.0, 200.0, 2.0, 5.0),("Pizza",  "O1", "S1", 100.0, 150.0, 1.5, 3.0),("Biryani", "O2", "S2", 80.0,  80.0,  1.0, 2.5),("Pizza",   "O3", "S1", 120.0, 120.0, 1.0, 1.0),("Dosa",    "O4", "S3",  50.0,  50.0,  1.0, 1.5),("Biryani", "O5", "S1", 200.0, 200.0, 1.0, 2.0),("Salad",   "O6", "S2",  30.0,  30.0,  1.0, 0.5)]


df = spark.createDataFrame(data, schema=schema)

df.printSchema()
df.show(truncate=False)

df.cache()

df_full = df
dishes_of_interest = ["Biryani", "Pizza", "Dosa"]
df = df.filter(F.col("DISH").isin(dishes_of_interest))
agg = (
    df.groupBy("ORDERID", "STOREID", "PRICE")
      .agg(
          F.count("*").alias("cnt"),
          F.sum("TOTALAMOUNT").alias("sum_total"),
          F.sum("QUANTITY").alias("sum_qty"),
          F.sum("JUSTANOTHERFEATURE").alias("sum_feat"),
          F.collect_set("DISH").alias("dishes"),
          F.collect_set("JUSTANOTHERFEATURE").alias("feat_vals")
      )
)
agg2 = agg.withColumn(
    "has_pizza_dosa",
    F.arrays_overlap(F.col("dishes"), F.array(*[F.lit(d) for d in ["Pizza", "Dosa"]]))
).withColumn(
    "has_biryani",
    F.array_contains(F.col("dishes"), "Biryani")
)
agg3 = agg2.withColumn(
    "rep_dish",
    F.when(
        (F.col("cnt") > 1) & F.col("has_biryani") & (~F.col("has_pizza_dosa")),
        F.lit("Biryani")
    ).when(
        (F.col("cnt") > 1) & F.col("has_pizza_dosa"),
        F.expr("filter(dishes, x -> x IN ('Pizza','Dosa'))")[0]
    )
)

to_agg = agg3.filter(F.col("rep_dish").isNotNull() & F.col("cnt") > 1)
agg_sel = to_agg.withColumn(
    "final_total", F.col("sum_total")
).withColumn(
    "final_qty",
    F.when((F.col("sum_qty") == 0) & (F.col("sum_total") > 0), F.lit(10.0))
     .when((F.col("sum_qty") == 0) & (F.col("sum_total") < 0), F.lit(-20.0))
     .otherwise(F.col("sum_qty"))
).withColumn(
    "final_feat",
    F.when((F.col("sum_qty") == 0) & (F.col("sum_total") > 0), F.lit(20.0))
     .when((F.col("sum_qty") == 0) & (F.col("sum_total") < 0), F.lit(-10.0))
     .otherwise(F.col("sum_feat"))
).withColumn(
    "feat_list",
    F.expr("filter(transform(feat_vals, x -> cast(x as double)), x -> x > 1)")
).select(
    F.expr("ORDERID"),
    F.expr("STOREID"),
    F.expr("PRICE"),
    F.col("rep_dish").alias("DISH"),
    F.col("final_total").alias("TOTALAMOUNT"),
    F.col("final_qty").alias("QUANTITY"),
    F.col("feat_list").alias("JUSTANOTHERFEATURE")
)
kept_keys = agg3.filter(
    (F.col("cnt") == 1) |
    ((F.col("cnt") > 1) & F.col("rep_dish").isNull())
).select("ORDERID", "STOREID", "PRICE")

df_kept = df.join(kept_keys, on=["ORDERID", "STOREID", "PRICE"], how="inner")

agg_sel = agg_sel.withColumn("JUSTANOTHERFEATURES",F.explode(F.col("JUSTANOTHERFEATURE"))).drop("JUSTANOTHERFEATURE").withColumnRenamed("JUSTANOTHERFEATURES","JUSTANOTHERFEATURE")

df_result = df_kept.select(df.columns).unionByName(agg_sel.select(df.columns))
df_others = df_full.filter(~F.col("DISH").isin(dishes_of_interest))
df_final = df_others.unionByName(df_result)
output_path = "any s3 path"
df_final.repartition("DISH").write.mode("overwrite").partitionBy("DISH").parquet(output_path)
