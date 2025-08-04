#assuming we have a spark dataframe like below
from pyspark.sql.functions import *
data = [("50000.0#0#0#", "#"),("0@1000.0@", "@"),("1$", "$"),("1000.00^Test_string", "^")]
df = spark.createDataFrame(data,["VALUES", "Delimiter"])
meta = ["^", "$", ".", "|", "?", "+", "*", "(", ")", "[", "]", "{", "}", "\\"]

df2 = df.withColumn("quotedDelim",when(col("Delimiter").isin(*meta),concat(lit("\\"), col("Delimiter"))).otherwise(col("Delimiter"))
).withColumn("splited_text",expr("filter(split(VALUES, quotedDelim), x -> trim(x) != '')"))

df2.show(truncate=False)
