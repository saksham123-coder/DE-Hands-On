data = [
    ("100","John", 35,   None),
    ("100","John", None, "Georgia"),
    ("101","Mike", 25,   None),
    ("101","Mike", None, "New York"),
    ("103","Mary", 22,   None),
    ("103","Mary", None, "Texas"),
    ("104","Smith",25,   None),
    ("105","Jake", None, "Florida")
]
df = spark.createDataFrame(data, ["id","name","age","city"])
from pyspark.sql.window import Window
from pyspark.sql import functions as F
win = Window.partitionBy("id").orderBy(F.lit(1))
df.withColumn("age_ff", F.first("age", ignorenulls=True).over(win)).withColumn("city_ff", F.first("city", ignorenulls=True).over(win)).drop("age","city").dropDuplicates(["id","name"]).withColumnRenamed("age_ff","age").withColumnRenamed("city_ff","city").show()
