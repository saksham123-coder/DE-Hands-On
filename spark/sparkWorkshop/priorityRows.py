#assuming i have the following df
from pyspark.sql.functions import *
from pyspark.sql.window import *
data = [(1,"MV1"),(1,"MV2"),(2,"VPV"),(2,"Others")]
df = spark.createDataFrame(data,["id", "values"])
df2 = df
df3 = df2.withColumn("rnk",when(col("values")=="VPV",1).when(col("values")=="MV1",2).when(col("values")=="MV2",3).otherwise(4)).show()
window_spec = Window.partitionBy("id").orderBy(col("rnk").asc())
df3.withColumn("rank",row_number().over(window_spec)).filter(col("rank")==1).show()
