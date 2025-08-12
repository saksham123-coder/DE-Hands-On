
data = [("1", "P101", "2023-01-01", 5, 10.0),("2", "P102", "2023-01-01", 2, 50.0),
        ("3", "P101", "2023-01-01", 3, 10.0),("4", "P103", "2023-01-02", 10, 5.0),
        ("5", "P102", "2023-01-02", 1, 50.0),("6", "P103", "2023-01-02", 2, 5.0),
        ("7", "P101", "2023-01-02", 1, 10.0)]
schema = ["sale_id", "product_id", "sale_date", "quantity", "price"]

#revenue per product for each day 
df = df.withColumn("revenue", F.col("quantity") * F.col("price"))
dff = df.groupBy("sale_date","product_id").agg(sum("revenue").alias("total_revenue"))

#top 3 products by revenue for each day
window_spec = Window.partitionBy("sale_date").orderBy(F.col("total_revenue").desc())
dff.withColumn("rnk",F.row_number().over(window_spec)).show()
