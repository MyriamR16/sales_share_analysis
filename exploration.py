from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, month, round, to_date, year

## Create a Spark session
spark = SparkSession.builder.appName("Exploration").getOrCreate()

## Load the data
df = (
	spark.read.option("header", True)
	.option("quote", '"')
	.option("escape", '"')
	.option("multiLine", True)
	.option("mode", "PERMISSIVE")
	.csv("train.csv")
)
# df.printSchema()

## Clean the data

# convert Order date and ship date from string to a date type
df = df.withColumn("Order Date", to_date(df["Order Date"], "dd/MM/yyyy"))
df = df.withColumn("Ship Date", to_date(df["Ship Date"], "dd/MM/yyyy"))

# convert Sales from string to double
df = df.withColumn("Sales", expr("try_cast(`Sales` as double)"))
df = df.filter(df["Sales"].isNotNull())

# total sales used as denominator for internal market share
total_sales = df.agg(expr("sum(Sales) as total_sales")).collect()[0]["total_sales"]

#df.printSchema()

## Create a table for sales by Region 
sales_by_region = (
	df.groupBy("Region")
	.sum("Sales")
	.withColumnRenamed("sum(Sales)", "Total Sales")
	.withColumn("Internal Market Share %", round((col("Total Sales") / lit(total_sales)) * 100, 2))
	.orderBy(col("Total Sales").desc())
)

## Create a table for sales by Category 
sales_by_category = (
	df.groupBy("Category")
	.sum("Sales")
	.withColumnRenamed("sum(Sales)", "Total Sales")
	.withColumn("Internal Market Share %", round((col("Total Sales") / lit(total_sales)) * 100, 2))
	.orderBy(col("Total Sales").desc())
)

## Create a table for sales by Segment 
sales_by_segment = (
	df.groupBy("Segment")
	.sum("Sales")
	.withColumnRenamed("sum(Sales)", "Total Sales")
	.withColumn("Internal Market Share %", round((col("Total Sales") / lit(total_sales)) * 100, 2))
	.orderBy(col("Total Sales").desc())
)

## Create a table for sales by City 
sales_by_city = (
	df.groupBy("City")
	.sum("Sales")
	.withColumnRenamed("sum(Sales)", "Total Sales")
	.withColumn("Internal Market Share %", round((col("Total Sales") / lit(total_sales)) * 100, 4))
	.orderBy(col("Total Sales").desc())
)

## Create a table for sales by Month/Year
sales_by_month_year = (
	df.groupBy(
		year("Order Date").alias("Order Year"),
		month("Order Date").alias("Order Month"),
	)
	.sum("Sales")
	.withColumnRenamed("sum(Sales)", "Total Sales")
	.withColumn("Internal Market Share %", round((col("Total Sales") / lit(total_sales)) * 100, 2))
	.orderBy("Order Year", "Order Month")
)

## Show the results
print("Sales by Region:")
sales_by_region.show(20, truncate=False)

print("Sales by Category:")
sales_by_category.show(20, truncate=False)

print("Sales by Segment:")
sales_by_segment.show(20, truncate=False)

print("Sales by City:")
sales_by_city.show(20, truncate=False)

print("Sales by Month/Year:")
sales_by_month_year.show(20, truncate=False)

## Export results as CSV for Power BI
exports = {
	"output/sales_by_region": sales_by_region,
	"output/sales_by_category": sales_by_category,
	"output/sales_by_segment": sales_by_segment,
	"output/sales_by_city": sales_by_city,
	"output/sales_by_month_year": sales_by_month_year,
}

for path, table in exports.items():
	(
		table.coalesce(1)
		.write.mode("overwrite")
		.option("header", True)
		.csv(path)
	)

print("Exports created in ./output")

## Stop the Spark session
spark.stop()