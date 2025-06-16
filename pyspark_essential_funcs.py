# 🧩 pyspark_essential_funcs.py

This module defines **46 essential PySpark DataFrame functions** with clear code examples, explanations, and guidance for practical use. It’s designed to be importable and usable in notebooks, scripts, or production pipelines.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, concat, substring, trim, upper, lower,
    current_date, date_add, datediff, year, month,
    array, explode, size, array_contains, sort_array,
    when, coalesce, isnull,
    count, sum as _sum, avg, max as _max, min as _min,
    monotonically_increasing_id
)

def create_spark(app_name="Essentials"):
    """🛠 Initialize Spark session with ERROR log level."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def create_sample_df(spark):
    """📥 Create a simple sample DataFrame for testing/demonstration."""
    data = [("Alice", 34), ("Bob", None), ("Carol", 29)]
    return spark.createDataFrame(data, ["name", "age"])

# 1. select – choose columns
def select_columns(df):
    """Extract 'name' column."""
    return df.select("name")

# 2. filter – keep rows with age > 30
def filter_rows(df):
    return df.filter(col("age") > 30)

# 3. withColumn – add derived column
def with_new_column(df):
    """Add a boolean 'is_senior' flag for age > 30."""
    return df.withColumn("is_senior", col("age") > 30)

# 4. drop – remove the 'age' column
def drop_column(df):
    return df.drop("age")

# 5. withColumnRenamed – rename column
def rename_column(df):
    return df.withColumnRenamed("name", "full_name")

# 6. count – return number of rows in DataFrame
def count_rows(df):
    return df.count()

# 7–11. groupBy + agg(count, sum, avg, max, min)
def group_and_aggregate(df):
    return df.groupBy("name").agg(
        count("*").alias("cnt"),
        avg("age").alias("avg_age"),
        _sum("age").alias("sum_age"),
        _max("age").alias("max_age"),
        _min("age").alias("min_age")
    )

# 12. concat – merge string columns
def concat_string(df):
    return df.withColumn("greeting", concat(col("name"), lit("!")))

# 13. substring – extract substring
def substring_col(df):
    return df.withColumn("short", substring(col("name"), 1, 2))

# 14. trim – remove surrounding whitespace
def trim_string(df):
    return df.withColumn("trimmed", trim(col("name")))

# 15–16. upper & lower string case conversion
def upper_lower(df):
    return df.withColumn("upper", upper(col("name"))) \
             .withColumn("lower", lower(col("name")))

# 17. current_date – add column with today’s date
def add_current_date(df):
    return df.withColumn("today", current_date())

# 18. date_add – add 30 days to 'today'
def add_days(df):
    return df.withColumn("later", date_add(col("today"), 30))

# 19. datediff – days between dates
def datediff_col(df):
    return df.withColumn("delta", datediff(col("later"), col("today")))

# 20–21. year & month extraction
def extract_date_parts(df):
    return df.withColumn("yr", year(col("today"))) \
             .withColumn("mo", month(col("today")))

# 22. array – build array from columns
def make_array(df):
    return df.withColumn("arr", array(col("name"), col("age")))

# 23. explode – expand array columns into multiple rows
def explode_array(df):
    return df.withColumn("arr", array(col("name"), lit("ZZ"))) \
             .select("name", explode("arr"))

# 24. size – compute array length
def array_size(df):
    return df.withColumn("len", size(col("arr")))

# 25. array_contains – check membership
def array_contains_col(df):
    return df.withColumn("has_Alice", array_contains(col("arr"), "Alice"))

# 26. sort_array – sort elements in array
def sort_array_col(df):
    return df.withColumn("arr_sorted", sort_array(col("arr")))

# 27–28. when + otherwise – conditional column creation
def conditional_when(df):
    return df.withColumn("cat", when(col("age") > 30, "Old").otherwise("Young"))

# 29. expr CASE WHEN – SQL-style conditional logic
def expr_case(df):
    return df.withColumn("grade", expr("CASE WHEN age > 30 THEN 'B' ELSE 'A' END"))

# 30. coalesce – choose first non-null
def coalesce_col(df):
    return df.withColumn("age2", coalesce(col("age"), lit(0)))

# 31. isnull – detect nulls
def isnull_col(df):
    return df.withColumn("isNull", isnull(col("age")))

# 32. lit – literal column
def lit_column(df):
    return df.withColumn("const", lit(1))

# 33. expr IF – inline conditional
def expr_if(df):
    return df.withColumn("discount", expr("if(age > 30, 0.1, 0)"))

# 34. na.drop – drop rows with nulls
def dropna(df):
    return df.na.drop()

# 35. na.fill – fill null values
def fillna(df):
    return df.na.fill({"age": 0})

# 36. distinct – deduplicate rows
def distinct_rows(df):
    return df.distinct()

# 37. sort – sort by age descending
def sort_rows(df):
    return df.sort(col("age").desc())

# 38. repartition – adjust number of partitions
def repartition_df(df, num=4):
    return df.repartition(num)

# 39. cache – persist DataFrame in-memory
def cache_df(df):
    df.cache()
    return df

# 40. repartitionByRange – range partitioning
def repartition_range(df):
    return df.repartitionByRange("age")

# 41. monotonically_increasing_id – add unique IDs
def monotonic_id(df):
    return df.withColumn("id", monotonically_increasing_id())

# 42. selectExpr – select using SQL expressions
def select_expr(df):
    return df.selectExpr("name", "age * 2 as age2")

# 43. alias – rename DataFrame for joins
def alias_df(df):
    return df.alias("t")

# 44. union – stack DataFrames vertically
def union_df(df):
    return df.union(df)

# 45. intersect – find common rows
def intersect_df(df):
    return df.intersect(df)

# 46. exceptAll – exclude common rows
def except_df(df):
    return df.exceptAll(df)

