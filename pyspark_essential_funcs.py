from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, concat, substring, trim, upper, lower,
    current_date, date_add, datediff, year, month,
    array, explode, size, array_contains, sort_array,
    when, coalesce, monotonically_increasing_id,
    count, sum as _sum, avg, max as _max, min as _min,
    isnull
)

def create_spark(app_name="Essentials"):
    """Initialize Spark session."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def create_sample_df(spark):
    """Create sample DataFrame."""
    data = [("Alice", 34), ("Bob", None), ("Carol", 29)]
    cols = ["name", "age"]
    return spark.createDataFrame(data, schema=cols)

def select_columns(df):
    "1. select – choose columns"
    return df.select("name")

def filter_rows(df):
    "2. filter – keep rows meeting condition"
    return df.filter(col("age") > 30)

def with_new_column(df):
    "3. withColumn – add derived column"
    return df.withColumn("is_senior", col("age") > 30)

def drop_column(df):
    "4. drop – remove a column"
    return df.drop("age")

def rename_column(df):
    "5. alias/withColumnRenamed – rename a column"
    return df.withColumnRenamed("name", "full_name")

def count_rows(df):
    "6. count – count rows"
    return df.count()

def group_and_aggregate(df):
    "7‑11. groupBy + agg(count, sum, avg, max, min)"
    return df.groupBy("name").agg(
        count("*").alias("cnt"),
        avg("age").alias("avg_age"),
        _sum("age").alias("sum_age"),
        _max("age").alias("max_age"),
        _min("age").alias("min_age")
    )

def concat_string(df):
    "12. concat – merge string columns"
    return df.withColumn("greeting", concat(col("name"), lit("!")))

def substring_col(df):
    "13. substring – get part of string"
    return df.withColumn("short", substring(col("name"), 1, 2))

def trim_string(df):
    "14. trim – remove whitespace"
    return df.withColumn("trimmed", trim(col("name")))

def upper_lower(df):
    "15‑16. upper & lower – case conversion"
    return df.withColumn("upper", upper(col("name"))).withColumn("lower", lower(col("name")))

def add_current_date(df):
    "17. current_date – today's date"
    return df.withColumn("today", current_date())

def add_days(df):
    "18. date_add – add days to a date"
    return df.withColumn("later", date_add(col("today"), 30))

def datediff_col(df):
    "19. datediff – days between dates"
    return df.withColumn("delta", datediff(col("later"), col("today")))

def extract_date_parts(df):
    "20‑21. year & month"
    return df.withColumn("yr", year(col("today"))).withColumn("mo", month(col("today")))

def make_array(df):
    "22. array – combine columns into an array"
    return df.withColumn("arr", array(col("name"), col("age")))

def explode_array(df):
    "23. explode – expand array into rows"
    return df.withColumn("arr", array(col("name"), lit("ZZ"))).select("name", explode("arr"))

def array_size(df):
    "24. size – length of array"
    return df.withColumn("len", size(col("arr")))

def array_contains_col(df):
    "25. array_contains – test array membership"
    return df.withColumn("has_Alice", array_contains(col("arr"), "Alice"))

def sort_array_col(df):
    "26. sort_array – sort elements"
    return df.withColumn("arr_sorted", sort_array(col("arr")))

def conditional_when(df):
    "27‑28. when + otherwise"
    return df.withColumn("cat", when(col("age") > 30, "Old").otherwise("Young"))

def expr_case(df):
    "29. expr – SQL CASE WHEN"
    return df.withColumn("grade", expr("CASE WHEN age>30 THEN 'B' ELSE 'A' END"))

def coalesce_col(df):
    "30. coalesce – pick first non-null"
    return df.withColumn("age2", coalesce(col("age"), lit(0)))

def isnull_col(df):
    "31. isnull – test nulls"
    return df.withColumn("isNull", isnull(col("age")))

def lit_column(df):
    "32. lit – add literal as column"
    return df.withColumn("const", lit(1))

def expr_if(df):
    "33. expr IF – inline conditional"
    return df.withColumn("discount", expr("if(age>30, 0.1, 0)"))

def dropna(df):
    "34. na.drop – drop null rows"
    return df.na.drop()

def fillna(df):
    "35. na.fill – fill nulls"
    return df.na.fill({"age": 0})

def distinct_rows(df):
    "36. distinct – remove dupes"
    return df.distinct()

def sort_rows(df):
    "37. sort/orderBy – sort DataFrame"
    return df.sort(col("age").desc())

def repartition_df(df):
    "38. repartition – change partitions"
    return df.repartition(4)

def cache_df(df):
    "39. cache – cache DataFrame"
    df.cache()
    return df

def repartition_range(df):
    "40. repartitionByRange"
    return df.repartitionByRange("age")

def monotonic_id(df):
    "41. monotonically_increasing_id – add unique ID"
    return df.withColumn("id", monotonically_increasing_id())

def select_expr(df):
    "42. selectExpr – SQL expression selection"
    return df.selectExpr("name", "age*2 as age2")

def alias_df(df):
    "43. alias – alias DataFrame"
    return df.alias("t")

def union_df(df):
    "44. union – stack DataFrames"
    return df.union(df)

def intersect_df(df):
    "45. intersect – find common rows"
    return df.intersect(df)

def except_df(df):
    "46. except – all but common rows"
    return df.exceptAll(df)
