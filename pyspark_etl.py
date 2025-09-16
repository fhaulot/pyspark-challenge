from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, BooleanType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("Blockchain Transaction ETL") \
        .getOrCreate()

    # Define schema matching the actual data
    out_schema = ArrayType(StructType([
        StructField("spent", BooleanType()),
        StructField("tx_index", IntegerType()),
        StructField("type", IntegerType()),
        StructField("addr", StringType()),
        StructField("value", LongType()),
        StructField("n", IntegerType()),
        StructField("script", StringType()),
    ]))
    x_schema = StructType([
        StructField("hash", StringType()),
        StructField("out", out_schema)
    ])
    schema = StructType([
        StructField("op", StringType()),
        StructField("x", x_schema)
    ])

    # Read streaming data from JSON Lines files
    df = spark.readStream \
        .format("json") \
        .option("multiLine", False) \
        .schema(schema) \
        .load("data_stream/")

    # Extract transaction hash and total value (sum of out values)
    tx_df = df.select(
        col("x.hash").alias("tx_hash"),
        expr("aggregate(x.out, 0L, (acc, o) -> acc + o.value)").alias("total_value")
    )

    # Write the result to the console (for demo)
    query = tx_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()