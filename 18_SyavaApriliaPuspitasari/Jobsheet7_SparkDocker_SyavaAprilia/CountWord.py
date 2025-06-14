from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    data = ["Hello Spark", "Hello Docker", "Spark is awesome"]
    lines = spark.sparkContext.parallelize(data)

    counts = (
        lines.flatMap(lambda x: x.split(' '))
             .map(lambda x: (x, 1))
             .reduceByKey(lambda a, b: a + b)
    )

    output = counts.collect()

    for (word, count) in output:
        print(f"{word}: {count}")

    spark.stop()