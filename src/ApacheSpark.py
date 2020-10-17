from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import numpy as np


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField('title', StringType(), False),
        StructField('id', IntegerType(), False),
        StructField('text', StringType(), False),
    ])

    df = spark.read.format("com.databricks.spark.xml").option("rowTag", "record").load("data/Sample/test.xml", schema=schema)
    print(df)
    # sc = SparkContext(master="local[4]")
    #
    # lst = np.random.randint(0, 10, 20)
    # A = sc.parallelize(lst)
    #
    # print(type(A))
    # A.collect()
    #
    # print(A.glom().collect())
    #
    # sc.stop()
    # sc = SparkContext

    # lines = sc.textFile("test.xml")

    # words = lines.flatMap(lambda line: line.split(" "))
    #
    # wordCounts = words.countByValue()
    #
    # for word, count in wordCounts.items():
    #     print("{}:{}".format(word, count))
