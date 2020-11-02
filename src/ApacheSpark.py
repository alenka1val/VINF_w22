from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, regexp_extract, to_date

REGEX = (
    "(?i)""((?<=[^a-z0-9])((((0?[1-9])|(1[0-2]))|((jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(uary|ruary|ch|il|e|y|ust|tember|tober|ember)?)))([^0-9a-z:])( *)((0?[1-9])|([1-2][0-9])|(3[0-1]))([^0-9a-z:])( *)(([1-2]{1}[0-9]{3})|([1-9][0-9]{0,2}))(?=[^0-9a-z:]))")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format('xml').options(rowTag='page').load(
        'data/in/All/enwiki-latest-pages-articles-multistream1.xml-p1p30303')
    # df = spark.read.format('xml').options(rowTag='page').load('data/in/Sample/test.xml')
    header = df.select('id', 'ns', 'title', 'redirect', 'revision.model', 'revision.format').filter(
        "redirect is null and ns == 0")

    date = df.select('id', 'revision.text._VALUE').filter("redirect is null and ns == 0").withColumn('_VALUE', explode(
        split(col("_VALUE"), "\n")))

    # SPLIT INTO SENTENCES
    date = date.withColumn("Sentence", explode(split(col("_VALUE"), '(?<=[^A-Z])\. (?=[A-Z])')))

    # EXTRACT DATE FROM SENTENCE
    date = date.withColumn('Date', regexp_extract(col('_VALUE'), pattern=REGEX, idx=0)).filter("Date != ''")

    # date.show()

    date.write.csv('date.csv', mode='overwrite', header=True)

    # date = date.withColumn('Date', to_date(col("Date")))
    # date.show()
