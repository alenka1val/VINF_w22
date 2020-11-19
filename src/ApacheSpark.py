from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, regexp_extract, regexp_replace
from dateutil.parser import parse
import sys

REGEX = (
    "(?i)""((?<=[^a-z0-9])((((0?[1-9])|(1[0-2]))|((jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(uary|ruary|ch|il|e|y|ust|tember|tober|ember)?)))([^0-9a-z:,])( *)((0?[1-9])|([1-2][0-9])|(3[0-1]))([^0-9a-z:])( *)([1-2]{1}[0-9]{3})(?=[^0-9a-z:]))|((?<=[^a-z0-9])(((0?[1-9])|([1-2][0-9])|(3[0-1]))[^0-9a-z:,])( *)((((0?[1-9])|(1[0-2]))|((jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(uary|ruary|ch|il|e|y|ust|tember|tober|ember)?)))([^0-9a-z:])( *)([1-2]{1}[0-9]{3}))")


def format_line(x):
    # Found date
    sentence = x[2]  # x[2] is line with sentence
    date = x[3]  # x[3] is date
    sentence = str(sentence).replace(str(date), "<*date*>").strip() \
        # .replace("\[\[.*\]\]", "<*tag*>")
    sentence = sentence.strip()

    list_of_types = []
    v_num = -1
    p_num = -1
    d_num = -1

    if sentence[0] == "|":
        sentence = sentence.replace("| ", "").replace("|", "").split(" ")[0].replace("_", " ")
        # todo for "|... |..."
    else:
        # REMOVE space from tags and replace them by _
        remove_space = False
        for i in range(0, len(sentence) - 1):
            if sentence[i] == "[" and sentence[i + 1] == "[":
                remove_space = True
                continue
            if remove_space and sentence[i] == " ":
                sentence = sentence[:i] + "_" + sentence[i + 1:]
                continue
            if sentence[i] == "]" and sentence[i + 1] == "]":
                remove_space = False

        sentence = sentence.split(" ")

        list_of_types = []

        i = 0
        for word in sentence:
            if word.endswith("ed") or word.endswith("ing"):
                list_of_types.append("V")  # V as verb

                if v_num == -1:
                    v_num = i
                else:
                    if d_num == -1:
                        v_num = i
                    else:
                        if abs(d_num - v_num) > abs(d_num - i):
                            v_num = i
            else:
                if word.startswith("[[") or (
                        i > 0 and sentence[i - 1] == "The" or sentence[i - 1] == "A" or sentence[i - 1] == "Some" or
                        sentence[i - 1] == "Any"):
                    list_of_types.append("P")  # P as podmet

                    if p_num == -1:
                        p_num = i
                    else:
                        if d_num == -1:
                            p_num = i
                        else:
                            if abs(d_num - p_num) > abs(d_num - i):
                                p_num = i

                else:
                    # print(word == "<*date*>")
                    if word.startswith("<*date*>"):
                        list_of_types.append("D")
                        d_num = i
                    else:
                        list_of_types.append("E")
            i += 1
        temp = ""
        if v_num > -1:
            temp = temp + sentence[v_num] + " "
        if p_num >= -1:
            temp = temp + sentence[p_num]

        sentence = temp

    # Date Formatting
    try:
        return (int(x[0]), str(x[1]), str(x[2]), str(parse(x[3]).date()), str(x[4]), str(sentence))
    except:
        return (int(x[0]), str(x[1]), str(x[2]), "", str(x[4]), str(sentence))


def parse_data(my_master, my_configuration, my_input_file):
    spark = SparkSession.builder.master(my_master).appName("Wikipedia_dates_Valova").config("spark.executor.uri",
                                                                                            my_configuration).getOrCreate()
    # spark = SparkSession.builder.getOrCreate()

    # LOAD XML TO DF
    df = spark.read.format('xml').options(rowTag='page').load(my_input_file)

    # df = spark.read.format('xml').options(rowTag='page').load('data/in/Sample/test.xml')

    # HEADER
    header = df.select('id', 'ns', 'title', 'redirect', 'revision.model', 'revision.format').filter(
        "redirect is null and ns == 0")

    # SPLIT TO SECTIONS
    date = df.select('id', 'revision.text._VALUE') \
        .filter("redirect is null and ns == 0") \
        .withColumn("_VALUE", regexp_replace(col("_VALUE"), "<.*>.*</.*>", "")) \
        .withColumn("_VALUE", regexp_replace(col("_VALUE"), "^\* *\{\{.*\}\}", "")) \
        .withColumn("_VALUE", regexp_replace(col("_VALUE"), "\[\[ *File.*\]\]", "")) \
        .withColumn("_VALUE", regexp_replace(col("_VALUE"), "<!--.*-->", "")) \
        .withColumn("_VALUE", regexp_replace(col("_VALUE"), "https?:\/\/.*\.", "")) \
        .withColumn('_VALUE', explode(split(col("_VALUE"), "\n"))) \
        .withColumn("_VALUE", regexp_replace(col("_VALUE"), "\{\{.*\}\}", "")) \
        .filter("_VALUE != ''")

    # SPLIT TO SENTENCES
    date = date.withColumn("Sentence", explode(split(col("_VALUE"), '(?<=[^A-Z])\. (?=[A-Z\[\[])')))

    # EXTRACT DATE FROM SENTENCE
    date = date.withColumn('Date', regexp_extract(col('Sentence'), pattern=REGEX, idx=0)).filter("Date != ''")

    # EXTRACT MEANING
    date = date.withColumn('Expression', regexp_extract(col('Sentence'), pattern='(\| *).* ?= ', idx=0))

    # EXTRACT NOUN
    date = date.withColumn('Noun', regexp_extract(col('Sentence'), pattern="\[\[[^\]]*]]", idx=0))

    # DATE FORMATTING
    date = date.rdd.map(format_line).toDF().filter("_4 != ''")

    # date.show()

    # STORE TO CSV
    date.write.csv('data/out/csv', mode='overwrite', header=True)


if __name__ == "__main__":
    # parse_data("", "", "")
    parse_data(sys.argv[1], sys.argv[2], sys.argv[3])
