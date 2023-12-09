from pyspark import SparkConf  # набір конфігурацій для нашого кластеру
from pyspark.sql import SparkSession  # сесія на основі модуля sql

# тобто на далі ми працюватимемо з підмодулем Dataframes/SQL_API (є ще ML, Graphs, Streamimg)
import pyspark.sql.types as t
import pyspark.sql.functions as f

import participants.yano as yano
import participants.shponarskyi as shponarskyi
import datasets_paths as paths


# підняти кластер (тобто створити нашу точку входу в spark application - це буде наша спарк сесія)
spark_session = (
    SparkSession.builder.master("local")  # посилання на кластер
    .appName("first app")
    .config(conf=SparkConf())  # default conf
    .getOrCreate()
)  # якщо сесія вже запущена то її отримати, якщо немає то створити

PATH = paths.PATH_TITLE_BASICS


def test_docker():
    print(f"Hi, from docker")


def create_df_basic():
    # DF creation
    data = [("Tonya", 18), ("Nina", 44)]

    # не обовʼязково схема, але краще створити (бо якщо не створимо, то спарк сам її створює)
    # і може створити погано (не той тип дати, і всім колонкам ставить nullable навіть якщо всі значення присутні (не довіряє))
    schema = t.StructType(
        [  # тип структура
            t.StructField(
                "name", dataType=t.StringType(), nullable=True
            ),  # поле в структурі
            t.StructField("age", dataType=t.ByteType(), nullable=True),
        ]
    )

    # people_df = spark_session.createDataFrame(data)
    people_df = spark_session.createDataFrame(data, schema)
    people_df.show()  # створивши нову змінну для датасету (переназвавши) то спарк не створюватиме копію, а робитиме референс

    people_df.printSchema()
    people_df.explain(mode="extended")

    return people_df


def create_df_file(path):
    schemas = {
        "title.basics.tsv": t.StructType(
            [  # тип структура
                t.StructField("tconst", dataType=t.StringType()),  # поле в структурі
                t.StructField("titleType", dataType=t.StringType()),  # поле в структурі
                t.StructField(
                    "primaryTitle", dataType=t.StringType()
                ),  # поле в структурі
                t.StructField(
                    "originalTitle", dataType=t.StringType()
                ),  # поле в структурі
                # t.StructField('isAdult', dataType=t.BooleanType()),  # creates null values
                t.StructField("isAdult", dataType=t.ByteType()),
                t.StructField("startYear", dataType=t.ShortType()),
                # t.StructField('endYear', dataType=t.ShortType()),
                t.StructField("endYear", dataType=t.StringType()),
                t.StructField("runtimeMinutes", dataType=t.ShortType()),
                t.StructField("genres", dataType=t.StringType()),
            ]
        )
    }

    # df1 -> title.basics.tsv
    def post_conversion_df1(imdb_df):
        imdb_df = imdb_df.withColumn(
            "isAdult", f.expr("case when isAdult = 1 then true else false end")
        )
        return imdb_df

    post_explicit_conversion = {"title.basics.tsv": post_conversion_df1}

    df_file_name = path.split("/")[-1]

    df_schema = schemas[df_file_name]
    imdb_df = spark_session.read.csv(path, sep=r"\t", header=True, schema=df_schema)

    imdb_df = post_explicit_conversion[df_file_name](imdb_df)

    imdb_df.show()

    imdb_df.printSchema()
    imdb_df.explain(mode="extended")

    return imdb_df


# test_docker()
# create_df_basic()
# create_df_file(PATH)


# name_basics_df = yano.load_name_basics_df(paths.PATH_NAME_BASICS, spark_session, f)
# print(' -------> "main.py" name_basics_df :')
# name_basics_df.show()
# name_basics_df.printSchema()
# stats_df = name_basics_df.describe() # займає багато часу
# stats_df.show()

# yano.load_title_akas_df(paths.PATH_TITLE_AKAS, spark_session, f)
shponarskyi.process_title_ratings(spark_session=spark_session, f=f, title_ratings_path=paths.PATH_TITLE_RATINGS, title_basics_path=paths.PATH_TITLE_BASICS)

