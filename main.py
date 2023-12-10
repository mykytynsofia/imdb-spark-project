from pyspark import SparkConf  # набір конфігурацій для нашого кластеру
from pyspark.sql import SparkSession  # сесія на основі модуля sql
# тобто на далі ми працюватимемо з підмодулем Dataframes/SQL_API (є ще ML, Graphs, Streamimg)
import pyspark.sql.types as t
import pyspark.sql.functions as f
from participants import (yano,
                        shcherbii,
                        molochii,
                        mykytyn,
                        koval,
                            )
import datasets_paths as paths
from pyspark.sql import Window

# підняти кластер (тобто створити нашу точку входу в spark application - це буде наша спарк сесія)
spark_session = (
    SparkSession.builder.master("local")  # посилання на кластер
    .appName("first app")
    .config(conf=SparkConf())  # default conf
    .getOrCreate()
)  # якщо сесія вже запущена то її отримати, якщо немає то створити


name_basics_df = yano.load_name_basics_df(paths.PATH_NAME_BASICS, spark_session, f, t)
title_akas_df = yano.load_title_akas_df(paths.PATH_TITLE_AKAS, spark_session, f, t, Window)
title_basics_df = shcherbii.load_title_basics_df(paths.PATH_TITLE_BASICS, spark_session, f)
title_episode_df = mykytyn.load_title_episode_df(paths.PATH_TITLE_EPISODE, spark_session, f)
title_principals_df = koval.load_title_principals_df(paths.PATH_TITLE_PRINCIPALS, spark_session, f)


# # name_basics_df.show()
# name_basics_df.printSchema()

# yano.load_title_akas_df(paths.PATH_TITLE_AKAS, spark_session, f)
# shponarskyi.process_title_ratings(spark_session=spark_session, f=f, title_ratings_path=paths.PATH_TITLE_RATINGS, title_basics_path=paths.PATH_TITLE_BASICS)
# title_akas_df = yano.load_title_akas_df(paths.PATH_TITLE_AKAS, spark_session, f, t, Window);
# title_akas_df.show()
# title_akas_df.printSchema()

# # title_basics_df.show()
# title_basics_df.printSchema()

# # title_episode_df.show()
# title_episode_df.printSchema()

title_principals_df.show()
title_principals_df.printSchema()

