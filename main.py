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
                        shponarskyi)
import datasets_paths as paths
from useful_functions import (init_datasets_folders,
                              init_queries_results_folder,
                              check_folder_content)
from pyspark.sql import Window
from queries import yano_queries

init_datasets_folders() # Створяться папки datasets, datasets_mod, де зберігатимуться сирі та оброблені dfs відповідно
                        # !NOTE! Проте, щоб запустити обробку dfs та запити, потрібно власноруч завантажити сирі датафрейми [https://datasets.imdbws.com/] у папку ʼdatasetsʼ (створену програмою) та назвати їх:
                        # name.basics.tsv
                        # title.akas.tsv
                        # ...

init_queries_results_folder()   # Ф-я для створення папки ʼqueries_resultsʼ в корені проекту,
                                # щоб зберігати результати запитів

check_folder_content()  # Перевірити, чи користувач завантажив усі потрібні датасети

# підняти кластер (тобто створити нашу точку входу в spark application - це буде наша спарк сесія)
spark_session = (
    SparkSession.builder.master("local")  # посилання на кластер
    .appName("first app")
    .config(conf=SparkConf())  # default conf
    # .config("spark.executor.cores", "4")
    .getOrCreate()
)  # якщо сесія вже запущена то її отримати, якщо немає то створити

# HANDLE DATASETS [first time] OR LOAD HANDLED DATASETS
name_basics_df = yano.load_name_basics_df(paths.PATH_NAME_BASICS, spark_session, f, t)
title_akas_df = yano.load_title_akas_df(paths.PATH_TITLE_AKAS, spark_session, f, t, Window)
title_basics_df = shcherbii.load_title_basics_df(paths.PATH_TITLE_BASICS, spark_session, f)
title_episode_df = mykytyn.load_title_episode_df(paths.PATH_TITLE_EPISODE, spark_session, f)
title_principals_df = koval.load_title_principals_df(paths.PATH_TITLE_PRINCIPALS, spark_session, f)
title_ratings_df = shponarskyi.load_title_ratings_df(spark_session, paths.PATH_TITLE_RATINGS, f)
title_crew_df = molochii.load_title_crew_df(paths.PATH_TITLE_CREW, spark_session, f, t)

# VISUALIZE THEM + PRINT SCHEMA
df_s = [name_basics_df, title_akas_df, title_basics_df, title_episode_df,
        title_principals_df, title_ratings_df, title_crew_df]
for df in df_s:
    df.show()
    df.printSchema()

# DO QUERIES [first time] OR LOADED ALREADY RESULTS OF THEM
koval_queries.query_one(title_basics_df, spark_session, f, t).show(truncate=False)
koval_queries.query_two(spark_session, f, title_principals_df, name_basics_df, title_basics_df, t, Window).show()
koval_queries.query_three(spark_session, f, title_basics_df, title_akas_df, t).show(truncate=False)
koval_queries.query_four(spark_session, title_episode_df, title_principals_df, title_basics_df, f, t).show(truncate=False)
koval_queries.query_five(title_ratings_df, title_basics_df, f, t, spark_session, Window).show(truncate=False)
koval_queries.query_six(name_basics_df, title_basics_df, title_crew_df, f, t, spark_session).show(truncate=False)