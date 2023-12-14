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
from queries import shcherbii_queries

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


name_basics_df = yano.load_name_basics_df(paths.PATH_NAME_BASICS, spark_session, f, t)
title_akas_df = yano.load_title_akas_df(paths.PATH_TITLE_AKAS, spark_session, f, t, Window)
title_basics_df = shcherbii.load_title_basics_df(paths.PATH_TITLE_BASICS, spark_session, f)
title_episode_df = mykytyn.load_title_episode_df(paths.PATH_TITLE_EPISODE, spark_session, f)
title_principals_df = koval.load_title_principals_df(paths.PATH_TITLE_PRINCIPALS, spark_session, f)
title_ratings_df = shponarskyi.load_title_ratings_df(spark_session, paths.PATH_TITLE_RATINGS, f)
title_crew_df = molochii.load_title_crew_df(paths.PATH_TITLE_CREW, spark_session, f, t)

# # name_basics_df.show()
# name_basics_df.printSchema()

# name_basics_df.show()
# name_basics_df.printSchema()

# title_akas_df.show()
# title_akas_df.printSchema()

# title_basics_df.show()
# title_basics_df.printSchema()

# title_episode_df.show()
# title_episode_df.printSchema()

# title_principals_df.show()
# title_principals_df.printSchema()

# title_ratings_df.show()
# title_ratings_df.printSchema()

# title_crew_df.show()
# title_crew_df.printSchema()

# movies_per_genre = shshcerbii_queries.count_movies_per_genre(title_basics_df)
# movies_per_genre.show(50)


# longest_movie_per_genre = shshcerbii_queries.longest_movie_per_genre(title_basics_df)
# longest_movie_per_genre.show(50)


# ukrainian_max_episodes = shshcerbii_queries.ukrainian_max_episodes(title_episode_df, title_akas_df)
# ukrainian_max_episodes.show()


# ukrainian_top_movies = shshcerbii_queries.ukrainian_top_movies(title_ratings_df, title_akas_df, title_basics_df)
# ukrainian_top_movies.show()


# yearly_movie_count = shshcerbii_queries.yearly_movie_count(title_basics_df)
# yearly_movie_count.show()


# most_episodes_per_year = shshcerbii_queries.most_episodes_per_year(title_basics_df, title_episode_df)
# most_episodes_per_year.show()