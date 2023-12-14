
import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                            camel_to_snake,
                            str_to_arr_type,
                            create_folder)

import columns.columns_title_basics as columns_title_basics

from schemas import schema_title_basics, schema_title_basics_final

from pyspark.sql.window import Window
import pyspark.sql.functions as f


def count_movies_per_genre(title_basics_df):
    """
       Бізнес питання 1: Кількість фільмів кожного жанру 
    """
    exploded_df = title_basics_df.withColumn("genre", f.explode(title_basics_df["genres"])) \
        .filter(f.col("genre") != "")

    genre_count_df = exploded_df.groupBy("genre")\
        .agg(f.count("*").alias("movie_count"))
    return genre_count_df.orderBy("genre", ascending=False)


def longest_movie_per_genre(title_basics_df):
    """
       Бізнес питання 2: Найдовший фільму кожного жанру 
    """
    exploded_df = title_basics_df.withColumn("genre", f.explode(title_basics_df["genres"])) \
            .filter(f.col("genre") != "")

    window_spec = Window.partitionBy("genre") \
            .orderBy(f.desc("runtime_minutes"), f.desc("start_year"))

    longest_movie_df = exploded_df.withColumn("rank", f.rank().over(window_spec)) \
        .filter(f.col("rank") == 1).drop("rank").select("tconst", "original_title", "runtime_minutes", "genre")
    return longest_movie_df.orderBy("genre")


def ukrainian_max_episodes(title_episode_df, title_akas_df):
    """
       Бізнес питання 3:  Пошук українського серіалу, що містить найбільшу кількість епізодів
    """
    df = title_episode_df.join(title_akas_df, title_episode_df.tconst == title_akas_df.title_id) \
    .filter((f.col("region") == "UA") & (f.col("language") == "uk")) \
    .orderBy(f.desc("episode_number")).limit(1)
    return df.select("title_id", "episode_number", "title")


def ukrainian_top_movies(title_ratings_df, title_akas_df, title_basics_df):
    """
       Бізнес питання 4:  Знаходження 10 фільмів 
                        продубльованих українською з найбільшим рейтингом
    """
    title_akas_df = title_akas_df.filter(f.col("region") == "UA")
    title_basics_df = title_basics_df.filter(f.col("title_type") == 'movie')

    joined_df = title_basics_df.join(title_akas_df, \
                            title_basics_df.tconst == title_akas_df.title_id)\
        .select("tconst", "title", "title_type", "region")
    
    df = title_ratings_df.join(joined_df, 'tconst').dropDuplicates(['tconst'])

    return df.filter((f.col("averageRating") > 8) \
                   & (f.col("numVotes") > 1000)) \
    .orderBy(f.desc("averageRating")).limit(10)


def yearly_movie_count(title_basics_df):
    """
       Бізнес питання 5:  Визначення кількосі фільмів, що містить жанр драма за останні 10 років
    """
    filtered_df = title_basics_df.filter(f.array_contains("genres", "Drama"))

    return filtered_df.filter((f.col('start_year').isNotNull()) & \
                              ((f.col("title_type") == 'movie'))) \
        .groupBy("start_year").agg(f.count("*").alias("movie_count")) \
        .orderBy(f.desc("movie_count")).limit(10)


def most_episodes_per_year(title_basics_df, title_episode_df):
    """
       Бізнес питання 6:  Знаходження фільму з найбільшою кількісті епізодів  для кожного року
    """

    joined_df = title_episode_df.join(title_basics_df, 'tconst').filter(f.col('start_year').isNotNull())\
        .groupBy("start_year").agg(f.max("episode_number").alias("max_episodes"))

    return joined_df.orderBy("start_year")