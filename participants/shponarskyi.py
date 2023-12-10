import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                              camel_to_snake,
                              str_to_arr_type,
                              create_folder)
import columns.columns_title_ratings as columns_title_ratings

from schemas import schema_title_basics, schema_title_basics_final, schema_title_ratings, schema_title_ratings_final

from pyspark.sql import Window
from pyspark.sql.functions import col, sum


def load_title_ratings_df(spark_session, path, f):
    if os.listdir(paths.PATH_TITLE_RATINGS_MOD):
        print(f"Title ratings already preprocessed")

        df = spark_session.read.csv(paths.PATH_NAME_BASICS_MOD,
                                          sep=r"\t",
                                          header=True,
                                          nullValue="\\N",
                                          schema=schema_title_basics_final)

        return str_to_arr_type(df, [], ',', f)

    title_ratings_df = spark_session.read.csv(path,
                                            sep=r"\t",
                                            header=True,
                                            nullValue="\\N",
                                            schema=schema_title_basics_final)

    columns = title_ratings_df.columns 
    renamed_columns = [camel_to_snake(c) for c in columns]

    for i, column in enumerate(columns):
        title_ratings_df = title_ratings_df.withColumnRenamed(column, renamed_columns[i])
    
    get_statistics(title_ratings_df, 'title_ratings_df')

    title_ratings_df = title_ratings_df.na.drop(subset=title_ratings_df.columns)

    title_ratings_df.show(30, truncate=False)
    title_ratings_df.printSchema()

    create_folder(paths.PATH_TITLE_RATINGS_MOD)
    print(f'Saving to {paths.PATH_TITLE_RATINGS_MOD} ...')
    title_ratings_df.write.csv(paths.PATH_TITLE_RATINGS_MOD, header=True, mode='overwrite', sep='\t')

    return str_to_arr_type(title_ratings_df, [], ',', f)


def process_title_ratings(spark_session, title_ratings_path, title_basics_path, f):
    ratings_df = spark_session.read.csv(
        title_ratings_path, sep=r"\t", header=True, nullValue='\\N', schema=schema_title_ratings)

    title_basics_df = spark_session.read.csv(title_basics_path,
                                             sep=r"\t",
                                             header=True,
                                             nullValue="\\N",
                                             schema=schema_title_basics)

    ratings_df.printSchema()
    title_basics_df.printSchema()

    title_basics_df = title_basics_df.na.drop(subset=title_basics_df.columns)
    ratings_df = ratings_df.na.drop(subset=ratings_df.columns)

    # Топ 100 фільмів з найвищою середньою оцінкою та кількістю відгуків >= 1000
    average_rating_df = ratings_df.join(title_basics_df, "tconst").filter(
        "numVotes >= 1000").orderBy("averageRating", ascending=False).limit(100)

    average_rating_df.show()

    # Як змінювалась середня оцінка фільмів жанру Action протягом років?
    grouped_rating_df = ratings_df.join(title_basics_df, "tconst").filter(
        "runtimeMinutes >= 50 AND genres = 'Action'").groupBy("startYear").agg({"averageRating": "avg"}).orderBy("startYear")

    grouped_rating_df.show()

    # Як змінювалась середня тривалість фільмів кожного жанру протягом десятиліть?
    df_movies = ratings_df.join(title_basics_df, "tconst")

    decade_df = df_movies.withColumn("decade", f.floor(df_movies["startYear"] / 10) * 10).filter(
        "averageRating >= 8").groupBy("decade", "genres").agg({"runtimeMinutes": "avg"}).orderBy("decade")

    decade_df.show()

    # Які жанри фільмів мають найбільшу кількість відгуків?
    joined_df = ratings_df.join(title_basics_df, "tconst")

    window_spec = Window.partitionBy("genres").orderBy(col("numVotes").desc())

    ratings_genre_df = joined_df.withColumn("cumulative_ratings", sum(
        "numVotes").over(window_spec)).orderBy("cumulative_ratings", ascending=False)

    ratings_genre_df.drop('startYear').drop('genres').drop(
        'endYear').drop('runtimeMinutes').show()

    # Як змінювався рейтинг фільмів у жанрах 'Action', 'History', 'Crime', 'Sci-Fi' протягом років?
    avg_rating_per_year_genre = ratings_df.join(title_basics_df, "tconst").filter(
        "genres IN ('Action', 'History', 'Crime', 'Sci-Fi')").groupBy("startYear", "genres").agg({"averageRating": "avg"}).orderBy('startYear')

    avg_rating_per_year_genre.show()

    # Скільки фільмів виходили кожного року і яка кількість відгуків та середня оцінка?
    movies_per_year = ratings_df.join(title_basics_df, "tconst").groupBy("startYear").agg(
        {"averageRating": "avg", "tconst": "count", "numVotes": "sum"}).orderBy("startYear")

    movies_per_year.show()
