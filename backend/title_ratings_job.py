#! /usr/bin/python

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql import Window
from pyspark.sql.functions import col, sum
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("Processing").getOrCreate()

def processTop100(ratings_df, title_basics_df):
    average_rating_df = ratings_df.join(title_basics_df, "tconst").filter(
        "numVotes >= 1000").orderBy("averageRating", ascending=False).limit(100)

    average_rating_df.write.csv("gs://imdb-dataframes-results/results/title.ratings/0", mode="overwrite", header=True, sep="|")

def processYearAverage(ratings_df, title_basics_df):
    grouped_rating_df = ratings_df.join(title_basics_df, "tconst").filter(
        "runtimeMinutes >= 50 AND genres = 'Action'").groupBy("startYear").agg({"averageRating": "avg"}).orderBy("startYear")

    grouped_rating_df.write.csv("gs://imdb-dataframes-results/results/title.ratings/1", mode="overwrite", header=True, sep="|")

def processDecadeAverage(ratings_df, title_basics_df):
    df_movies = ratings_df.join(title_basics_df, "tconst")

    decade_df = df_movies.withColumn("decade", f.floor(df_movies["startYear"] / 10) * 10).filter(
        "averageRating >= 8").groupBy("decade", "genres").agg({"runtimeMinutes": "avg"}).orderBy("decade")

    decade_df.write.csv("gs://imdb-dataframes-results/results/title.ratings/2", mode="overwrite", header=True, sep="|")

def processMostVoted(ratings_df, title_basics_df):
    joined_df = ratings_df.join(title_basics_df, "tconst")

    window_spec = Window.partitionBy("genres").orderBy(col("numVotes").desc())

    ratings_genre_df = joined_df.withColumn("cumulative_ratings", sum(
        "numVotes").over(window_spec)).orderBy("cumulative_ratings", ascending=False).limit(100)

    ratings_genre_df.drop('startYear').drop('genres').drop(
        'endYear').drop('runtimeMinutes').write.csv("gs://imdb-dataframes-results/results/title.ratings/3", mode="overwrite", header=True, sep="|")

def processGenresAverage(ratings_df, title_basics_df):
    avg_rating_per_year_genre = ratings_df.join(title_basics_df, "tconst").filter(
        "genres IN ('Action', 'History', 'Crime', 'Sci-Fi')").groupBy("startYear", "genres").agg({"averageRating": "avg"}).orderBy('startYear')

    avg_rating_per_year_genre.write.csv("gs://imdb-dataframes-results/results/title.ratings/4", mode="overwrite", header=True, sep="|")

def processYearCount(ratings_df, title_basics_df):
    movies_per_year = ratings_df.join(title_basics_df, "tconst").groupBy("startYear").agg(
        {"averageRating": "avg", "tconst": "count", "numVotes": "sum"}).orderBy("startYear")

    movies_per_year.write.csv("gs://imdb-dataframes-results/results/title.ratings/5", mode="overwrite", header=True, sep="|")

if __name__ == '__main__':
    ratings_df = spark.read.csv(
        'gs://imdb-dataframes/title_ratings/', sep=r"\t", header=True, nullValue='\\N')
    
    # ratings_df = ratings_df.na.drop(subset=ratings_df.columns)

    title_basics_df = spark.read.csv('gs://imdb-dataframes/title.basics.tsv',
                                             sep=r"\t",
                                             header=True,
                                             nullValue="\\N")
    
    # title_basics_df = title_basics_df.na.drop(subset=title_basics_df.columns)
    
    # processTop100(ratings_df, title_basics_df)
    # processYearAverage(ratings_df, title_basics_df)
    # processDecadeAverage(ratings_df, title_basics_df)
    processMostVoted(ratings_df, title_basics_df)
    processGenresAverage(ratings_df, title_basics_df)
    processYearCount(ratings_df, title_basics_df)
