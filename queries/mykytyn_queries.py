from pyspark.sql import Window
from pyspark.sql.functions import col, stddev, avg, rank, count, desc, asc, array_contains


# Get the total number of episodes for each TV series where the startYear is after 2000.
def totalYearAfter2000(title_episode_df, title_basics_df, name_basics_df):
    joined_df = title_episode_df.join(title_basics_df, title_episode_df.tconst == title_basics_df.tconst, "left_outer")

    result_df = joined_df.join(name_basics_df, joined_df.parent_tconst == name_basics_df.nconst, "left_outer")

    filtered_df = result_df.filter((result_df.start_year > 2000) & (result_df.episode_number != -1))

    grouped_df = filtered_df.groupBy("parent_tconst", "primary_title").agg({"episode_number": "sum"}).withColumnRenamed(
        "sum(episode_number)", "totalEpisodes"
    )

    return grouped_df.show()


# Top 10 TV Series with Lowest Episode Numbers:
def top10LowestEpisodes(title_episode_df, title_basics_df):
    joined_df = title_episode_df.join(title_basics_df, title_episode_df.tconst == title_basics_df.tconst)

    filtered_df = joined_df.filter(col('episode_number') > 0)

    total_episodes_df = filtered_df.groupBy(title_basics_df.primary_title).agg({'episode_number': 'sum'})

    sorted_df = total_episodes_df.sort(asc('sum(episode_number)'))

    top_10_series = sorted_df.limit(10)

    top_10_series.show()


# Episodes with the Longest Runtime:
def longestRuntime(title_episode_df, title_basics_df):
    result_df = title_episode_df.join(title_basics_df,
                                      title_episode_df.parent_tconst == title_basics_df.tconst).groupBy(
        "parent_tconst", "primary_title").agg({"runtime_minutes": "max"}).orderBy("max(runtime_minutes)",
                                                                                  ascending=False).limit(
        10).withColumnRenamed(
        "max(runtime_minutes)", "maxRuntimeMinutes")
    result_df.show()


# Total Number of Episodes for Each Genre:
def totalEpisodesEachGenre(title_episode_df, title_basics_df):
    result_df = (title_episode_df.
                 join(title_basics_df, title_episode_df.parent_tconst == title_basics_df.tconst)
                 .groupBy("genres")
                 .agg({"episode_number": "sum"})
                 .withColumnRenamed("sum(episode_number)", "totalEpisodesByGenre"))
    result_df.show()


# Total Episodes for Game of Thrones
def totalEpisodesForGameOfThrones(title_episode_df, title_basics_df):
    game_of_thrones_df = title_basics_df.filter(col("primary_title") == "Game of Thrones")

    result_df = title_episode_df.join(game_of_thrones_df,
                                      title_episode_df.parent_tconst == game_of_thrones_df.tconst).groupBy(
        "parent_tconst", "primary_title").agg({"episode_number": "sum"}).withColumnRenamed("sum(episode_number)",
                                                                                           "totalEpisodes")

    result_df.show()

def topComedyActors(title_principals_df, title_basic_df, title_name_df):
    comedy_actors = (
        title_principals_df
        .join(title_basic_df, title_principals_df['tconst'] == title_basic_df['tconst'], 'inner')
        .filter((array_contains(title_basic_df['genres'], 'Comedy')) & (title_principals_df["category"] == "actor"))
        .groupBy(title_principals_df['nconst'])
        .agg(count(title_principals_df['tconst']).alias("numComedyFilms"))
        .orderBy(desc("numComedyFilms"))
    )
    result_df = (
        comedy_actors
        .join(title_name_df, comedy_actors['nconst'] == title_name_df['nconst'], 'inner')
        .select(comedy_actors['numComedyFilms'], title_name_df['primary_name'])
    )

    result_df.show()