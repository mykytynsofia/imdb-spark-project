
from useful_functions import (create_folder)
import os

#* ------------------------------------------------ HELPER FUNCTIONS ------------------------------------------------

def get_schema(query_n, t):
    schemas =  {
        1:  t.StructType(
            [
                t.StructField("tconst", t.StringType(), True),
                t.StructField("primary_title", t.StringType(), True),
                t.StructField("title_type", t.StringType(), True),
                t.StructField("start_year", t.IntegerType(), True),
                t.StructField("averageRating", t.FloatType(), True),
                t.StructField("genre", t.StringType(), True),
            ]),
        2: t.StructType(
            [
                t.StructField("primary_name", t.StringType(), True),
                t.StructField("num_characters", t.IntegerType(), True),
            ]),
        3: t.StructType(
            [
                t.StructField("primary_name", t.StringType(), True),
                t.StructField("characters", t.StringType(), True),
                t.StructField("count_characters_by_actor", t.IntegerType(), True),
                t.StructField("avarage_rating_by_role", t.FloatType(), True),    
            ]),
        4: t.StructType(
            [
                t.StructField("start_year", t.IntegerType(), True),
                t.StructField("avg_rating", t.FloatType(), True),
                t.StructField("avg_num_votes", t.FloatType(), True),    
            ]),
        5: t.StructType(
            [
                t.StructField("season_number", t.IntegerType(), True),
                t.StructField("avg_rating", t.FloatType(), True),
            ]), 
        6: t.StructType(
            [
                t.StructField("genre", t.StringType(), True),
                t.StructField("region", t.StringType(), True),
                t.StructField("avg_rating", t.FloatType(), True),    
                t.StructField("total_votes", t.IntegerType(), True),    
            ])               
    }
    return schemas.get(query_n)


def check_if_already_did_query(path, schema_query_res, spark_session):
    if os.path.exists(path):
        print(f"You've already did query [{path[-1]}]:")
        return spark_session.read.csv(path,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N", 
                                    schema=schema_query_res)
    return False   


def save_query_to_file(df, path):
    folder_name, file_name = os.path.split(path)
    create_folder(folder_name, file_name)
    print(f'Saving to {path} ...')
    df.write.csv(path,
                header=True, 
                mode='overwrite', 
                sep='\t')  


#* ------------------------------------------------ BUSINESS QUESTIONS ------------------------------------------------

#% Дістати фільми з 2018 і вище року випуску, які мають найвищий рейтинг у своєму жанрі  
def query_one(title_basics_df, title_ratings_df, spark_session, Window, f, t):
    path = './queries_results/yano_query_1'
    schema_query_res = get_schema(1, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_basics_df = title_basics_df.select('tconst', 'primary_title', 'title_type', 'start_year', 'genres')
    recent_movies = title_basics_df.filter((f.col("title_type") == "movie") & (f.col("start_year") >= 2018))

    title_ratings_df = title_ratings_df.select('tconst', 'averageRating')
    top_rated_movies = (recent_movies.join(title_ratings_df, on="tconst")
                                    .orderBy(["averageRating",'start_year'], ascending=[False, False])
                                    ) 

    # Explode the genres array into separate rows
    top_rated_movies_expanded_genre = top_rated_movies.withColumn("genre", f.explode("genres"))
    top_rated_movies_expanded_genre = top_rated_movies_expanded_genre.withColumn("genre",
                                                                                f.trim(f.col("genre")))

    top_rated_movies_expanded_genre = top_rated_movies_expanded_genre.drop('genres')
    # top_rated_movies_expanded_genre.show()

    # distinct_genres = top_rated_movies_expanded_genre.select("genre").distinct()
    # distinct_genres.show()


    window_spec = Window.partitionBy("genre").orderBy(f.desc("averageRating"))

    # Use window function to get the max average rating for each genre
    top_rated_movies_by_genre = (top_rated_movies_expanded_genre.withColumn("max_average_rating",
                                                        f.max("averageRating").over(window_spec))
                .withColumn('rank', f.row_number().over(window_spec))
                .filter(f.col('rank') == 1)
                ) 
    
    top_rated_movies_by_genre = top_rated_movies_by_genre.drop('rank', 'max_average_rating')
    top_rated_movies_by_genre = top_rated_movies_by_genre.filter(~(f.col('genre') == ''))

    # Save to csv file
    save_query_to_file(top_rated_movies_by_genre, path)

    return top_rated_movies_by_genre


#% Хто входить до 10-ки акторів, які зіграли найбільше персонажів у різних фільмах?
def query_two(spark_session, f, title_principals_df, name_basics_df, t):
    path = './queries_results/yano_query_2'
    schema_query_res = get_schema(2, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_principals_df = title_principals_df.select('nconst', 'characters')
    top_actors_characters = (title_principals_df.groupBy('nconst')
                        .agg(f.countDistinct('characters')
                            .alias('num_characters'))
                        .orderBy('num_characters', ascending=False)
                        .limit(10))
    
    name_basics_df = name_basics_df.select('nconst', 'primary_name')
    # Join with name_basics_df to get the names of the top actors
    top_actors_with_names = (
        top_actors_characters
        .join(name_basics_df, 'nconst' , how='left')
        .select('primary_name', 'num_characters')
        .orderBy('num_characters', ascending=False)
    )

    # Save to csv file
    save_query_to_file(top_actors_with_names, path)

    return top_actors_with_names


#% Для кожного актора дістати, його роль (characters), яка показала найкращий середній рейтинг кінострічок
def query_three(spark_session, f, title_principals_df, title_ratings_df, name_basics_df, Window, t):
    path = './queries_results/yano_query_3'
    schema_query_res = get_schema(3, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_principals_df = title_principals_df.withColumn("characters",
                                                    f.when(f.array_contains(f.col("characters"), "not stated"), None)
                                                    .otherwise(f.col("characters"))
                                                    )
        

    window = Window.partitionBy('nconst', 'characters').orderBy('nconst', 'characters')

    actors_and_characters = (title_principals_df.filter(f.col("characters").isNotNull())
                                                .withColumn('count_characters_by_actor', f.count(f.col('characters')).over(window)))
    
    # actors_and_characters.show()

    join_condition = title_ratings_df["tconst"] == actors_and_characters["tconst"]
    # Додатково можна долучити рейтинг творів, в яких брали участь ці актори та грали вказану роль.
    top_actors_and_ratings = (actors_and_characters.join(title_ratings_df, join_condition, how='left')
                                                        .select("nconst",
                                                                "characters",
                                                                "count_characters_by_actor", 
                                                                "averageRating")
                                                        ) # .orderBy(["count_characters_by_actor"], ascending=[False])

    # top_actors_and_ratings.show()

    top_actors_and_ratings = (top_actors_and_ratings.groupBy("nconst",
                                                            "characters",
                                                            "count_characters_by_actor")
                                                    .agg(f.avg('averageRating')
                                                    .alias('avarage_rating_by_role'))
                                                    .orderBy(['nconst', 'count_characters_by_actor', 'avarage_rating_by_role']))
    # top_actors_and_ratings.show(100)

    window = Window.partitionBy('nconst').orderBy(top_actors_and_ratings['avarage_rating_by_role'].desc())
    best_roles = top_actors_and_ratings.withColumn('max_avarage_rating_by_role',
                                                   f.max(f.col('avarage_rating_by_role')).over(window))
    best_roles = best_roles.filter(f.col('avarage_rating_by_role') == f.col('max_avarage_rating_by_role'))
    best_roles = best_roles.drop('max_avarage_rating_by_role')

        # Join with name_basics_df to get the names of the top actors
    actors_with_names = (
        best_roles
        .join(name_basics_df, 'nconst' , how='left')
        .select('primary_name', 'characters', 'count_characters_by_actor', 'avarage_rating_by_role')
    )

    actors_with_names = actors_with_names.withColumn("characters", f.concat_ws(", ", f.col("characters")))

    actors_with_names = actors_with_names.orderBy(['count_characters_by_actor', 'avarage_rating_by_role'], ascending=[False,False] )

    # Save to csv file
    save_query_to_file(actors_with_names, path)
    return actors_with_names


#% Побачити, як змінювалися середні рейтинги фільмів режисера Quentin Tarantino по роках, а також середня к-сть відгуків
def query_four(spark_session, title_crew_df, title_ratings_df, title_basics_df, name_basics_df, Window, f, t):
    path = './queries_results/yano_query_4'
    schema_query_res = get_schema(4, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready


    target_producer = 'Quentin Tarantino'
    # target_producer = 'Christopher Nolan'
    target_nconst = name_basics_df.filter(f.col('primary_name') == target_producer).select('nconst').first()['nconst']


    # Select relevant columns from dataframes
    title_crew_df = title_crew_df.select('tconst', 'directors')
    title_basics_df = title_basics_df.select('tconst', 'start_year', 'primary_title')

    # Filter films by Quentin Tarantino
    target_ratings = (
        title_crew_df.filter(f.array_contains(f.col("directors"), target_nconst))
        .join(title_ratings_df, on="tconst", how='left')
        .join(title_basics_df, on="tconst", how='left')
    )

    target_ratings.show()

    # Calculate average rating 
    average_rating = (
        target_ratings
        .groupBy("start_year")
        .agg(f.avg("averageRating").alias("avg_rating"), f.avg("numVotes").alias("avg_num_votes"))
        .orderBy("start_year")
    )

    average_rating = average_rating.dropna()

    # Save to CSV file
    save_query_to_file(average_rating, path)

    return average_rating

#% Як змінювався рейтинг сезонів серіалу "Гра Престолів" 
def query_five(title_episode_df,  title_ratings_df, title_basics_df, f, t, spark_session):
    path = './queries_results/yano_query_5'
    schema_query_res = get_schema(5, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    tvseries_name = 'Game of Thrones'
    # filtered_df = title_basics_df.filter((f.lower(title_basics_df['primary_title'])
    #                                         .like('%game of thrones%'))
    filtered_df = title_basics_df.filter((f.col('primary_title') == tvseries_name)
                                        & (f.col('title_type') == 'tvSeries'))
    # filtered_df.show(truncate=False)
    target_tconst = filtered_df.select('tconst').first()['tconst']
    
    got_episodes_df = title_episode_df.filter(f.col("parent_tconst") == target_tconst)

    # got_episodes_df.show(truncate=False)
    # got_episodes_df.describe().show()
    # +-------+---------+-------------+-----------------+-----------------+
    # |summary|   tconst|parent_tconst|    season_number|   episode_number|
    # +-------+---------+-------------+-----------------+-----------------+
    # |  count|       73|           73|               73|               73|
    # |   mean|     null|         null|4.205479452054795|5.191780821917808|
    # | stddev|     null|         null|2.191897352305563|2.821827295322499|
    # |    min|tt1480055|    tt0944947|                1|                1|
    # |    max|tt6027920|    tt0944947|                8|               10|
    # +-------+---------+-------------+-----------------+-----------------+

    # Об'єднання таблиць episode_df та ratings_df за tconst
    merged_df = got_episodes_df.join(title_ratings_df, "tconst", "left")

    # Групування за номером сезону та обчислення середнього рейтингу
    season_avg_ratings = (merged_df.groupBy("season_number")
                                    .agg(f.avg("averageRating").alias("avg_rating"))
                                    .orderBy('season_number'))

    # Save to CSV file
    save_query_to_file(season_avg_ratings, path)

    return season_avg_ratings


#% Дізнатися середній рейтинг та к-сть відгуків найулюбленішого жанру кожної країни 
def query_six(title_akas_df, title_ratings_df, title_basics_df, f, t, spark_session, Window):
    path = './queries_results/yano_query_6'
    schema_query_res = get_schema(6, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_basics_df = title_basics_df.select('tconst', 'genres')
    title_akas_df = title_akas_df.select('title_id', 'region')

    join_condition = (title_akas_df['title_id'] == title_basics_df['tconst'])

    # Яка кількість оцінок та середній рейтинг фільмів за жанрами в різних регіонах?
    genre_ratings_by_region = (title_basics_df.join(title_ratings_df, "tconst", how='left')
                                                .join(title_akas_df, join_condition, how='left')
                                                .dropna(subset=['region'])
                                                .withColumn("genre", f.explode("genres"))
                                                .groupBy("genre", "region")
                                                .agg(f.avg("averageRating").alias("avg_rating"),
                                                    f.sum("numVotes").alias("total_votes"))
                                                .orderBy(['region', 'avg_rating'], ascending=[True, False]))


    window = Window.partitionBy('region').orderBy(f.col('avg_rating').desc())

    genre_ratings_by_region = (genre_ratings_by_region.withColumn('rank', f.row_number().over(window))
                                                        .filter(f.col('rank') == 1))

    genre_ratings_by_region = genre_ratings_by_region.drop('rank')
    genre_ratings_by_region = genre_ratings_by_region.filter(~(f.col('genre') == ''))

    # Save to CSV file
    save_query_to_file(genre_ratings_by_region, path)

    return genre_ratings_by_region
