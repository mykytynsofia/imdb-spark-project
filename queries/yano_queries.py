
import os
from useful_functions import create_folder
import columns.columns_name_basics as c_name_basics
import columns.columns_title_akas as c_title_akas
import columns.columns_title_basics as c_title_basics
import columns.columns_title_crew as c_title_crew
import columns.columns_title_episode as c_title_episode
import columns.columns_title_principals as c_title_principals
import columns.columns_title_ratings as c_title_ratings
from datasets_paths import QUERIES_RESULTS_PATH

#* ------------------------------------------------ HELPER FUNCTIONS ------------------------------------------------

def get_schema(query_n, t):
    schemas =  {
        1:  t.StructType(
            [
                t.StructField(c_title_basics.tconst, t.StringType(), True),
                t.StructField(c_title_basics.primary_title, t.StringType(), True),
                t.StructField(c_title_basics.title_type, t.StringType(), True),
                t.StructField(c_title_basics.start_year, t.IntegerType(), True),
                t.StructField(c_title_ratings.averageRating, t.FloatType(), True),
                t.StructField("genre", t.StringType(), True),
            ]),
        2: t.StructType(
            [
                t.StructField(c_name_basics.primary_name, t.StringType(), True),
                t.StructField("num_characters", t.IntegerType(), True),
            ]),
        3: t.StructType(
            [
                t.StructField(c_name_basics.primary_name, t.StringType(), True),
                t.StructField(c_title_principals.characters, t.StringType(), True),
                t.StructField("count_characters_by_actor", t.IntegerType(), True),
                t.StructField("avarage_rating_by_role", t.FloatType(), True),    
            ]),
        4: t.StructType(
            [
                t.StructField(c_title_basics.start_year, t.IntegerType(), True),
                t.StructField("avg_rating", t.FloatType(), True),
                t.StructField("avg_num_votes", t.FloatType(), True),    
            ]),
        5: t.StructType(
            [
                t.StructField(c_title_episode.season_number, t.IntegerType(), True),
                t.StructField("avg_rating", t.FloatType(), True),
            ]), 
        6: t.StructType(
            [
                t.StructField("genre", t.StringType(), True),
                t.StructField(c_title_akas.region, t.StringType(), True),
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
    queryNumber = 1
    path = f'{QUERIES_RESULTS_PATH}yano_query_{queryNumber}'
    schema_query_res = get_schema(queryNumber, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_basics_df = title_basics_df.select(c_title_basics.tconst, c_title_basics.primary_title,
                                            c_title_basics.title_type, c_title_basics.start_year,
                                            c_title_basics.genres)
    recent_movies = title_basics_df.filter((f.col(c_title_basics.title_type) == "movie") 
                                            & (f.col(c_title_basics.start_year) >= 2018))

    title_ratings_df = title_ratings_df.select(c_title_basics.tconst, c_title_ratings.averageRating)
    top_rated_movies = (recent_movies.join(title_ratings_df, on=c_title_basics.tconst)
                                    .orderBy([c_title_ratings.averageRating, c_title_basics.start_year],
                                            ascending=[False, False])) 

    # Explode the genres array into separate rows
    top_rated_movies_expanded_genre = top_rated_movies.withColumn("genre", f.explode(c_title_basics.genres))
    top_rated_movies_expanded_genre = top_rated_movies_expanded_genre.withColumn("genre",
                                                                                f.trim(f.col("genre")))

    top_rated_movies_expanded_genre = top_rated_movies_expanded_genre.drop(c_title_basics.genres)
    # top_rated_movies_expanded_genre.show()

    # distinct_genres = top_rated_movies_expanded_genre.select("genre").distinct()
    # distinct_genres.show()

    window_spec = Window.partitionBy("genre").orderBy(f.desc(c_title_ratings.averageRating))

    # Use window function to get the max average rating for each genre
    top_rated_movies_by_genre = (top_rated_movies_expanded_genre
                                    .withColumn("max_average_rating",
                                                f.max(c_title_ratings.averageRating).over(window_spec))
                                    .withColumn('rank',
                                                f.row_number().over(window_spec))
                                    .filter(f.col('rank') == 1)
                                    ) 
    
    top_rated_movies_by_genre = top_rated_movies_by_genre.drop('rank', 'max_average_rating')
    top_rated_movies_by_genre = top_rated_movies_by_genre.filter(~(f.col('genre') == ''))

    # Save to csv file
    save_query_to_file(top_rated_movies_by_genre, path)

    return top_rated_movies_by_genre


#% Хто входить до 10-ки акторів, які зіграли найбільше персонажів у різних фільмах?
def query_two(spark_session, f, title_principals_df, name_basics_df, t):
    queryNumber = 2
    path = f'{QUERIES_RESULTS_PATH}yano_query_{queryNumber}'
    schema_query_res = get_schema(queryNumber, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_principals_df = title_principals_df.select(c_title_principals.nconst, c_title_principals.characters)
    top_actors_characters = (title_principals_df.groupBy(c_title_principals.nconst)
                        .agg(f.countDistinct(c_title_principals.characters)
                            .alias('num_characters'))
                        .orderBy('num_characters', ascending=False)
                        .limit(10))
    
    name_basics_df = name_basics_df.select(c_name_basics.nconst, c_name_basics.primary_name)
    # Join with name_basics_df to get the names of the top actors
    top_actors_with_names = (
        top_actors_characters
        .join(name_basics_df, c_name_basics.nconst , how='left')
        .select(c_name_basics.primary_name, 'num_characters')
        .orderBy('num_characters', ascending=False)
    )

    # Save to csv file
    save_query_to_file(top_actors_with_names, path)

    return top_actors_with_names


#% Для кожного актора дістати, його роль (characters), яка показала найкращий середній рейтинг кінострічок
def query_three(spark_session, f, title_principals_df, title_ratings_df, name_basics_df, Window, t):
    queryNumber = 3
    path = f'{QUERIES_RESULTS_PATH}yano_query_{queryNumber}'
    schema_query_res = get_schema(queryNumber, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_principals_df = title_principals_df.withColumn(c_title_principals.characters,
                                                    f.when(f.array_contains(f.col(c_title_principals.characters),
                                                            "not stated"), None)
                                                    .otherwise(f.col(c_title_principals.characters))
                                                    )
        

    window = (Window.partitionBy(c_name_basics.nconst, c_title_principals.characters)
                    .orderBy(c_name_basics.nconst, c_title_principals.characters))

    actors_and_characters = (title_principals_df.filter(f.col(c_title_principals.characters).isNotNull())
                                                .withColumn('count_characters_by_actor',
                                                            f.count(f.col(c_title_principals.characters)).over(window)))
    
    # actors_and_characters.show()

    join_condition = title_ratings_df[c_title_ratings.tconst] == actors_and_characters[c_title_ratings.tconst]
    # Додатково можна долучити рейтинг творів, в яких брали участь ці актори та грали вказану роль.
    top_actors_and_ratings = (actors_and_characters.join(title_ratings_df, join_condition, how='left')
                                                        .select(c_name_basics.nconst,
                                                                c_title_principals.characters,
                                                                "count_characters_by_actor", 
                                                                c_title_ratings.averageRating)
                                                        ) # .orderBy(["count_characters_by_actor"], ascending=[False])

    # top_actors_and_ratings.show()

    top_actors_and_ratings = (top_actors_and_ratings.groupBy(c_name_basics.nconst,
                                                            c_title_principals.characters,
                                                            "count_characters_by_actor")
                                                    .agg(f.avg(c_title_ratings.averageRating)
                                                    .alias('avarage_rating_by_role'))
                                                    .orderBy([c_name_basics.nconst,
                                                                'count_characters_by_actor',
                                                                'avarage_rating_by_role']))
    # top_actors_and_ratings.show(100)

    window = (Window.partitionBy(c_name_basics.nconst)
                    .orderBy(top_actors_and_ratings['avarage_rating_by_role'].desc()))
    best_roles = top_actors_and_ratings.withColumn('max_avarage_rating_by_role',
                                                    f.max(f.col('avarage_rating_by_role')).over(window))
    best_roles = best_roles.filter(f.col('avarage_rating_by_role') == f.col('max_avarage_rating_by_role'))
    best_roles = best_roles.drop('max_avarage_rating_by_role')

    # Join with name_basics_df to get the names of the top actors
    actors_with_names = (
        best_roles
        .join(name_basics_df, 'nconst' , how='left')
        .select(c_name_basics.primary_name, c_title_principals.characters,
                'count_characters_by_actor', 'avarage_rating_by_role')
    )

    actors_with_names = actors_with_names.withColumn(c_title_principals.characters,
                                                    f.concat_ws(", ", f.col(c_title_principals.characters)))

    actors_with_names = actors_with_names.orderBy(['count_characters_by_actor', 'avarage_rating_by_role'],
                                                    ascending=[False,False] )

    # Save to csv file
    save_query_to_file(actors_with_names, path)
    return actors_with_names


#% Побачити, як змінювалися середні рейтинги фільмів режисера Quentin Tarantino по роках, а також середня к-сть відгуків
def query_four(spark_session, title_crew_df, title_ratings_df, title_basics_df, name_basics_df, f, t):
    queryNumber = 4
    path = f'{QUERIES_RESULTS_PATH}yano_query_{queryNumber}'
    schema_query_res = get_schema(queryNumber, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready


    target_producer = 'Quentin Tarantino'
    # target_producer = 'Christopher Nolan'
    target_nconst = (name_basics_df.filter(f.col(c_name_basics.primary_name) == target_producer)
                                    .select(c_name_basics.nconst)
                                    .first()[c_name_basics.nconst])


    # Select relevant columns from dataframes
    title_crew_df = title_crew_df.select(c_title_crew.tconst, 
                                        c_title_crew.directors)
    title_basics_df = title_basics_df.select(c_title_basics.tconst,
                                            c_title_basics.start_year, 
                                            c_title_basics.primary_title)

    # Filter films by Quentin Tarantino
    target_ratings = (
        title_crew_df.filter(f.array_contains(f.col(c_title_crew.directors), target_nconst))
                    .join(title_ratings_df, on=c_title_ratings.tconst, how='left')
                    .join(title_basics_df, on=c_title_basics.tconst, how='left')
    )

    target_ratings.show()

    # Calculate average rating 
    average_rating = (
        target_ratings
        .groupBy(c_title_basics.start_year)
        .agg(f.avg(c_title_ratings.averageRating).alias("avg_rating"),
            f.avg(c_title_ratings.numVotes).alias("avg_num_votes"))
        .orderBy(c_title_basics.start_year)
    )

    average_rating = average_rating.dropna()

    # Save to CSV file
    save_query_to_file(average_rating, path)

    return average_rating

#% Як змінювався рейтинг сезонів серіалу "Гра Престолів" 
def query_five(title_episode_df,  title_ratings_df, title_basics_df, f, t, spark_session):
    queryNumber = 5
    path = f'{QUERIES_RESULTS_PATH}yano_query_{queryNumber}'
    schema_query_res = get_schema(queryNumber, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    tvseries_name = 'Game of Thrones'
    # filtered_df = title_basics_df.filter((f.lower(title_basics_df['primary_title'])
    #                                         .like('%game of thrones%'))
    filtered_df = title_basics_df.filter((f.col(c_title_basics.primary_title) == tvseries_name)
                                        & (f.col(c_title_basics.title_type) == 'tvSeries'))
    # filtered_df.show(truncate=False)
    target_tconst = filtered_df.select(c_title_basics.tconst).first()[c_title_basics.tconst]
    
    got_episodes_df = title_episode_df.filter(f.col(c_title_episode.parent_tconst) == target_tconst)

    # got_episodes_df.show(truncate=False)
    # got_episodes_df.describe().show()

    # Об'єднання таблиць episode_df та ratings_df за tconst
    merged_df = got_episodes_df.join(title_ratings_df, c_title_basics.tconst, "left")

    # Групування за номером сезону та обчислення середнього рейтингу
    season_avg_ratings = (merged_df.groupBy(c_title_episode.season_number)
                                    .agg(f.avg(c_title_ratings.averageRating).alias("avg_rating"))
                                    .orderBy(c_title_episode.season_number))

    # Save to CSV file
    save_query_to_file(season_avg_ratings, path)

    return season_avg_ratings


#% Дізнатися середній рейтинг та к-сть відгуків найулюбленішого жанру кожної країни 
def query_six(title_akas_df, title_ratings_df, title_basics_df, f, t, spark_session, Window):
    queryNumber = 6
    path = f'{QUERIES_RESULTS_PATH}yano_query_{queryNumber}'
    schema_query_res = get_schema(queryNumber, t)
    df_ready = check_if_already_did_query(path, schema_query_res, spark_session)
    if df_ready: return df_ready

    title_basics_df = title_basics_df.select(c_title_basics.tconst, c_title_basics.genres)
    title_akas_df = title_akas_df.select(c_title_akas.title_id, c_title_akas.region)

    join_condition = (title_akas_df[c_title_akas.title_id] == title_basics_df[c_title_basics.tconst])

    # Яка кількість оцінок та середній рейтинг фільмів за жанрами в різних регіонах?
    genre_ratings_by_region = (title_basics_df.join(title_ratings_df, c_title_basics.tconst, how='left')
                                                .join(title_akas_df, join_condition, how='left')
                                                .dropna(subset=[c_title_akas.region])
                                                .withColumn("genre", f.explode(c_title_basics.genres))
                                                .groupBy("genre", c_title_akas.region)
                                                .agg(f.avg(c_title_ratings.averageRating).alias("avg_rating"),
                                                    f.sum(c_title_ratings.numVotes).alias("total_votes"))
                                                .orderBy([c_title_akas.region, 'avg_rating'], ascending=[True, False]))


    window = Window.partitionBy(c_title_akas.region).orderBy(f.col('avg_rating').desc())

    genre_ratings_by_region = (genre_ratings_by_region.withColumn('rank', f.row_number().over(window))
                                                        .filter(f.col('rank') == 1))

    genre_ratings_by_region = genre_ratings_by_region.drop('rank')
    genre_ratings_by_region = genre_ratings_by_region.filter(~(f.col('genre') == ''))

    # Save to CSV file
    save_query_to_file(genre_ratings_by_region, path)

    return genre_ratings_by_region
