from pyspark.sql.window import Window
from pyspark.sql.functions import desc , col, avg, explode, when
from pyspark.sql.types import IntegerType



# Як рейтинг залежить від кількості сезонів у телесеріалах?
def query_one(spark_session, f, title_episode_df, title_ratings_df, title_basics_df):

    title_episode_df = title_episode_df.withColumn("season_number", 
                                               when(col("season_number").between(0, 244),
                                                     col("season_number")).otherwise(None))
   
    total_seasons = title_episode_df.groupBy("parent_tconst").agg( {'season_number': 'max'} )
    
    title_ratings_df = title_ratings_df.select("tconst", "averageRating").dropna(subset = ["averageRating"])

    title_basics_df = title_basics_df.filter(title_basics_df['title_type'] == 'tvSeries')
    title_basics_df = title_basics_df.select("tconst",
                                             "primary_title", 'title_type').dropna(subset = ["title_type"])

    merged_df = total_seasons.join(title_ratings_df,
                                   total_seasons["parent_tconst"] == title_ratings_df["tconst"], how= "left")

    result_df = merged_df.select(f.col("parent_tconst"),
                                f.col("max(season_number)").alias("total_seasons"),
                                f.col("averageRating").alias("rating"))
    result_df = result_df.dropna(subset = ["rating"])

    result_df= result_df.join(title_basics_df,
                              result_df["parent_tconst"] == title_basics_df["tconst"], how= "left")
    
    result_df= result_df.select(f.col("primary_title").alias("TVSeries")
                                ,"total_seasons",
                                "rating").orderBy("total_seasons", ascending = [False])
    result_df.show(20)
# ------------------------------------------------------------------------------------------------------------------------------

# Які три жанри телесеріалів є найпопулярнішими серед глядачів? 
def query_two(spark_session, f, title_basics_df):

    title_basics_df = title_basics_df.filter(title_basics_df['title_type'] == 'tvSeries')
    title_basics_df = title_basics_df.select("genres",
                                            'title_type').dropna(subset = ["title_type"])
    result_df = title_basics_df.groupBy('genres').count().orderBy('count', ascending=False).limit(3)
    result_df.show()

#-------------------------------------------------------------------------------------------------------------------------------
# Які три кінострічки в Україні, США та Канаді за 2023 рік мають найвищий рейтинг?
def query_three(spark_session, f, title_basics_df,title_ratings_df, title_akas_df):

    current_year = 2023
    title_basics_df = title_basics_df.select("primary_title",
                                            "start_year",
                                            "tconst")
    title_basics_df = title_basics_df.filter(title_basics_df["start_year"] == current_year)

    title_akas_df = title_akas_df.select("title_id","region")
    title_akas_df = title_akas_df.filter(col("region").isin("US", "UA", "CN"))
    combined_df = title_basics_df.join(title_akas_df,
                                        title_akas_df["title_id"] ==title_basics_df["tconst"])

    title_ratings_df = title_ratings_df.select("tconst",
                                                "averageRating").dropna(subset = ["averageRating"])
    
    combined_df = combined_df.join(title_ratings_df, "tconst")
    
    window = Window.partitionBy('region').orderBy(f.desc('averageRating'))

    result_df = (combined_df.withColumn('max_rating', f.max(f.col('averageRating')).over(window))
                            .withColumn('rank', f.row_number().over(window))
                            .filter(f.col('rank') <= 3))

    result_df.show()

#-------------------------------------------------------------------------------------------------------------------------------
# Хто з режисерів має найбільшу кількість знятих фільмів?
def query_four(spark_session, f, title_crew_df, name_basics_df):

    title_crew_df = title_crew_df.withColumn("director_id", f.explode('directors'))
    title_crew_df = title_crew_df.filter(col("director_id").isNotNull())

    name_basics_df = name_basics_df.select("nconst", "primary_name")
    title_crew_df = title_crew_df.join(name_basics_df,
                                        name_basics_df["nconst"]== title_crew_df["director_id"])
    
    result_df = title_crew_df.groupBy('director_id',
                                       'primary_name').count().orderBy('count',
                                                                        ascending=False).limit(5)
    result_df.show()



#-------------------------------------------------------------------------------------------------------------------------------
# топ 10 людей, які зіграли найбільшу к-сть раз (будь-які ролі)

def query_five(spark_session,f, title_principals_df, name_basics_df):
    title_principals_df = title_principals_df.select("nconst","category","characters")
    
    title_principals_df = title_principals_df.filter(f.col("category").isin('actor','actress')
                                                    & (~f.array_contains(f.col("characters"), 'not stated')))

    top_actors_characters = title_principals_df.withColumn("character", f.explode('characters'))
    top_actors_characters = top_actors_characters.drop('characters')
 
    top_actors_characters = top_actors_characters.groupBy("nconst").count()

    name_basics_df = name_basics_df.select("nconst", "primary_name")
    
    top_actors_with_names = (
        top_actors_characters
        .join(name_basics_df, "nconst" , how='left')
        .select("primary_name", 'count')
        .orderBy('count', ascending=False)
        .limit(10)
    )

    top_actors_with_names.show()


#-------------------------------------------------------------------------------------------------------------------------------

# Для кожного режисера знайти його фільми перекладені українською мовою, які є вище середньої оцінки(рейтингу) усіх його фільмів
def query_six(title_akas_df, title_ratings_df, title_basics_df, title_crew_df, name_basics_df, f, Window, t, spark_session):

    title_basics_df = title_basics_df.select('tconst', 'primary_title')
    title_akas_df = title_akas_df.select('title_id', 'region', 'language')
    title_ratings_df = title_ratings_df.select('tconst', 'averageRating')
    name_basics_df = name_basics_df.select('nconst', 'primary_name')

    title_crew_df = title_crew_df.drop('writers')
    title_crew_df = title_crew_df.dropna()

    title_akas_df = title_akas_df.filter(f.col('language') == 'uk')

    join_cond = (title_akas_df['title_id'] == title_ratings_df['tconst'])
    lang_rating_df = title_akas_df.join(title_ratings_df, join_cond, how='left')
    lang_rating_df = lang_rating_df.drop('tconst')

    title_crew_df = title_crew_df.withColumn("director", f.explode('directors'))
    title_crew_df = title_crew_df.drop('directors')

    
    join_cond = (lang_rating_df['title_id'] == title_crew_df['tconst'])
    lang_rating_director_df = lang_rating_df.join(title_crew_df, join_cond, how='left')
    lang_rating_director_df = lang_rating_director_df.drop('title_id')
    lang_rating_director_df = lang_rating_director_df.dropna(subset=['averageRating', 'director'])

    window = Window.partitionBy('director').orderBy('director', 'averageRating')
    lang_rating_director_df = (lang_rating_director_df.withColumn('avg_rating', f.avg(f.col('averageRating')).over(window))
                                                        .filter(f.col('avg_rating') < f.col('averageRating'))) 


    lang_rating_director_df = lang_rating_director_df.join(title_basics_df, on='tconst', how='left')
    lang_rating_director_df.show()

    window_spec = Window.partitionBy('director')
    movies_in_list_df = (lang_rating_director_df.withColumn('films_collection', 
                                                    f.collect_list('primary_title').over(window_spec))
                                                .withColumn('average_rating',
                                                    f.avg('averageRating').over(window_spec)))
    
    movies_in_list_dedupl_df = movies_in_list_df.dropDuplicates(subset=['director'])


    movies_in_str_df = movies_in_list_dedupl_df.withColumn('films_collection_higher',
                                                            f.concat_ws(", ", 
                                                                        f.col('films_collection')))
    lang_rating_director_df = movies_in_str_df.select('director', 'average_rating',
                                                        'language', 'films_collection_higher', 'films_collection')

    join_cond = (name_basics_df['nconst'] == lang_rating_director_df['director'])
    lang_rating_director_df = (
        lang_rating_director_df
        .join(name_basics_df, join_cond, how='left')
        .select(f.col('primary_name').alias('director_name'), 'average_rating', 'language', 'films_collection_higher', 'films_collection')
    )
    lang_rating_director_df = (lang_rating_director_df.withColumn('films_quantity', f.size(f.col('films_collection')))
                                                        .drop('films_collection')
                                                        .orderBy(['average_rating', 'films_quantity'], ascending=[False, False])
                                                        .withColumn('average_rating', f.round('average_rating', 2)))

    lang_rating_director_df.show()
