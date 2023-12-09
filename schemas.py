import pyspark.sql.types as t
import columns as c
import columns.columns_title_principals as columns_title_principals
import columns.columns_title_ratings as columns_title_ratings
import columns.columns_title_basics as columns_title_basics

# --------------------------------------- NAME BASICS -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету
schema_name_basics = t.StructType(
    [
        t.StructField("nconst", t.StringType(), True),
        t.StructField("primaryName", t.StringType(), True),
        t.StructField("birthYear", t.IntegerType(), True),
        t.StructField("deathYear", t.IntegerType(), True),
        t.StructField("primaryProfession", t.StringType(), True),
        t.StructField("knownForTitles", t.StringType(), True),
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_name_basics_final = t.StructType(
    [
        t.StructField(c.columns_name_basics.nconst, t.StringType(), True),
        t.StructField(c.columns_name_basics.primary_name, t.StringType(), True),
        t.StructField(c.columns_name_basics.primary_profession, t.StringType(), True),
        t.StructField(c.columns_name_basics.known_for_titles, t.StringType(), True),
    ]
)

# --------------------------------------- TITLE AKAS -----------------------------------------




# --------------------------------------- TITLE PRINCIPLES -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету
schema_title_principals = t.StructType(
    [
        t.StructField("tconst", dataType=t.StringType()),  # поле в структурі
        t.StructField("ordering", dataType=t.IntegerType()),  # поле в структурі
        t.StructField("nconst", dataType=t.StringType()),  # поле в структурі
        t.StructField("category", dataType=t.StringType()),  # поле в структурі
        t.StructField("job", dataType=t.StringType()), # поле в структурі
        t.StructField("characters", dataType=t.StringType()), # поле в структурі
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_principals_final = t.StructType(
    [
        t.StructField(columns_title_principals.tconst, t.StringType(), True),
        t.StructField(columns_title_principals.ordering, t.IntegerType(), True),
        t.StructField(columns_title_principals.nconst, t.StringType(), True),
        t.StructField(columns_title_principals.category, t.StringType(), True),
        t.StructField(columns_title_principals.job, t.StringType(), True),
        t.StructField(columns_title_principals.characters, t.StringType(), True),
    ]
)

# --------------------------------------- TITLE RATINGS -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету

schema_title_ratings = t.StructType(
    [
        t.StructField("tconst", dataType=t.StringType()),  # поле в структурі
        t.StructField("averageRating", dataType=t.StringType()),  # поле в структурі
        t.StructField("numVotes", dataType=t.StringType()),  # поле в структурі
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_ratings_final = t.StructType(
    [
        t.StructField(columns_title_ratings.tconst, t.StringType(), True),
        t.StructField(columns_title_ratings.averageRating, t.StringType(), True),
        t.StructField(columns_title_ratings.numVotes, t.StringType(), True),
    ]
)

# --------------------------------------- TITLE BASICS -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету

schema_title_basics = t.StructType(
    [
        t.StructField("tconst", dataType=t.StringType()),  # поле в структурі
        t.StructField("titleType", dataType=t.StringType()), # поле в структурі
        t.StructField("primaryTitle", dataType=t.StringType()),  # поле в структурі
        t.StructField("originalTitle", dataType=t.StringType()),  # поле в структурі
        t.StructField("isAdult", dataType=t.StringType()),  # поле в структурі
        t.StructField("startYear", dataType=t.StringType()),  # поле в структурі
        t.StructField("endYear", dataType=t.StringType()),  # поле в структурі
        t.StructField("runtimeMinutes", dataType=t.StringType()),  # поле в структурі
        t.StructField("genres", dataType=t.StringType()),  # поле в структурі
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_basics_final = t.StructType(
    [
        t.StructField(columns_title_basics.tconst, t.StringType(), True),
        t.StructField(columns_title_basics.primaryTitle, t.StringType(), True),
        t.StructField(columns_title_basics.startYear, t.StringType(), True),
        t.StructField(columns_title_basics.runtimeMinutes, t.StringType(), True),
        t.StructField(columns_title_basics.genres, t.StringType(), True),
        t.StructField(columns_title_basics.originalTitle, t.StringType(), True),
        t.StructField(columns_title_basics.isAdult, t.StringType(), True),
        t.StructField(columns_title_basics.endYear, t.StringType(), True),
    ]
)
