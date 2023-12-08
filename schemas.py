import pyspark.sql.types as t
import columns as c

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

# Схема, яка використовуватиметься для першого зчитування датасету
schema_title_akas = t.StructType(
    [
        t.StructField("titleId", t.StringType(), True),
        t.StructField("ordering", t.IntegerType(), True),
        t.StructField("title", t.StringType(), True),
        t.StructField("region", t.StringType(), True),
        t.StructField("language", t.StringType(), True),
        t.StructField("types", t.StringType(), True),
        t.StructField("attributes", t.StringType(), True),
        t.StructField("isOriginalTitle", t.IntegerType(), True),
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_akas_final = t.StructType(
    [
        t.StructField(c.columns_title_akas.title_id, t.StringType(), True),
        t.StructField(c.columns_title_akas.ordering, t.IntegerType(), True),
        t.StructField(c.columns_title_akas.title, t.StringType(), True),
        t.StructField(c.columns_title_akas.region, t.StringType(), True),
        t.StructField(c.columns_title_akas.language, t.StringType(), True),
        t.StructField(c.columns_title_akas.is_original_title, t.BooleanType(), True),
    ]
)

