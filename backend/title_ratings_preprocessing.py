import pyspark.sql.types as t
import re
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Processing").getOrCreate()

tconst = "tconst"
averageRating = "average_rating"
numVotes = "num_votes"



schema_title_ratings = t.StructType(
    [
        t.StructField("tconst", dataType=t.StringType()),
        t.StructField("averageRating", dataType=t.FloatType()),
        t.StructField("numVotes", dataType=t.IntegerType()),
    ]
)

def camel_to_snake(str):
    # Use regular expression to insert underscores before capital letters
    snake_case_str = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str)
    # Convert to lower case
    return snake_case_str.lower()


def str_to_arr_type(df, cols, splitter, f):
    '''
    Function that converts 'cols' of 'df' from t.StringType() to t.ArrayType(t.StringType())
    Example: 'val1,val2' -> ['val1', 'val2']

    Args:
        df: df to handle
        cols: list of column names to handle
        splitter: a string or symbol by which cols values will be splitted
        f: pyspark.sql.functions
    
    Returns:
        Modified dataframe with 'cols' of type t.ArrayType(t.StringType())
        
    '''
    for col_name in cols:
        df = df.withColumn(
            col_name,
            f.split(f.trim(f.col(col_name)), splitter),
        ) 

    for col_name in cols:
        print(df.select(col_name).schema.fields[0].dataType)

    return df

schema_title_ratings_final = t.StructType(
    [
        t.StructField(tconst, t.StringType(), True),
        t.StructField(averageRating, t.FloatType(), True),
        t.StructField(numVotes, t.IntegerType(), True),
    ]
)

def load_title_ratings_df(spark_session):
    title_ratings_df = spark_session.read.csv('gs://imdb-dataframes/title.ratings.tsv',
                                            sep=r"\t",
                                            header=True,
                                            nullValue="\\N",
                                            schema=schema_title_ratings)

    columns = title_ratings_df.columns 
    renamed_columns = [camel_to_snake(c) for c in columns]

    for i, column in enumerate(columns):
        title_ratings_df = title_ratings_df.withColumnRenamed(column, renamed_columns[i])
    

    title_ratings_df = title_ratings_df.na.drop(subset=title_ratings_df.columns)

    title_ratings_df.write.csv('gs://imdb-dataframes/title_ratings/', header=True, mode='overwrite', sep='\t')

if __name__ == '__main__':
    load_title_ratings_df(spark_session=spark)