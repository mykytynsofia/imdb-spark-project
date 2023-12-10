from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

from pyspark import SparkConf
from pyspark.sql import SparkSession

import pyspark.sql.types as t


from schemas import schema_title_basics, schema_title_ratings

import queries.title_ratings as title_ratings

app = FastAPI()

class Query(BaseModel):
    option: int

spark_session = (
    SparkSession.builder.master("local")
    .appName("IMDB Spark")
    .config(conf=SparkConf())
    .getOrCreate()
)

@app.post("/title_ratings")
async def predict(query: Query):
    option = query.option

    ratings_df = spark_session.read.csv(
        title_ratings_path, sep=r"\t", header=True, nullValue='\\N', schema=schema_title_ratings)

    title_basics_df = spark_session.read.csv(title_basics_path,
                                             sep=r"\t",
                                             header=True,
                                             nullValue="\\N",
                                             schema=schema_title_basics)

    if option == 0:
        return title_ratings.processTop100(ratings_df=ratings_df, title_basics_df=title_basics_df)
    elif option == 1:
        return title_ratings.processYearAverage(ratings_df=ratings_df, title_basics_df=title_basics_df)
    elif option == 2:
        return title_ratings.processDecadeAverage(ratings_df=ratings_df, title_basics_df=title_basics_df)
    elif option == 3:
        return title_ratings.processMostVoted(ratings_df=ratings_df, title_basics_df=title_basics_df)
    elif option == 4:
        return title_ratings.processGenresAverage(ratings_df=ratings_df, title_basics_df=title_basics_df)
    else:
        return title_ratings.processYearCount(ratings_df=ratings_df, title_basics_df=title_basics_df)



if __name__ == '__main__':
    uvicorn.run(app, port=8080, host='0.0.0.0')