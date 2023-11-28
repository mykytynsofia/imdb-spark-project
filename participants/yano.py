# here Yurii Yano will implement his 6 business questions to the data

# docker run -p 80:3000 --rm -v '/Users/yurayano/PycharmProjects/pyspark:/app' --name container-spark-dev-mode spark-image:3.0 


import columns.columns_name_basics as columns_name_basics


def load_name_basics_df(path, spark_session):
    imdb_df = spark_session.read.csv(path, sep=r'\t', header=True) # , schema=df_schema
    imdb_df.show()
    imdb_df = spark_session.read.csv('./datasets/title.akas.tsv', sep=r'\t', header=True) # , schema=df_schema
    imdb_df.show()
    # print(columns_name_basics.kaka)
    # print(path)




