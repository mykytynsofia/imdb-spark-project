# here Yurii Yano will implement his 6 business questions to the data

# docker run -p 80:3000 --rm -v '/Users/yurayano/PycharmProjects/pyspark:/app' --name container-spark-dev-mode spark-image:3.0


import columns.columns_name_basics as columns_name_basics
import re
import datasets_paths as paths
import os


# from main import t                # CIRCULAR IMPORT ERROR


def camel_to_snake(name):
    # Use regular expression to insert underscores before capital letters
    snake_case_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Convert to lower case
    return snake_case_name.lower()


def get_statistics(df):
    print(
        "--------------------------------- name.basics.tsv ---------------------------------"
    )
    print("Number of entries:", df.count())
    stats_df = df.describe()
    stats_df.show()
    stats_df2 = df.summary()
    stats_df2.show()
    print(
        "--------------------------------- --------------- ---------------------------------"
    )


# Function to Convert 'primary_profession' and 'known_for_titles' from t.StringType() to t.ArrayType() 
# (бо по дефолту в оріг. датасеті 'primary_profession' and 'known_for_titles' мають  значення в строці через кому)
''' 'val1,val2' -> ['val1', 'val2'] тобто t.StringType() -> t.ArrayType(t.StringType()) '''
def str_to_arr(name_basics_df, f):
    for col_name in [columns_name_basics.primary_profession, columns_name_basics.known_for_titles]:
        name_basics_df = name_basics_df.withColumn(
            col_name,
            f.split(f.trim(name_basics_df[col_name]), ","),
        )
    return name_basics_df
    # # Перевірка типу даних колонки "split_values" та "known_for_titles" (також у printSchema)
    # print(name_basics_df.select(columns_name_basics.primary_profession).schema.fields[0].dataType,
    #       name_basics_df.select(columns_name_basics.known_for_titles).schema.fields[0].dataType)
    # # подивимося, як виглядає наше значення ʼnot statedʼ в колонках primary_profession та known_for_titles 
    # # (взяв ті рядки, де точно є значення ʼnot statedʼ)
    # print(name_basics_df.filter(name_basics_df["nconst"] == 'nm0002652').select("primary_profession").collect())
    # print(name_basics_df.filter(name_basics_df["nconst"] == 'nm0003936').select("known_for_titles").collect())


def load_name_basics_df(path, spark_session, t, f):
    if os.listdir(paths.PATH_NAME_BASICS_MOD):
        print(f"You already saved name_basics df !")

        schema_name_basics = t.StructType(
            [
                t.StructField(columns_name_basics.nconst, t.StringType(), True),
                t.StructField(columns_name_basics.primary_name, t.StringType(), True),
                # t.StructField(columns_name_basics.primary_profession, t.ArrayType(t.StringType()), True), # doesn't work
                # t.StructField(columns_name_basics.known_for_titles, t.ArrayType(t.StringType()), True),   # doesn't work
                t.StructField(columns_name_basics.primary_profession, t.StringType(), True),
                t.StructField(columns_name_basics.known_for_titles, t.StringType(), True),
            ]
        )
        
        df_ready = spark_session.read.csv(paths.PATH_NAME_BASICS_MOD,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N", 
                                    schema=schema_name_basics)
        return str_to_arr(df_ready, f)

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
    name_basics_df = spark_session.read.csv(path,
                                            sep=r"\t", 
                                            header=True, 
                                            nullValue="\\N", 
                                            schema=schema_name_basics)
                                            

    cols = name_basics_df.columns  # in order of columns in df
    new_cols_names = [camel_to_snake(c) for c in cols]

    # Rename columns to 'snake' case    -  using 'withColumnRenamed(old, new)'
    for i, old_col in enumerate(cols):
        name_basics_df = name_basics_df.withColumnRenamed(old_col, new_cols_names[i])

    # get_statistics(name_basics_df)

    """
        Бачимо, що у нашому df всього 13045749 записів, з яких not null:
            - primary_name = 13045749       [  100%  ]
            - birth_year = 598439           [  4.59% ]
            - death_year = 222539           [  1.7%  ]         
            - primary_profession = 10446409 [  80.07 %  ]
            - known_for_titles = 11590804   [  88.85 %  ]

        Отож, прийнято рішення видалити колонки (birth_year, death_year) через малу к-сть not null значень і не здатність заповнити пропущені значення
        
        Щодо колонок які мають невелику к-сть null значень (primary_profession, known_for_titles) вирішено замінити їх на 
        значення ('not_stated', 'not_stated') відповідно.  
    """

    arrayed_cols_names = [columns_name_basics.primary_profession,
                            columns_name_basics.known_for_titles]

    for col_name in arrayed_cols_names:   
        name_basics_df = name_basics_df.withColumn(
            col_name,
            f.when(f.col(col_name).isNotNull(), f.col(col_name))
            .otherwise(f.lit("not stated"))
        )
    # оскільки ми викликаємо f.col в межах виклику ф-ї withColumn до name_basics_df, то f.col доступатиметься саме до цього датасету (не буде колізій). також можна через [] і .

    # for col_name in arrayed_cols_names:   
    #     # Витягнення рядків, де значення в колонці col_name дорівнює "not stated"
    #     not_stated = name_basics_df.filter(name_basics_df[col_name] == "not stated")
    #     # Виведення результату
    #     print(f"not stated [{col_name}] = ", not_stated.count())
    #     not_stated.show()
    '''
    RESULT:
        not stated [primary_profession] =  2599340 (13045749 - 10446409 ✅)                                
        not stated [known_for_titles] =  1454945   (13045749 - 11590804 ✅)              
    '''
    # get_statistics(name_basics_df) # screen 1


    name_basics_df.show(30, truncate=False)


    ''' Видалимо колонки birth_year, death_year '''
    name_basics_df = name_basics_df.drop('birth_year', 'death_year')

    name_basics_df.show(truncate=False)
    name_basics_df.printSchema()

    # get_statistics(name_basics_df) # no sense, cuz no numeric columns

    # save file
    print(f'Saving to {paths.PATH_NAME_BASICS_MOD} ...')
    name_basics_df.write.csv(paths.PATH_NAME_BASICS_MOD, header=True, mode='overwrite', sep='\t')
    print(f"name_basis's been modified and saved successfully!")

    return str_to_arr(name_basics_df, f)