# here Iryna Koval will implement her 6 business questions to the data

# docker run -p 80:3000 --rm -v '/Users/yurayano/PycharmProjects/pyspark:/app' --name container-spark-dev-mode spark-image:3.0

from pyspark.sql import SparkSession  # сесія на основі модуля sql
from pyspark import SparkConf
import columns.columns_title_principals as columns_title_principals
import re
import datasets_paths as paths
import os
from useful_functions import (get_statistics,
                            camel_to_snake,
                            str_to_arr_type,
                            create_folder)
from schemas import schema_title_principals, schema_title_principals_final

def load_title_principals_df(path, spark_session, f):
    arrayed_cols_names = [columns_title_principals.characters]

    if os.path.exists(paths.PATH_TITLE_PRINCIPALS_MOD):
        print(f"You already saved title_principals df !")
        
        df_ready = spark_session.read.csv(paths.PATH_TITLE_PRINCIPALS_MOD,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N", 
                                    schema=schema_title_principals_final)
        return str_to_arr_type(df_ready, arrayed_cols_names, ',', f)

    title_principals_df = spark_session.read.csv(path,
                                            sep=r"\t", 
                                            header=True, 
                                            nullValue="\\N", 
                                            schema=schema_title_principals)
                                            

    """
        Бачимо, що у нашому df всього 59367768 записів, з яких not null:
            - tconst = 59367768           [  100%  ]
            - ordering = 59367768         [  100% ]
            - nconst = 59367768           [  100%  ]         
            - job = 9746971               [  16.42 %  ]
            - characters = 28572846       [  48.13 %  ]

        Отож, прийнято рішення видалити колонки (birth_year, death_year) через малу к-сть not null значень і не здатність заповнити пропущені значення
        
        Щодо колонок які мають невелику к-сть null значень (primary_profession, known_for_titles) вирішено замінити їх на 
        значення ('not_stated', 'not_stated') відповідно.  
    """
    
    ''' Видалимо колонки job '''
    title_principals_df = title_principals_df.drop('job')

    title_principals_df = title_principals_df.fillna('not stated', subset=arrayed_cols_names)

    for col_name in arrayed_cols_names:   
        # Витягнення рядків, де значення в колонці col_name дорівнює "not stated"
        not_stated = title_principals_df.filter(title_principals_df[col_name] == "not stated")
        # Виведення результату
        print(f"not stated [{col_name}] = ", not_stated.count())
    '''
    RESULT:
        not stated [primary_profession] =  2599340 (13045749 - 10446409 ✅)                                
        not stated [known_for_titles] =  1454945   (13045749 - 11590804 ✅)              
    '''


    print(f'Showing first 30 rows')
    title_principals_df.show(130, truncate=False)

    # ''' Видалимо колонки birth_year, death_year '''
    # title_principals_df = title_principals_df.drop(columns_title_principals.job)

    # name_basics_df.show(truncate=False)
    title_principals_df.printSchema()

    # # get_statistics(name_basics_df) # no sense, cuz no numeric columns

    # Save to csv file
    create_folder(paths.PATH_TITLE_PRINCIPALS_MOD, 'title_principals_mod')
    print(f'Saving to {paths.PATH_TITLE_PRINCIPALS_MOD} ...')
    title_principals_df.write.csv(paths.PATH_TITLE_PRINCIPALS_MOD, header=True, mode='overwrite', sep='\t')
    print(f"title_principals's been modified and saved successfully!")

    title_principals_df_with_array_type = str_to_arr_type(title_principals_df, arrayed_cols_names, ',', f)
    
    return title_principals_df_with_array_type


