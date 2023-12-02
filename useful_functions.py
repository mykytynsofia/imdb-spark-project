import re
import os

def get_statistics(df, count=True, describe=True, summary=True):
    print("--------------------------------- name.basics.tsv ---------------------------------")
    if count:
        print("Number of entries:", df.count())
    if describe:
        print(' --- DESCRIBE --> ')
        stats_df = df.describe()
        stats_df.show()
    if summary:
        print(' --- SUMMARY --> ')
        stats_df2 = df.summary()
        stats_df2.show()
    print("--------------------------------- --------------- ---------------------------------")


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

    # Перевірка типу даних колонок з масиву ʼcolsʼ (також є у printSchema)
    for col_name in cols:
        print(df.select(col_name).schema.fields[0].dataType)

    return df


def create_folder(path, folder_name):
    '''
    Args:
        path: the path where you want to create the folder
        folder_name: the folder name you want to create

    Returns: None
    '''

    # Construct the full path for the new folder
    # new_folder_path = os.path.join(path, folder_name)

    # Check if the folder doesn't exist, then create it
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Folder '{folder_name}' created successfully in '{path}'")
    else:
        print(f"Folder '{folder_name}' already exists in '{path}'")
