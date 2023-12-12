import pandas as pd
import matplotlib.pyplot as plt

# SCRIPT FOR VISUALIZATION OF YURA YANO'S RESULT OF QUERY #4

# Please, if you want to run this script:
# 1) create folder queries_results in a root of a project
# 2) run [  yano_queries.query_four(spark_session, title_crew_df, title_ratings_df, title_basics_df, name_basics_df, Window, f, t).show(truncate=False)  ]  command in main.py
# 3) Having your 'yano_query_4' folder created, insert your full path to created csv file in this folder below:
file_path = '/Users/yurayano/Documents/LPNU4/Big Data/git_big/imdb-spark-project/queries_results/yano_query_4/part-00000-ffae02ef-67ce-4124-98c3-b8801e1f2a6e-c000.csv'
# 4) run script LOCALY NOT using docker run. 
# 5) Have fun to try other directors. Please, be sure that he is present in dfs. Also delete 'yano_query_4' before repeting step 2.


# Read the TSV into a pandas DataFrame
df = pd.read_csv(file_path, sep='\t')

# Sort the DataFrame by 'start_year' for a smoother curve
df = df.sort_values(by='start_year')

# Plot the first curve: avg_rating
plt.figure(figsize=(10, 6))
plt.plot(df['start_year'], df['avg_rating'], linestyle='-', color='b')
plt.title('Curve Plot of start_year vs avg_rating')
plt.xlabel('start_year')
plt.ylabel('avg_rating')
plt.grid(True)
plt.show()

# Plot the second curve: avg_num_votes
plt.figure(figsize=(10, 6))
plt.plot(df['start_year'], df['avg_num_votes'], linestyle='-', color='r')
plt.title('Curve Plot of start_year vs avg_num_votes')
plt.xlabel('start_year')
plt.ylabel('avg_num_votes')
plt.grid(True)
plt.show()



# import pandas as pd
# import matplotlib.pyplot as plt

# # Assuming you have a CSV file named 'your_file.csv'
# file_path = '/Users/yurayano/Documents/LPNU4/Big Data/git_big/imdb-spark-project/queries_results/yano_query_4/part-00000-ffae02ef-67ce-4124-98c3-b8801e1f2a6e-c000.csv'

# # Read the TSV into a pandas DataFrame
# df = pd.read_csv(file_path, sep='\t')

# # Sort the DataFrame by 'start_year' for a smoother curve
# df = df.sort_values(by='start_year')

# # Calculate the weighted sum of avg_rating and avg_num_votes
# df['weighted_sum'] = df['avg_rating'] + df['avg_num_votes']/1000_000

# # Plot the weighted sum curve
# plt.figure(figsize=(10, 6))
# plt.plot(df['start_year'], df['weighted_sum'], linestyle='-', color='purple', label='Weighted Sum')
# plt.title('Curve Plot of start_year vs Weighted Sum')
# plt.xlabel('start_year')
# plt.ylabel('Weighted Sum')
# plt.legend()
# plt.grid(True)
# plt.show()
