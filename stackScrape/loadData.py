import pandas as pd

df = pd.read_pickle('/home/satvik/stackScrape/dataset/stackoverflow_merged.pickle')
column_to_check = 'post_answer'  # Replace 'column_name' with the actual column name
value_to_drop = ""  # Replace 'value_to_drop' with the specific value you want to drop

df_new = df[df['post_answer'] != value_to_drop]
df_new.to_pickle('/home/satvik/stackScrape/dataset/stackoverflow.pickle')
print(df_new.head())
