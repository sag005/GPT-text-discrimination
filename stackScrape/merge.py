import pandas as pd
import glob


def read_pickle_file(file_path):
    return pd.read_pickle(file_path)


pickle_files = glob.glob('/home/satvik/stackScrape/dataset/*.pkl')
dataframes = []

for file in pickle_files:
    df = read_pickle_file(file)
    dataframes.append(df)

merged_df = pd.concat(dataframes, ignore_index=True)
merged_df.to_pickle('/home/satvik/stackScrape/dataset/stackoverflow_merged.pickle')
