import pandas as pd
# import openai
import concurrent.futures
from ratelimit import limits, sleep_and_retry
from tqdm import tqdm

file_path = '/home/satvik/stackScrape/dataset/stackoverflow_merged'

df = pd.read_pickle(file_path + '.pkl')
column_to_check = 'post_answer'  # Replace 'column_name' with the actual column name
value_to_drop = ""  # Replace 'value_to_drop' with the specific value you want to drop

df_new = df[df['post_answer'] != value_to_drop]

avg_len = df_new['post_question'].str.split().str.len().mean()
print(f"Number of Posts: {len(df_new)}\nAverage Length of Posts: {avg_len}")

# Define the rate limit parameters
RATE_LIMIT = 30  # Number of calls allowed per second
RATE_LIMIT_PERIOD = 60  # Time period in seconds


# openai.api_key = open("key.txt", "r").read().strip("\n")

def STACKOVERFLOW_PROMPT(question):
    return f"""
Question: {question}
Answer:
"""


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=RATE_LIMIT_PERIOD)
def get_completion(question):
    subreddit = "AskCulinary"
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",  # this is "ChatGPT" $0.002 per 1k tokens
        messages=[{"role": "system", "content": f"You are now a helpful Stackoverflow user who helps other people by "
                                                f"answering their question succinctly and with a friendly and gentle "
                                                f"tone. You will use the question to answer the question "
                                                f"in a comprehensive manner. If you do not have enough information on "
                                                f"the question asked, you will return with some blog or documentation "
                                                f"references for the user."}, {"role": "user", "content": question}]
    )
    return completion["choices"][0]["message"]["content"]


def get_answers(df):
    # Define your chat prompts
    prompts = [STACKOVERFLOW_PROMPT(question) for question in df['post_question'].values]
    print(f"LEN = {len(prompts)}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = []
        with tqdm(total=len(prompts)) as pbar:
            for result in executor.map(get_completion, prompts):
                results.append(result)
                pbar.update(1)
    return results


df_new['gpt-3.5-turbo'] = get_answers(df_new)
print(df_new.head(2))
df_new.to_pickle(file_path + '_Answers.pkl', index=False)

