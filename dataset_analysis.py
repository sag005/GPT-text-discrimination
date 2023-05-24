"""
File to check the length of the datapoints in our dataset.
"""
import transformers
import numpy as np

preproc_tokenizer = transformers.AutoTokenizer.from_pretrained('t5-small', model_max_length=512)

data = ["example string 1", "example string 2"] # this should be the response column in our dataset

# strip whitespace around each example and remove newlines
data = [x.strip() for x in data]
data = [strip_newlines(x) for x in data]

# try to keep only examples with > 250 words
long_data = [x for x in data if len(x.split()) > 250]
if len(long_data) > 0:
    data = long_data

# keep only examples with <= 512 tokens according to mask_tokenizer
# this step has the extra effect of removing examples with low-quality/garbage content
tokenized_data = preproc_tokenizer(data)
data = [x for x, y in zip(data, tokenized_data["input_ids"]) if len(y) <= 512]

# print stats about remainining data
print(f"Total number of samples: {len(data)}")
print(f"Average number of words: {np.mean([len(x.split()) for x in data])}")
