# ALL IMPORTS
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import nltk

import numpy as np
# # Keras
# from keras.preprocessing.text import Tokenizer
# from keras.preprocessing.sequence import pad_sequences

# NLTK
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import pickle
# Other
import re
import string
import numpy as np

# Import data and preprocess
import pandas as pd
import os
from os import listdir
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import seaborn as sns

import random
import tensorflow as tf
import inflect as inflect
#import pandas_profiling
from sklearn.utils import resample
from sklearn.model_selection import KFold
from heapq import heappush, heappushpop
from sklearn.metrics import *
import sentencepiece as spm
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.cluster import SpectralClustering
from sklearn.cluster import BisectingKMeans
from sklearn.cluster import DBSCAN
from sklearn.cluster import Birch

nltk.download('stopwords')
pd.set_option('display.max_columns', None)
tf.config.run_functions_eagerly(True)

os.system('wget -P ../sentencepiece/ https://nlp.h-its.org/bpemb/multi/multi.wiki.bpe.vs1000000.d300.w2v.bin.tar.gz')
os.system('wget -P ../sentencepiece/ https://nlp.h-its.org/bpemb/multi/multi.wiki.bpe.vs1000000.model')
os.system('wget -P ../sentencepiece/ https://nlp.h-its.org/bpemb/multi/multi.wiki.bpe.vs1000000.vocab')
os.system('tar -xzvf ../sentencepiece/multi.wiki.bpe.vs1000000.d300.w2v.bin.tar.gz -C ../sentencepiece/')


import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--directory', help='link_to_file')
args = parser.parse_args()


# CLEAN TEXT METHOD
def replace_numbers(text):
    """Replace all interger occurrences in list of tokenized words with textual representation"""
    p = inflect.engine()
    words = text.split()
    new_words = []
    for word in words:
        if word.isdigit():
            new_word = p.number_to_words(word)
            new_words.append(new_word)
        else:
            new_words.append(word)
    return ' '.join(new_words)

def clean_text(text):
    # Convert I and İ into lowercase
    lower_map = {
    ord(u'I'): u'ı',
    ord(u'İ'): u'i',
    }

    text = text.translate(lower_map)

    ## Remove puncuation
    text = text.translate(string.punctuation)

    ## Convert words to lower case and split them
    text = text.lower().split()

    ## Remove stop words
    stops = set(stopwords.words("english")).union(stopwords.words("turkish"))
    text = [w for w in text if not w in stops and len(w) >= 3]
    text = " ".join(text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\n', ' ', text)
    return text
def postProcessDataset(input_data):
    combined_text_features = np.asarray(['{} {} {} {}'.format(bio,name,location,url) for bio,name,location,url in zip(input_data['bio'], input_data['name'], input_data['location'], input_data['url'])])
    text_data_vec = tf.keras.preprocessing.sequence.pad_sequences([sp.EncodeAsIds(x) for x in combined_text_features], maxlen=max_len)
    number_data_vec = np.asarray([[x, y, z, t, w] for x, y, z, t, w in zip(input_data['tweets'], input_data['following'], input_data['followers'], input_data['likes'], input_data['media'])])
    return text_data_vec, number_data_vec

# IMPORT DATA INTO PANDAS DATAFRAME
dataset_path = args.directory

data = pd.read_csv(dataset_path)
print('Data shape: ', data.shape)
data.head()
# Mapping to label
data['label'] = data['label'].replace({'individual': 0, 'organization': 1})
print('Data shape: ', data.shape)

# Replace null values with empty string
data = data.replace(np.nan, '', regex=True)
print('Data shape: ', data.shape)

# Make sure to truncate rows with corrupted labels.
data = data[(data['label'] == 0) | (data['label'] == 1)]
print('Data shape: ', data.shape)
print(data['label'].value_counts())

data = data.drop_duplicates(subset=['username'], keep='first')
print('------After Duplicate Elimination------')
print(':::Data Values:::')
print('Data shape: ', data.shape)
print(data['label'].value_counts())
orig_data = data.copy()
print(orig_data.shape)

# Clean Text Features
data['bio'] = data['bio'].map(lambda x: replace_numbers(clean_text(x)))
data['name'] = data['name'].map(lambda x: replace_numbers(clean_text(x)))
# data['username'] = data['username'].map(lambda x: clean_text(x))
data['location'] = data['location'].map(lambda x: replace_numbers(clean_text(x)))
data['url'] = data['url'].map(lambda x: replace_numbers(clean_text(x.split('com')[0])))

# Interpolate(Scale) Number Features
scaler = MinMaxScaler()
data[['tweets']] = scaler.fit_transform(data[['tweets']])
data[['following']] = scaler.fit_transform(data[['following']])
data[['followers']] = scaler.fit_transform(data[['followers']])
data[['likes']] = scaler.fit_transform(data[['likes']])
data[['media']] = scaler.fit_transform(data[['media']])

data['label'] = data['label'].astype('int32')

sp = spm.SentencePieceProcessor()
sp.Load("../sentencepiece/multi.wiki.bpe.vs1000000.model")
print('Indexing word vectors.')
EMBEDDING_DIM = 300
max_len = 64
test_data = pd.read_csv(args.directory)
df = pd.DataFrame(test_data)

# Assuming sp is already defined, and max_len is defined somewhere in your code
# If not, you should define them appropriately before running this code

# Initialize lists to store the processed data
combined_data_list = []
#labels_list = []

# Iterate over the first 5 rows in the DataFrame
for index, row in df.iterrows():
    # Call the postProcessDataset function for each row
    text_data_vec, number_data_vec = postProcessDataset(pd.DataFrame(row).transpose())

    # Concatenate text_data_vec and number_data_vec into one list
    combined_data = text_data_vec.flatten().tolist() + number_data_vec.flatten().tolist()

    # Append the combined data to lists
    combined_data_list.append(combined_data)
    #labels_list.append(row['label'])  # Use the label from the original frame

# Determine the maximum length of combined_data across all rows
max_combined_length = max(len(combined_data) for combined_data in combined_data_list)
print(max_combined_length)
# Create a DataFrame with redistributed columns
test_df = pd.DataFrame(
    [combined_data + [np.nan] * (max_combined_length - len(combined_data)) for combined_data in combined_data_list],
    columns=[f'feature_{i+1}' for i in range(max_combined_length)]
)

# Fill NaN values with 0 (replace NaN with 0)
test_df = test_df.fillna(0)

# Display the processed DataFrame# Display the processed DataFrameimport pickle
loaded_model = pickle.load(open('/content/logistic-reg.pickle', 'rb'))
result = loaded_model.predict(test_df)
print(f"The classification result is 0 for individual, 1 for organization: {result}")
f = pd.DataFrame(test_df)
from sklearn.preprocessing import MinMaxScaler

scaler = MinMaxScaler()

# Normalize all columns into the range [0, 1]
df_normalized = pd.DataFrame(scaler.fit_transform(test_df), columns=df.columns)
# Perform PCA
pca = PCA()
pca_result = pca.fit_transform(df_normalized)

# Create a DataFrame with the principal components
pc_df = pd.DataFrame(data=pca_result, columns=[f'PC{i}' for i in range(1, df.shape[1] + 1)])

# Visualize the explained variance ratio
explained_variance_ratio = pca.explained_variance_ratio_
cumulative_variance_ratio = explained_variance_ratio.cumsum()

# Visualize the 2D or 3D projection of the data
if df.shape[1] >= 2:
    # 2D Plot
    plt.figure(figsize=(10, 6))
    plt.scatter(pc_df['PC1'], pc_df['PC2'])
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.title('2D PCA Projection')
    plt.show()

if df.shape[1] >= 3:
    # 3D Plot
    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(pc_df['PC1'], pc_df['PC2'], pc_df['PC3'])
    ax.set_xlabel('Principal Component 1')
    ax.set_ylabel('Principal Component 2')
    ax.set_zlabel('Principal Component 3')
    ax.set_title('3D PCA Projection')
    plt.show()
num_clusters = 4

# Perform KMeans clustering on the original DataFrame
df = pd.DataFrame(df_normalized)
#spectral = SpectralClustering(n_clusters=num_clusters, random_state=42)
#df['Cluster'] = spectral.fit_predict(df)
#kmeans = KMeans(n_clusters=num_clusters, max_iter = 10, algorithm = "elkan", random_state=42)
#df['Cluster'] = kmeans.fit_predict(df)
eps = 2  # Radius of the neighborhood
min_samples = 4  # Minimum number of samples required in a neighborhood to form a core point

# Perform DBSCAN
#dbscan = DBSCAN(eps=eps, min_samples=min_samples)
#df['Cluster'] = dbscan.fit_predict(df)
#bisecting = BisectingKMeans(n_clusters=num_clusters, random_state=42)
#df['Cluster'] = bisecting.fit_predict(df)
birch = Birch(n_clusters=num_clusters, threshold=0.5, branching_factor=50)
df['Cluster'] = birch.fit_predict(df)
# Perform PCA
pca = PCA(n_components=2)
pca_result = pca.fit_transform(df.drop('Cluster', axis=1))
pca_result
# Create a DataFrame with the principal components
pc_df = pd.DataFrame(data=pca_result, columns=['PC1', 'PC2'])
samples_per_cluster = 5
sample_indices = []

for cluster_id in df['Cluster'].unique():
    cluster_indices = df[df['Cluster'] == cluster_id].sample(min(samples_per_cluster, len(df))).index.tolist()
    sample_indices.append(cluster_indices)
print(sample_indices)
# Visualize the 2D projection with clusters
plt.figure(figsize=(10, 6))
sns.scatterplot(x='PC1', y='PC2', hue=df['Cluster'], palette='viridis', data=pc_df, legend='full')
plt.title('2D PCA Projection with Clusters')
plt.show()