import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
from sklearn import preprocessing
import pandas as pd
from datetime import datetime

d = pd.read_json(
    r"C:\Users\oljohnson\Desktop\clustering\BLOGPOSTS_sliced.json")
d = d[d['blogpost_id'] < 25]
print(d.shape)

# STORE BLOGPOST_ID AS KEY AND POST AS VALUE
key_val = {}
for i in range(len(d['post'].values)):
    key_val[d['blogpost_id'].values[i]] = d['post'].values[i]


true_k = 10
documents = list(key_val.values())

vectorizer = TfidfVectorizer(stop_words='english')

X = vectorizer.fit_transform(documents)
km = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)

alldistances = km.fit_predict(X)
print(alldistances)

km.fit(X)
# model.fit_predict(X)
print("Top terms per cluster:")
order_centroids = km.cluster_centers_.argsort()[:, ::-1]

df = pd.DataFrame(order_centroids)
print(df.shape)

print(type(order_centroids))
print(len(order_centroids))
print(order_centroids)

# def k_mean_distance(data, cx, cy, i_centroid, cluster_labels):
#         distances = [np.sqrt((x-cx)**2+(y-cy)**2)
#                      for (x, y) in data[cluster_labels == i_centroid]]
#         return distances


# clusters = km.fit_predict(X)
# centroids = km.cluster_centers_

# print('clusters')
# print(clusters)
# print()

# print('centroids')
# print(centroids)

# distances = []
# # for i, (cx, cy) in enumerate(centroids):
# #     mean_distance = k_mean_distance(X, cx, cy, i, clusters)
# #     distances.append(mean_distance)

# # print(distances)

# for i in enumerate(centroids):
#     print(i)
