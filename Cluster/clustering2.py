from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
from sklearn import preprocessing
import pandas as pd
from datetime import datetime


def get_posts_from_cluster_num(cluster_num, model, vectorizer, posts_key_value):
    result = []
    for key in posts_key_value:
        post = posts_key_value[key]
        Y = vectorizer.transform([post])
        prediction = model.predict(Y)
        
        if prediction[0] + 1 == cluster_num:
            result.append(key)
        
    return result

d = pd.read_json(r"C:\Users\oljohnson\Desktop\clustering\BLOGPOSTS_sliced.json")
d = d[d['blogpost_id'] < 12]

# STORE BLOGPOST_ID AS KEY AND POST AS VALUE
key_val = {}
for i in range(len(d['post'].values)):
    key_val[d['blogpost_id'].values[i]] = d['post'].values[i]


def buildModel(key_val):
    start = datetime.now()
    print(start)
    documents = list(key_val.values())

    vectorizer = TfidfVectorizer(stop_words='english')

    X = vectorizer.fit_transform(documents)
    # X_Norm = prep
    print('vec')
    print(vectorizer)
    
    print('x')
    print(X)
    true_k =10
    print('getting model ready')
    # model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1, n_jobs=-1)
    model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
    print('done')
    model.fit(X)
    # model.fit_predict(X)
    print("Top terms per cluster:")
    order_centroids = model.cluster_centers_.argsort()[:, ::-1]

    print("order_centroids")
    # print(order_centroids)
    # print(order_centroids)

    terms = vectorizer.get_feature_names()
    print("terms")
    # print(len(terms))

    f= {}
    for i in range(true_k):
        print("Cluster %d:" % i)
        topterms = []
        for ind in order_centroids[i, :10]:
            topterms.append(terms[ind])
            
        post_ids = get_posts_from_cluster_num(i+1, model, vectorizer ,key_val)    

        dic={}
        dic[f"cluster_{i+1}"] = {"post_ids":post_ids,"topterms":topterms}
        arr = []
        arr.append(dic)
        f[i] = arr
    stop = datetime.now()
    print(stop)
    print(stop - start)
    return f

buildModel(key_val)

# I need to understand how to plot the scattered plot for the clusters.. I dont know what values I am passing to plot ..
# how do i get the 2d array ? from the model
# Nihal said we cannot use TSNE 
# He said something about kmeans has already built the model for you.. That TSNE builds another model or something like that
# Please can we do it together ??

#     FINAL OUTPUT (f)... P.S -> I still have to generate the matrix to plot the cluster
# {
#     "0": [
#         {
#             "cluster_1": {
#                 "post_ids": [
#                     "9"
#                 ],
#                 "topterms": [
#                     "china",
#                     "chinese",
#                     "chinaâ",
#                     "crimea",
#                     "tatars",
#                     "xinjiang",
#                     "ethnic",
#                     "tibet",
#                     "independence",
#                     "south"
#                 ]
#             }
#         }
#     ],
#     "1": [
#         {
#             "cluster_2": {
#                 "post_ids": [
#                     "13"
#                 ],
#                 "topterms": [
#                     "nuclear",
#                     "iran",
#                     "iraq",
#                     "iranian",
#                     "leaders",
#                     "hardliners",
#                     "clear",
#                     "technology",
#                     "rouhani",
#                     "iranâ"
#                 ]
#             }
#         }
#     ],
#     "2": [
#         {
#             "cluster_3": {
#                 "post_ids": [
#                     "1",
#                     "4"
#                 ],
#                 "topterms": [
#                     "afghanistan",
#                     "taliban",
#                     "american",
#                     "leave",
#                     "troops",
#                     "withdrawal",
#                     "country",
#                     "karzai",
#                     "hamid",
#                     "2014"
#                 ]
#             }
#         }
#     ],
#     "3": [
#         {
#             "cluster_4": {
#                 "post_ids": [
#                     "10"
#                 ],
#                 "topterms": [
#                     "ukraine",
#                     "russian",
#                     "russia",
#                     "yugoslavia",
#                     "neutralism",
#                     "west",
#                     "economy",
#                     "ukraineâ",
#                     "government",
#                     "georgian"
#                 ]
#             }
#         }
#     ],
#     "4": [
#         {
#             "cluster_5": {
#                 "post_ids": [
#                     "7"
#                 ],
#                 "topterms": [
#                     "snowden",
#                     "nsa",
#                     "newspapers",
#                     "edward",
#                     "government",
#                     "guardian",
#                     "authorities",
#                     "hacking",
#                     "gchq",
#                     "data"
#                 ]
#             }
#         }
#     ],
#     "5": [
#         {
#             "cluster_6": {
#                 "post_ids": [
#                     "5"
#                 ],
#                 "topterms": [
#                     "kennedyâ",
#                     "operations",
#                     "kennedy",
#                     "vietnam",
#                     "option",
#                     "covert",
#                     "americaâ",
#                     "history",
#                     "opposed",
#                     "successors"
#                 ]
#             }
#         }
#     ],
#     "6": [
#         {
#             "cluster_7": {
#                 "post_ids": [
#                     "3",
#                     "8"
#                 ],
#                 "topterms": [
#                     "attacks",
#                     "volgograd",
#                     "bombings",
#                     "russian",
#                     "dead",
#                     "chechen",
#                     "terror",
#                     "united",
#                     "distinct",
#                     "regimes"
#                 ]
#             }
#         }
#     ],
#     "7": [
#         {
#             "cluster_8": {
#                 "post_ids": [
#                     "12"
#                 ],
#                 "topterms": [
#                     "nobel",
#                     "putin",
#                     "international",
#                     "award",
#                     "ukraine",
#                     "putinâ",
#                     "russian",
#                     "nato",
#                     "politics",
#                     "stephen"
#                 ]
#             }
#         }
#     ],
#     "8": [
#         {
#             "cluster_9": {
#                 "post_ids": [
#                     "11"
#                 ],
#                 "topterms": [
#                     "aid",
#                     "money",
#                     "rebels",
#                     "funds",
#                     "group",
#                     "report",
#                     "syria",
#                     "afghanistan",
#                     "uk",
#                     "humanitarian"
#                 ]
#             }
#         }
#     ],
#     "9": [
#         {
#             "cluster_10": {
#                 "post_ids": [],
#                 "topterms": [
#                     "iran",
#                     "ii",
#                     "geneva",
#                     "invitation",
#                     "assad",
#                     "return",
#                     "syrian",
#                     "west",
#                     "doesnâ",
#                     "economic"
#                 ]
#             }
#         }
#     ]
# }