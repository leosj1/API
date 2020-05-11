import pandas as pd
from sklearn.decomposition import PCA

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
from sklearn import preprocessing
# import pandas as pd
from datetime import datetime
# import matplotlib.pyplot as plt
import numpy as np
from sklearn.externals import joblib 

data_blogs = pd.read_json(r"C:\Users\oljohnson\Desktop\clustering\BLOGPOSTS.json")

# data_blogs = pd.read_json(r"C:\Users\oljohnson\Desktop\clustering\BLOGPOSTS.json")
# d = data_blogs[data_blogs['blogpost_id'] < 1000]
d = data_blogs
print(d.shape)

# STORE BLOGPOST_ID AS KEY AND POST AS VALUE
key_val = {}
for i in range(len(d['post'].values)):
    key_val[d['blogpost_id'].values[i]] = d['post'].values[i]

print('done getting data')
start = datetime.now()
documents = list(key_val.values())

vectorizer = TfidfVectorizer(stop_words='english')

vectorizer.fit(documents)

X = vectorizer.transform(documents)

data = PCA(n_components = 2).fit_transform(X.toarray())
  
# Save the model as a pickle in a file 
joblib.dump(data, 'filename.pkl') 
  
# Load the model from the file 
knn_from_joblib = joblib.load('filename.pkl')  
  
# Use the loaded model to make predictions 
# knn_from_joblib.predict(X_test) 
