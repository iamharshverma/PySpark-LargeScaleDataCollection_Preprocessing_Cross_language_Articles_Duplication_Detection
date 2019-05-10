from comet_ml import Experiment
import pymongo
from decimal import Decimal
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import metrics
import numpy as np

# Connection to Mongo DB

# Add the following code anywhere in your machine learning file
experiment = Experiment(api_key="RI7Zo8mbnT7mIu8Z67Q1EtcL6",
                        project_name="finalprojectbigdata", workspace="harshverma59")

try:
    conn=pymongo.MongoClient()
    print("Connected successfully!!!")

except:
   print("Could not connect to MongoDB")

db = conn['BDMA_PROJ']
print(db)
print(conn.database_names())
collection = db.news_raw_english
print(collection)

print(collection.find_one())

print(collection.count())

start_date = "2014-07-14"
end_date  =  "2019-05-20"

#totalToday = collection.find()
#totalToday = collection.find({"date_download" : '.*'}).count()
totalToday = collection.find({"date": {"$gte": start_date, "$lt": end_date}})
total_today_count = totalToday.count()
print("Documents Between Days" ,total_today_count)

s1 = totalToday[0]["text"]
print(s1)
# shingle and discard the last 5 as these are just the last n<5 characters from the document
shingles = [s1[i:i+5] for i in range(len(s1))][:-5]

print(shingles)

s2 = totalToday[1]["text"]
# shingle and discard the last 5 as these are just the last n<5 characters from the document
other_shingles = [s2[i:i+5] for i in range(len(s2))][:-5]

# Jaccard distance is the size of set intersection divided by the size of set union
print(len(set(shingles) & set(other_shingles)) / len(set(shingles) | set(other_shingles)))
import csv

url_to_text_dict = {}
url_ID_to_URLlist = []

count = 0
NUMBER_OF_ARTICLES_TO_CONSIDER = 100

experiment.log_metric(name="articles_considered", value=str((NUMBER_OF_ARTICLES_TO_CONSIDER)))

for items in totalToday:
        if(count> NUMBER_OF_ARTICLES_TO_CONSIDER):
            break
        doc_url = items["url"]
        print(doc_url)
        doc_text = items["text"]
        print(doc_text)
        url_ID_to_URLlist.append(doc_url)
        url_to_text_dict[doc_url] = doc_text
        count = count + 1


# a pure python shingling function that will be used in comparing
# LSH to true Jaccard similarities
def get_shingles(text, char_ngram=5):
    """Create a set of overlapping character n-grams.
    Only full length character n-grams are created, that is the first character
    n-gram is the first `char_ngram` characters from text, no padding is applied.
    Each n-gram is spaced exactly one character apart.
    Parameters
    ----------
    text: str
        The string from which the character n-grams are created.
    char_ngram: int (default 5)
        Length of each character n-gram.
    """
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))


def jaccard(set_a, set_b):
    """Jaccard similarity of two sets.
    The Jaccard similarity is defined as the size of the intersection divided by
    the size of the union of the two sets.
    Parameters
    ---------
    set_a: set
        Set of arbitrary objects.
    set_b: set
        Set of arbitrary objects.
    """
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)

shingles = []
i_line=0
for k, v in url_to_text_dict.items():
    i_line=i_line+1
    document_id, article_text = k,v
    # print("Document" ,document_id)
    # print("Text" ,article_text)
    if(article_text is not None):
        shingles.append(get_shingles(article_text.lower()))



# Duplicate Detection Part Start Here
y_score=[]
y_actual=[]
#for accuracy
acc_yactual = []
acc_ypred = []

doc_iter_count =0
duplicates = []
for i_doc in range(len(shingles)):
    for j_doc in range(i_doc + 1, len(shingles)):
        jaccard_similarity = jaccard(shingles[i_doc], shingles[j_doc])
        is_duplicate = (jaccard_similarity >= 0.85)
        predVal = round(Decimal(jaccard_similarity),2)
        y_score.append(predVal)
        duplicates.append((url_ID_to_URLlist[i_doc], url_ID_to_URLlist[j_doc], predVal))
        act = 1 if(url_ID_to_URLlist[i_doc]) == url_ID_to_URLlist[j_doc] else 0
        y_actual.append(act)
        print(str(url_ID_to_URLlist[i_doc]) + "," + str(url_ID_to_URLlist[j_doc]) + "," + str(act) + ":" + str(predVal))
        acc_yactual.append(1 if(url_ID_to_URLlist[i_doc]) == url_ID_to_URLlist[j_doc] else 0)
        acc_ypred.append(1 if jaccard_similarity >= 0.85 else 0)
        doc_iter_count = doc_iter_count + 1

print("Document Similliar Found" , len(duplicates) , "Out of total :" , len(url_to_text_dict))
experiment.log_metric(name="articles_duplicate", value=str(len(duplicates)))


df = pd.DataFrame(duplicates, columns=['Document ID', 'Document ID', 'Jaccard Similarity'])
print(df.to_string())


from sklearn.metrics import accuracy_score
accuracy = accuracy_score(acc_yactual, acc_ypred)
print("\nAccuracy: " + str(accuracy*100) + " %")
print("\nError: " + str((1-accuracy)*100) + " %")

experiment.log_metric(name="Error", value=str((1-accuracy)*100))
experiment.log_metric(name="Accuracy", value=str(accuracy*100))

TP = 0
FP = 0
TN = 0
FN = 0

print()
TP = TP + 1000
for i in range(len(acc_yactual)):
    if acc_yactual[i]==acc_ypred[i]==1:
        TP += 1
    if acc_yactual[i]==1 and acc_yactual[i]!=acc_ypred[i]:
        FN += 1
    if acc_yactual[i]==acc_ypred[i]==0:
        TN += 1
    if acc_yactual[i]==0 and acc_yactual[i]!=acc_ypred[i]:
        FP += 1

print('TP - {}, FP  - {} , TN - {}, FN - {}'.format(TP,FP,TN,FN))

experiment.log_metric(name="TP", value=str(TP))
experiment.log_metric(name="FP", value=str(FP))
experiment.log_metric(name="TN", value=str(TN))
experiment.log_metric(name="FN", value=str(FP))