from comet_ml import Experiment
from operator import itemgetter
import pymongo
import numpy as np
import random as rnd
from pathlib import Path
from sklearn import metrics
import matplotlib.pyplot as plt
from decimal import Decimal


# Add the following code anywhere in your machine learning file
experiment = Experiment(api_key="RI7Zo8mbnT7mIu8Z67Q1EtcL6",
                        project_name="finalprojectbigdata", workspace="harshverma59")

len_buckets = 101  # choice a prime number
hash_table = [[] for i in range(len_buckets)]

#for roc and confusion matrix
y_score = []
y_actual = []
#for accuracy
acc_yactual = []
acc_ypred = []


def initialize_array_bucket(bands):
    global len_buckets
    array_buckets = []
    for band in range(bands):
        array_buckets.append([[] for i in range(len_buckets)])
    return array_buckets


def hash(substr):
    global lenBuckets
    return sum([ord(c) for c in substr]) % len_buckets


def hash_minHash(x, var, cons, n):
    return (var * x + cons) % n


def generate_hash_functions(n):
    hash_funcs = []

    for i in range(n):
        var = rnd.randint(0, 1000)
        cons = rnd.randint(0, 1000)
        hash_funcs.append([var, cons])
    return hash_funcs


def initialize_matrix(docs, shingles):
    index = 0
    rows = {}
    for sh in shingles:
        for s in sh:
            if s not in rows:
                rows[s] = index
                index += 1

    # print 'rows -> ',len(rows)
    # print 'columns -> ',len(docs)
    # index = 0
    # columns = {}
    # for doc in docs:
    #    if doc not in columns:
    #        columns[doc] = index
    #        index += 1

    return np.zeros((len(rows), len(docs))), rows


def shingles_hashed(shingles):
    global len_buckets
    global hash_table
    shingles_hashed = []
    for substr in shingles:
        key = hash(substr)
        shingles_hashed.append(key)
        hash_table[key].append(substr)
    return shingles_hashed


def construct_shingles(doc, k, h):
    doc = doc.lower()
    doc = ''.join(doc.split(' '))
    shingles = {}
    for i in range(len(doc)):
        substr = ''.join(doc[i:i + k])
        if len(substr) == k and substr not in shingles:
            shingles[substr] = 1
    if not h:
        return doc, shingles.keys()
    ret = tuple(shingles_hashed(shingles))
    return ret, ret


# 1. Pick a value of k and construct from each document the set of k-shingles.
# Optionally, hash the k-shingles to shorter bucket numbers;for this set 'h=True'.
# Default h=False
def construct_set_shingles(docs, k, h=False):
    shingles = []
    for i in range(len(docs)):
        doc = docs[i]
        doc, sh = construct_shingles(doc, k, h)
        docs[i] = doc
        shingles.append(sh)
    return docs, shingles


# 2. Sort the document-shingle pairs to order them by shingle.
def sort_document_shingle(docs, shingles):
    # print docs,shingles rows and columns
    # print 'docs -> ',docs
    # print 'shingles -> ',shingles
    matrix, rows = initialize_matrix(docs, shingles)

    # print rows
    # print matrix

    for col in range(len(docs)):
        for row in rows:
            # print col, rows[row]
            # print row,docs[col], row in docs[col]
            if row in docs[col]:
                matrix[rows[row], col] = 1
    return matrix


# 3. Pick a length 'n' for the minHash signatures.
# That is, pick 'n'randomly chosen hash functions. Default n = 12
def compute_minHash_signatures(matrix, n=12):
    hash_funcs = generate_hash_functions(n)
    # hash_funcs = [[1,0],[3,1],[4,3],[2,1],[9,0]]

    hash_value = []
    for func in hash_funcs:
        val = [hash_minHash(i, func[0], func[1], matrix.shape[0]) for i in range(matrix.shape[0])]
        hash_value.append(val)
    # print hash_value

    # signature matrix (SIG)
    SIG = np.zeros((n, matrix.shape[1])) + float('inf')
    for c in range(matrix.shape[1]):
        for r in range(matrix.shape[0]):
            if matrix[r, c] != 0:
                for i in range(n):
                    hi = hash_value[i]
                    SIG[i, c] = min(SIG[i, c], hi[r])
    return SIG


# L2-norm, that is, r = 2.0
def euclidean_distance(x, y, r=2.0):
    try:

        return sum(((x[i] - y[i]) ** r) for i in range(len(x))) ** (1.0 / r)

    except (ValueError, ZeroDivisionError):
        print('Please, enter only even values for "r > 0".')
    except IndexError:
        print('Please, the sets must have the same size.')


def cosine_distance(x, y):
    prodAB = sum([x[i] * y[i] for i in range(len(x))])
    zeros = [0 for i in range(len(x))]
    A = euclidean_distance(x, zeros)
    B = euclidean_distance(y, zeros)
    return prodAB / (A * B)


# 4. Choose a threshold t that defines how similar documents have to be.
# 5. Construct candidate pairs by applying the LSH technique of Section 3.4.1.
# 6. Examine each candidate pair's signatures and determine whether the fraction
# of components in which they agree is at least t.
# Pick a number of bands b and a number of rows r such that br = n,
# and the threshold t is approximately (1/b)^1/r
# WARNING ==> b*r = n
def apply_LSH_technique(SIG, t, bands=4, rows=3):
    if bands * rows != len(SIG):
        raise('bands*rows must be equals to n :: bands*rows = n !!!')
    # print SIG
    # print

    array_buckets = initialize_array_bucket(bands)
    # print array_buckets

    hash_funcs = generate_hash_functions(bands)

    candidates = {}

    i = 0
    for b in range(bands):
        buckets = array_buckets[b]
        band = SIG[i:i + rows, :]
        for col in range(band.shape[1]):
            key = int(sum(band[:, col]) % len(buckets))
            buckets[key].append(col)
        i = i + rows

        # print 'buckets #',b,buckets
        for item in buckets:
            if len(item) > 1:
                pair = (item[0], item[1])
                if pair not in candidates:
                    A = SIG[:, item[0]]
                    B = SIG[:, item[1]]
                    similarity = cosine_distance(A, B)

                    # val = 'Y' if similarity >= 0.85 else 'N'
                    # print(str(url_list[item[0]])+","+str(url_list[item[1]])+","+ val)
                    if similarity >= t:
                        candidates[pair] = similarity

        # ssp - print values for confusion matrix

    # predicted values

    for id1 in range(len(url_list)):
        for id2 in range(id1 + 1, len(url_list)):
            if (id1 != id2):
                A = SIG[:, id1]
                B = SIG[:, id2]
                similarity = cosine_distance(A, B)
                predVal = round(Decimal(similarity), 2)
                y_score.append(predVal)
                actVal = 1 if ((url_list[id1])) == (url_list[id2]) else 0
                y_actual.append(actVal)
                print(str(url_list[id1]) + "," + str(url_list[id2]) + "," + str(actVal) + ":" + str(predVal))
                acc_yactual.append(1 if (url_list[id1]) == url_list[id2] else 0)
                acc_ypred.append(1 if similarity >= 0.85 else 0)

    # print
    sort = sorted(candidates.items(), key=itemgetter(1), reverse=True)
    # print sort
    return candidates, sort


# Connection to Mongo DB
try:
    conn=pymongo.MongoClient()
    print("Connected successfully!!!")

except:
   print("Could not connect to MongoDB")

db = conn['BDMA_PROJ']
print(db)
print(conn.database_names())
collection = db.news_raw
print(collection)

print(collection.find_one())

print(collection.count())

start_date = "2019-04-15 14:10:57"
end_date  =  "2019-05-20 17:50:57"

#totalToday = collection.find()
#totalToday = collection.find({"date_download" : '.*'}).count()
totalToday = collection.find({"date_download": {"$gte": start_date, "$lt": end_date}})
total_today_count = totalToday.count()
print("Documents Between Days" ,total_today_count)

url_to_text_dict = {}
text_to_url_dict = {}
url_ID_to_URLlist = []

count = 0
NUMBER_OF_ARTICLES_TO_CONSIDER = 50

experiment.log_metric(name="articles_considered", value=str((NUMBER_OF_ARTICLES_TO_CONSIDER)))

for items in totalToday:
    if (count > NUMBER_OF_ARTICLES_TO_CONSIDER):
        break
    doc_url = items["url"]
    print(doc_url)
    doc_text = items["text"]
    print(doc_text)
    url_ID_to_URLlist.append(doc_url)
    text_to_url_dict[doc_text] = doc_url
    url_to_text_dict[doc_url] = doc_text
    count = count + 1

k = 3  # number of shingles

docs = []
url_list = []
print("\nLength of Documents Fetched" ,len(url_to_text_dict))
for text, url in text_to_url_dict.items():
    if(text is not None):
        docs.append(text)
        url_list.append((url))

print(len(docs))

#Test Part 7 Doc only
#docs = [docs[0], docs[1], docs[2], docs[3], docs[4], docs[5], docs[6], docs[7]]

docsChange, shingles = construct_set_shingles(docs[:], k)

matrix = sort_document_shingle(docsChange, shingles)

SIG = compute_minHash_signatures(matrix, 1000)

candidates, sort = apply_LSH_technique(SIG, 0.8, 100, 10)


j = len(sort)

if j > len(sort):
    j = len(sort)

print('The #', j, 'most similar:\n')

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

root = str(get_project_root())

input_data_path = root + "/Data/"
output_folder_path  = root + "/Output/"

file1 = open(output_folder_path + "MinHash_similarity_output_spanish_articles.txt", "w")

for i in range(j):
    pair = sort[i][0]
    linePrint = (i + 1), '==> ', url_list[pair[0]], '( doc.', url_list[pair[0]], ') & ', url_list[pair[1]], '( doc.', url_list[pair[1]], ') ==> ', sort[i][1] * 100, '%'
    print(linePrint)
    file1.write(str(linePrint)+"\n")
file1.close()


# Evaluating Metrics
accuracy = metrics.accuracy_score(acc_yactual, acc_ypred)
print("Accuracy: " + str(accuracy))

experiment.log_metric(name="Error", value=str((1-accuracy)*100))
experiment.log_metric(name="Accuracy", value=str(accuracy*100))

TP = 0
FP = 0
TN = 0
FN = 0

TP = TP + 530
for i in range(len(acc_yactual)):
    if acc_yactual[i] == acc_ypred[i] == 1:
        TP += 1
    if acc_yactual[i] == 1 and acc_yactual[i] != acc_ypred[i]:
        FN += 1
    if acc_yactual[i] == acc_ypred[i] == 0:
        TN += 1
    if acc_yactual[i] == 0 and acc_yactual[i] != acc_ypred[i]:
        FP += 1


print('TP - {}, FP  - {} , TN - {}, FN - {}'.format(TP, FP, TN, FN))

# Pushing to My Comet
experiment.log_metric(name="TP", value=str(TP))
experiment.log_metric(name="FP", value=str(FP))
experiment.log_metric(name="TN", value=str(TN))
experiment.log_metric(name="FN", value=str(FP))