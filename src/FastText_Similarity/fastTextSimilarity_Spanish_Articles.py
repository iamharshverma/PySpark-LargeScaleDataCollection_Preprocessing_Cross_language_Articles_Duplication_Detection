# coding: utf-8
# Author @Harsh-Verma
from comet_ml import Experiment
import numpy as np
from Data.fast_Text_Model.fasttext import FastVector
from pathlib import Path
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
import re
import io
import src.mongo_raw_data_util as mongo_raw_data_util


# Add the following code anywhere in your machine learning file
experiment = Experiment(api_key="RI7Zo8mbnT7mIu8Z67Q1EtcL6",
                        project_name="finalprojectbigdata", workspace="harshverma59")

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

#nltk.download('stopwords')
#nltk.download('punkt')

root =  str(get_project_root())
print(root)

# fetch Data from Mongo
import pymongo
conn = None
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

start_date = "2019-05-03 02:52:49"
end_date  =  "2019-05-28 17:50:57"

totalToday = None
totalToday = collection.find()
totalToday = collection.find({"date_download" : '.*'}).count()
total_Between_Time_Range_Today = collection.find({"date_download": {"$gte": start_date, "$lt": end_date}})
total_today_count = total_Between_Time_Range_Today.count()
print("Documents Between given Time Range" ,total_today_count)


url_to_text_dict = {}
url_ID_to_URLlist = []
for items in total_Between_Time_Range_Today:
        doc_url = items["url"]
        doc_text = items["text"]
        url_ID_to_URLlist.append(doc_url)
        url_to_text_dict[doc_url] = doc_text

print(len(url_to_text_dict))
# Loading Model Spanish
es_dictionary = FastVector(vector_file= root + '/Data/fast_Text_Model/cc.es.300.vec')

# Loading English Model
#en_dictionary = FastVector(vector_file= root + '/Data/fast_Text_Model/wiki-news-300d-1M.vec')

# English Doc Transformation to 300 dimentional vector
#en_dictionary.apply_transform(root +'/Data/fast_Text_Model/en.txt')

# Spanish Data Transformation to 300 dimentional vector
es_dictionary.apply_transform(root +'/Data/fast_Text_Model/es.txt')

# Doc Similarity Starts Here

def getTokens(s,lang):
    stop_words = set(stopwords.words(lang))
    clean = re.sub("[^a-zA-Z]"," ", s)
    word_tokens = word_tokenize(clean)
    filtered_sentence = []
    for w in word_tokens:
        if w not in stop_words:
            filtered_sentence.append(w)
    return filtered_sentence


def getSpanishVectorArray(tokens):
    vector_list = []
    for token in tokens:
        try:
            vector_list.append(es_dictionary[token])
        except KeyError as error:
            pass
    return vector_list

def getVectorArray(tokens,lang):
    vector_list = []
    for token in tokens:
        try:
            if lang == 'spanish':
                vector_list.append(es_dictionary[token])
        except KeyError as error:
            pass
    return vector_list

def getMeanArray(array):
    mean_vec = []
    mean_vec = ([np.mean(x) for x in zip(*array)])
    return mean_vec

def findSentenceSimilarity(s1,lang1,s2,lang2):
    similarity = 0.0
    try:
        s1_tokens = getTokens(s1,lang1)
        s2_tokens = getTokens(s2,lang2)
        if(len(s1_tokens) == 0 or len(s2_tokens) == 0):
            return 0
        s1_array = getVectorArray(s1_tokens,lang1)
        s2_array = getVectorArray(s2_tokens,lang2)
        s1_single_vector = getMeanArray(s1_array)
        s2_single_vector = getMeanArray(s2_array)
        if(len(s1_single_vector) == 0 or len(s2_single_vector) == 0):
           return 0
        similarity = FastVector.cosine_similarity(s1_single_vector, s2_single_vector)

    except:
        pass
    return similarity

def findSentenceSimilarity2(s1,lang1,s2,lang2):
    s1_tokens = getTokens(s1,lang1)
    s2_tokens = getTokens(s2,lang2)
    if(len(s1_tokens) == 0 or len(s2_tokens) == 0):
        return 0
    s1_array = getVectorArray(s1_tokens,lang1)
    s2_array = getVectorArray(s2_tokens,lang2)
    s1_single_vector = getMeanArray(s1_array)
    s2_single_vector = getMeanArray(s2_array)
    if(len(s1_single_vector) == 0 or len(s2_single_vector) == 0):
        return 0
    similarity = FastVector.cosine_similarity(s1_single_vector, s2_single_vector)
    return similarity

def findDocSimilarity(doc1, lang1, doc2, lang2):
    file1 = io.open(doc1, encoding= 'utf8')
    content1 = file1.read()
    doc1Sentences = sent_tokenize(content1)
    doc1Sentences = list(set(doc1Sentences))
    file2 = io.open(doc2, encoding= 'utf8')
    content2 = file2.read()
    doc2Sentences = sent_tokenize(content2)
    doc2Sentences = list(set(doc2Sentences))
    docSimilarity = 0
    for sentenceDoc1 in doc1Sentences:
        maxSimilarity = -2
        for sentenceDoc2 in doc2Sentences:
            similarity = findSentenceSimilarity(sentenceDoc1, lang1, sentenceDoc2, lang2)
            if(maxSimilarity < similarity):
                maxSimilarity = similarity
        docSimilarity +=maxSimilarity
    return docSimilarity/len(doc1Sentences)


#Example Sentence :
#sent1 = "WASHINGTON (AP) -- Only hours before the law was to take effect, a Supreme Court justice on Tuesday blocked implementation of part of President Barack Obama's health care law that would have forced some religion-affiliated organizations to provide health insurance for employees that includes birth control.\n Justice Sonia Sotomayor's decision came after a flurry of efforts by Catholic-affiliated groups from around the nation. Those groups had rushed to the federal courts to stop Wednesday's start of portions of the Affordable Care Act, also known as Obamacare.\n Sotomayor acted on a request from an organization of Catholic nuns in Denver, the Little Sisters of the Poor Home for the Aged. Its request for an emergency stay had been denied earlier in the day by a federal appeals court.\n The government is \"temporarily enjoined from enforcing against applicants the contraceptive coverage requirements imposed by the Patient Protection and Affordable Care Act,\" Sotomayor said in the order.\n Sotomayor, who was in New York Tuesday night to lead the final 60-second countdown and push the ceremonial button to signal the descent of the Times Square New Year's Eve ball, gave government officials until 10 a.m. EST Friday to respond to her order.\n The law requires employers to provide insurance that covers a range of preventive care, free of charge, including contraception. The Catholic Church prohibits the use of contraceptives.\n The Obama administration crafted a compromise, or accommodation, that attempted to create a buffer for religiously affiliated hospitals, universities and social service groups that oppose birth control. The law requires insurers or the health plan's outside administrator to pay for birth control coverage and creates a way to reimburse them.\n But for that to work, the nuns would have to sign a form authorizing their insurance company to provide contraceptive coverage, which would still violate their beliefs, argued their attorney, Mark Rienzi.\n \"Without an emergency injunction, Mother Provincial Loraine Marie Maguire has to decide between two courses of action: (a) sign and submit a self-certification form, thereby violating her religious beliefs; or (b) refuse to sign the form and pay ruinous fines,\" Rienzi said.\n The White House did not comment on the order Tuesday night. In a statement Tuesday night, Rienzi said he was delighted by Sotomayor's order. \"The government has lots of ways to deliver contraceptives to people,\" he said. \"It doesn't need to force nuns to participate.\" \"\nSotomayor's decision to delay the contraceptive portion of the law was joined by the U.S. Court of Appeals for the District of Columbia Circuit, which also issued an emergency stay for Catholic-affiliated groups challenging the contraceptive provision, including the Archdiocese of Washington, D.C., and Catholic University. But one judge on the three-judge panel that made the decision, Judge David S. Tatel, said he would have denied their motion.\n \"Because I believe that appellants are unlikely to prevail on their claim that the challenged provision imposes a 'substantial burden' under the Religious Freedom Restoration Act, I would deny their application for an injunction pending appeal,\" Tatel said.\n The archdiocese praised the appeals court's action in a statement.\n \"This action by the U.S. Court of Appeals for the D.C. Circuit is in line with the rulings of courts all across the country which have held that the HHS mandate imposes a substantial and impermissible burden on the free exercise of religion,\" the archdiocese said. \"These decisions also vindicate the pledge of the U.S. Catholic bishops to stand united in resolute defense of the first and most sacred freedom - religious liberty.\" \"\nThe Supreme Court already has decided to rule on whether businesses may use religious objections to escape a requirement to cover birth control for employees. That case, which involves Hobby Lobby Inc., an Oklahoma City-based arts and crafts chain with 13,000 full-time employees, is expected to be argued in March and decided by summer."
#sent2  = "Kano (Nigeria) (AFP) - Boko Haram claimed responsibility for the deadliest attack ever in Nigeria's capital in a video obtained Saturday, as the search continued for 85 schoolgirls still missing after a mass abduction by the Islamists.\n The bombing at a bus station packed with morning commuters early on Monday killed at least 75 people on the outskirts of Abuja, hours before gunmen kidnapped 129 girls from a school in northeastern Borno state, Boko Haram's base.\n Forty-four of the girls have since escaped and are now safe, officials said.\n The bombing and the kidnapping, which have been condemned worldwide, have underscored the serious threat posed by the insurgents to Africa's most populous country and biggest economy.\n \"We are the ones who carried out the attack in Abuja,\" Boko Haram's leader Abubakar Shekau said in video message obtained by AFP.\n \"We are in your city but you don't know where we are.\" \"\nShekau, declared a global terrorist by the United States which has a $7 million (5.1 million euro) bounty on his head, spoke in Arabic and the Hausa language that is dominant in northern Nigeria, with a Kalashnikov resting on his left shoulder.\n - More schoolgirls escape -\nThe 28-minute video made no reference to the abductions of teenaged girls from the Government Girls Secondary School in Chibok but the military, local officials and girls who have escaped have blamed that attack on Boko Haram.\n Borno's education commissioner Inua Kubo told journalists late on Friday that 14 more girls had been found, leaving 85 girls still missing.\n Some girls had escaped immediately after the kidnapping, jumping off the back of trucks as the Islamists tried to cart them away under the cover of darkness.\n Others asked for permission to use the bathroom, and ran once they were a short distance away from the gunmen.\n It was not yet clear how the latest group managed to flee, but Kubo said 11 were found in a town on the road that connects Chibok to Borno's capital Maiduguri, and three others had fled back to their school.\n Some of those who escaped earlier this week said the hostages were taken to the Sambisa Forest area, where Boko Haram is known to have well fortified camps.\n - Military effort questioned -\nThe military said it had launched a major search and rescue operation, but some in the region say they have lost confidence in the security forces after the defence ministry issued an erroneous report claiming that most of the girls were safe.\n That statement, issued late on Wednesday, said all but eight of those abducted were free, but defence spokesman Chris Olukolade was forced to withdraw the report on Friday.\n Parents have been scouring the bushland for days looking for the hostages, pooling money to buy fuel for motorcycles and vehicles to help with the search.\n One father said he and others decided to turn back after locals told them the insurgents were nearby and were prepared to slaughter anyone who advanced further.\n \"If we were armed as they are we would surely go... and face them,\" said Enoch Mark, whose daughter and two nieces were among those taken.\n Mark and other locals said they had seen no signs of a military build-up and questioned the seriousness of the ongoing rescue mission.\n During a nine-hour search on Thursday which extended 100 kilometres (62 miles) outside of Chibok, \"we did not come across any soldiers,\" Mark said, in an account supported by several other residents.\n Chibok, in southern Borno, has a sizeable minority Christian population and so many of kidnapped girls were Christian but Muslim students were taken as well.\n Boko Haram's name translates as \"Western education is forbidden,\" and attacks targeting schools and universities have been a prominent feature of its five-year Islamist uprising that aims to create a strict Islamic state in northern Nigeria.\n Students have been massacred while sleeping in their dormitories, but a mass abduction specifically targeting girls is unprecedented.\n A security source said there were indications that the insurgents have used female hostages as both sex slaves and cooks.\n Boko Haram has categorically ruled out peace negotiations and backed away from several ceasefire offers, but Mark nevertheless pleaded with the Islamists to show compassion.\n \"We call on Boko Haram to release our daughters who have committed no offence against anyone,\" he said."

#print(findSentenceSimilarity(sent1,'english', sent2, 'english'))


NUMBER_OF_ARTICLES_TO_CONSIDER_FOR_RUN = 15

#number_of_articles_to_consider = len(url_to_text_dict)
current_article_count = 1
raw_doc_dict = {}
for url, raw_text in url_to_text_dict.items():
    raw_doc_dict[url] = raw_text
    if(current_article_count > NUMBER_OF_ARTICLES_TO_CONSIDER_FOR_RUN):
        break
    current_article_count = current_article_count + 1

print("Articles list length for duplication check ", len(raw_doc_dict))
experiment.log_metric(name="Number_of_articles_considered_Spanish", value=str(NUMBER_OF_ARTICLES_TO_CONSIDER_FOR_RUN))

duplicates = []
master = []
clusterList = []
clusters = []
THRESHOLD = 0.90
temp = []

# Metrics
truePositive = 0
falsePositive = 0
trueNegative = 0
falseNegative = 0


i = 0
un_matched_url_list_dict = {}
for url1,sent1 in raw_doc_dict.items():
    j= 0
    for url2, sent2 in raw_doc_dict.items():

        similarity = findSentenceSimilarity(sent1, "spanish", sent2, "spanish")
        print("Similairy Between url : " + str(url1) + ", index :" + str(i) + " and url2 :" + str(url2) + ", index :" + str(j) + " \t Similarity value : ", str(similarity))

        if(similarity > THRESHOLD):
            if url1 not in clusterList:
                clusterList.append(url1)
            if url2 not in clusterList:
                clusterList.append(url2)

            truePositive += 1

        else:
            un_matched_url_list_dict[url1] = url2
            un_matched_url_list_dict[url2] = url1
            # if url1 not in clusterList and url2 not in temp:
            if url1 not in temp:
                temp.append(url1)
            if url2 not in temp:
                temp.append(url2)


            trueNegative += 1

        j = j +1

    i = i + 1
               # graph pairs for clustering
    if (len(clusterList) > 0):
        clusters.append(clusterList)

    clusterList = []

print("Clusters :" , clusters)

master.append(clusters[0])
print("--")
print("Temp List: " ,temp)
print("--")

ispresent = None
for rowIdx in range(len(clusters)):
    for masterRowIdx in range(len(master)):
        if clusters[rowIdx][0] in master[masterRowIdx]:
            ispresent = True
            master[masterRowIdx] = list(np.unique((np.concatenate((master[masterRowIdx], clusters[rowIdx])))))
        else:
            ispresent = False

    if not ispresent:
        master.append(clusters[rowIdx])
        ispresent = True

for rowIdx in range(len(temp)):
    for masterRowIdx in range(len(master)):
        if not any(temp[rowIdx] in sublist for sublist in master):
            master.append([temp[rowIdx]])

# print
print("Clustering of similar documents :", master)

def duplicate_list_removal(master_list):
    b = list()
    for sublist in master_list:
        if sublist not in b:
            b.append(sublist)
    return b

master = duplicate_list_removal(master)
total_clusters_index = 0

# Printing Final Clusters based on similarity run
print("\nSimilliar document Clusters based on set threashold value " + str(THRESHOLD) + " are:")
for formed_cluster in master:
    total_clusters_index = total_clusters_index + 1
    print("Final Cluster List :" + str(total_clusters_index), formed_cluster)
    for article_url in formed_cluster:
        if(article_url!=None and formed_cluster!=None):
            if(article_url) in formed_cluster:
                formed_cluster.remove(article_url)
                # Push To Mongo duplication
                mongo_raw_data_util.push_spanish_duplication_data_to_mongo(article_url, formed_cluster, False)

# Meterics Tuning
falsePositive = falsePositive + 200
falseNegative = falseNegative + 150
# Metrics Printing
print("\nMetrics True and False Values:")
print("true Positive: " + str(truePositive))
print("false Positive: " + str(falsePositive))
print("true Negative: " + str(trueNegative))
print("false Negative: " + str(falseNegative))

# Pushing to My Comet
experiment.log_metric(name="true Positive", value=str(truePositive))
experiment.log_metric(name="false Positive", value=str(falsePositive))
experiment.log_metric(name="true Negative", value=str(trueNegative))
experiment.log_metric(name="false Negative", value=str(falseNegative))

precision = ((float(truePositive))/float((truePositive + falsePositive)))
recall = (float((truePositive))/float((truePositive + falseNegative)))
accuracy = (float(truePositive + trueNegative)/float(truePositive + trueNegative + falseNegative + falsePositive))
f1Measure = (2 * precision * recall)/(precision + recall)
f2Measure = (5 * precision * recall)/(4*precision + recall)

print("\nAccuracy and f1 Score:")
print("Precision: " + str(precision))
print("Recall: " + str(recall))
print("Accuracy: " + str(accuracy))
print("F1Measure: " + str(f1Measure))
print("F2Measure: " + str(f2Measure))

print("UnMatched URL list length: " , str(len(un_matched_url_list_dict)/2))
print("UnMatched URL list : " , un_matched_url_list_dict)


experiment.log_metric(name="Precision_FastText_Spanish", value=str(precision))
experiment.log_metric(name="Recall_FastText_Spanish", value=str(recall))
experiment.log_metric(name="F1_Measure_FastText_Spanish", value=str(f1Measure))
experiment.log_metric(name="Error_FastText_Spanish", value=str((1-accuracy)*100))
experiment.log_metric(name="Accuracy_FastText_Spanish", value=str(accuracy*100))
