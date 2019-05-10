![ppt1](ReportDocumentation/Screenshots/PPT_Screenshots/1.png)
![1](ReportDocumentation/Screenshots/Report_Screenshots/1.png)
![2](ReportDocumentation/Screenshots/Report_Screenshots/2.png)
![3](ReportDocumentation/Screenshots/Report_Screenshots/3.png)
![4](ReportDocumentation/Screenshots/Report_Screenshots/4.png)
![5](ReportDocumentation/Screenshots/Report_Screenshots/5.png)
![6](ReportDocumentation/Screenshots/Report_Screenshots/6.png)
![7](ReportDocumentation/Screenshots/Report_Screenshots/7.png)
![8](ReportDocumentation/Screenshots/Report_Screenshots/8.png)
![9](ReportDocumentation/Screenshots/Report_Screenshots/9.png)
![10](ReportDocumentation/Screenshots/Report_Screenshots/10.png)
![11](ReportDocumentation/Screenshots/Report_Screenshots/11.png)
![12](ReportDocumentation/Screenshots/Report_Screenshots/12.png)
![13](ReportDocumentation/Screenshots/Report_Screenshots/13.png)

**Project Full Architecture**
![ppt2](ReportDocumentation/Screenshots/PPT_Screenshots/2.png)

**Spanish and English Articles Duplication Algorithm and Metrics Results Discussion**
![ppt4](ReportDocumentation/Screenshots/PPT_Screenshots/4.png)
![ppt5](ReportDocumentation/Screenshots/PPT_Screenshots/5.png)
![ppt6](ReportDocumentation/Screenshots/PPT_Screenshots/6.png)
![ppt7](ReportDocumentation/Screenshots/PPT_Screenshots/7.png)
![ppt8](ReportDocumentation/Screenshots/PPT_Screenshots/8.png)
![ppt9](ReportDocumentation/Screenshots/PPT_Screenshots/9.png)
![ppt10](ReportDocumentation/Screenshots/PPT_Screenshots/10.png)
![ppt11](ReportDocumentation/Screenshots/PPT_Screenshots/11.png)

**Readme Notes :**

**How to Execute Project ::**

1. Before Running MainStartPoint.py Start the relavent server :
-> Start Mongo Server : mongod
-> Start MongoDB Compass
-> Start Kafka Server and Zookeper: zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
-> Start Spark Streaming: spark-submit --jars /Users/harshverma/PycharmProjects/Big_Data_Final_Project/Data/spark_streaming_jar/spark-streaming-kafka-0-8-assembly_2.11-2.4.1.jar  /Users/harshverma/PycharmProjects/Big_Data_Final_Project/src/udpipe_stream_processing.py
-> Start news-please using script sh scraph_data_news_please.sh

:**Start MainStartPoint.py:**

:**2. Run Duplication Algorithms:**

Run Different types of similarity algorithms i.e FastText, Jaccard_Similarity , Min-Hash Similarity, The similairty files is present in respective folder

:**1.  FastText_Similarity :**: fastTextSimilarity_English_Articles.py : For english articles duplication Similarity
                          fastTextSimilarity_Spanish_Articles.py : For spanish articles duplication Similarity


:**2. Jaccard_Similarity ::** EnglishJacardSimilairyAlgo.py :  For english articles duplication Similarity
                        SpanishJacardSimilairyAlgo.py :  For spanish articles duplication Similarity


:**3. Min-Hash_Similarity::** FindingDuplicates_MinHash_English.py : For english articles duplication Similarity
                        FindingDuplicates_MinHash_Spanish.py : For spanish articles duplication Similarity


Check the output in terminal or save it to output file.


:**Report/Documentation/Presentation::**

The report and presentation/Document is present in ReportDocumentation folder
