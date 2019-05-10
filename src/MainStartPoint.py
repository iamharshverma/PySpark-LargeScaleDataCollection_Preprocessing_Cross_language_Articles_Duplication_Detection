import src.news_articles_util as scrap_news_articles
import src.export_file_data_to_mongo as export_file_data_to_mongo
import src.kafka_produce as kafka_produce
import src.mongo_raw_data_util as mongo_raw_data_util
from pathlib import Path

# Before Running Main :
# Start Mongo Server : mongod
# Start MongoDB Compass
# Start Kafka Server and Zookeper: zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
# Start Spark Streaming: spark-submit --jars /Users/harshverma/PycharmProjects/Big_Data_Final_Project/Data/spark_streaming_jar/spark-streaming-kafka-0-8-assembly_2.11-2.4.1.jar  /Users/harshverma/PycharmProjects/Big_Data_Final_Project/src/udpipe_stream_processing.py
# Run Duplication Algorithms

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

root = str(get_project_root())

if __name__ == "__main__":
    print("Starting Execution")
    # Downloading Data from News-Please
    timeout_sec = 0
    time_in_minutes = 5

    total_timeout = time_in_minutes * timeout_sec
    root_path_news_please = "/Users/harshverma/news-please-repo/data/"
    news_downloded_data_path = scrap_news_articles.remove_old_run_data_and_return_data_path_used_by_news_please(root_path_news_please, total_timeout, False)
    print("News Data Downloded at path :" , news_downloded_data_path)


    # Mongo Part Extract JSON Files and write url, Json to news_raw file inside Data folder
    input_file_name = "news_raw.txt"
    file_output_path_with_name = root + "/Data/" + input_file_name
    mongo_raw_data_util.extract_json_files_and_push_Data_to_file(news_downloded_data_path, file_output_path_with_name)

    #Generated File to MongoDB
    input_data_path = root + "/Data/"
    db_name = "BDMA_PROJ"
    collection_name = "news_raw"
    rows_inserted = export_file_data_to_mongo.export_raw_data_mongo(input_data_path,input_file_name , db_name , collection_name)
    print("number of raw data rows inserted in mongodb ", rows_inserted)


    # Push Data from MongoDb to Kafka
    # Start Zookeper : zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
    # Create Kafka Topic : kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic espanol-news
    kafka_topic = 'espanol-news'
    number_of_articles_pushed_to_kafka = kafka_produce.read_MongoCollection_push_Data_to_kafka(db_name, collection_name, kafka_topic)
    print("Number of articles pushed to kafka" , number_of_articles_pushed_to_kafka)

    #Read from Kafka and Push Again parsed Data via UDPipe to MongoDB

    # spark-submit --jars /Users/harshverma/PycharmProjects/Big_Data_Final_Project/Data/spark_streaming_jar/spark-streaming-kafka-0-8-assembly_2.11-2.4.1.jar  /Users/harshverma/PycharmProjects/Big_Data_Final_Project/src/udpipe_stream_processing.py
    # Mongo-DB Test url : {url : "https://www.proceso.com.mx/575830/de-carlos-santana-a-cafe-tacuba-los-paisajes-del-vive-latino"}

    #Start Duplication Part, read 1000 articles from mongo and do content level similarity check with Algo1

    # Run file SpanishJacardSimilairyAlgo.py

    #Start Duplication Part, read 1000 articles from mongo and do content level similarity check with Algo2

    # Run file runMinHash.py

    #Start Duplication using fasttext and save final Result from Similarity file to output Mongo DB Again
    # Run file fastTextSimilarity_English_Articles.py for English and fastTextSimilarity_Spanish_Articles.py for Spanish