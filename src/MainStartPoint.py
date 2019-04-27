import src.scrap_news_articles as scrap_news_articles
import src.export_file_data_to_mongo as export_file_data_to_mongo
import src.kafka_produce as kafka_produce
from pathlib import Path

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

root = str(get_project_root())

if __name__ == "__main__":
    print("Starting Execution")

    # Downloading Data from News-Please
    root_path_news_please = "/Users/harshverma/news-please-repo/data/"
    timeout_time = 20
    news_downloded_data_path = scrap_news_articles.run_news_please_return_data_path(root_path_news_please, timeout_time)
    print("Data Succesfully Downloded at path :" , news_downloded_data_path)

    #Mongo Part

    #Generated File to MongoDB
    input_data_path = root + "/Data/"
    input_file_name = "news_raw.txt"
    db_name = "BDMA_PROJ"
    collection_name = "news_raw"
    rows_inserted = export_file_data_to_mongo.export_raw_data_mongo(input_data_path,input_file_name , db_name , collection_name)
    print("number of raw data rows inserted in mongodb ", rows_inserted)

    # Push Data from MongoDb to Kafka
    kafka_topic = 'espanol-news'
    number_of_articles_pushed_to_kafka = kafka_produce.read_MongoCollection_push_Data_to_kafka(db_name, collection_name, kafka_topic)
    print("Number of articles pushed to kafka" , number_of_articles_pushed_to_kafka)

    #Read from Kafka and Push Again parsed Data via UDPipe to MongoDB


    #Start Duplication Part, read 1000 articles from mongo and do content level similarity check with Algo1

    #Start Duplication Part, read 1000 articles from mongo and do content level similarity check with Algo2

    #Save final Result from Similarity file to output Mongo DB Again