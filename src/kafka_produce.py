import pymongo
from kafka import KafkaProducer


db = pymongo.MongoClient("mongodb://localhost:27017/")["BDMA_PROJ"]["news_raw"]
kafka_topic = 'espanol-news'
kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

for document in db.find():
    keyS = str(document['url'])
    valueS = str(document['text'])
    keyB = bytes(keyS, encoding='utf-8')
    valueB = bytes(valueS, encoding='utf-8')
    kafka_producer.send(kafka_topic, key=keyB, value=valueB)
    kafka_producer.flush()
    print("Article from", keyS, "published to Kafka")

def read_MongoCollection_push_Data_to_kafka(db_name , collection_name, kafka_topic):
    db = pymongo.MongoClient("mongodb://localhost:27017/")[db_name][collection_name]
    kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)
    count = 0
    for document in db.find():
        keyS = str(document['url'])
        valueS = str(document['text'])
        keyB = bytes(keyS, encoding='utf-8')
        valueB = bytes(valueS, encoding='utf-8')
        kafka_producer.send(kafka_topic, key=keyB, value=valueB)
        kafka_producer.flush()
        print("Article from", keyS, "published to Kafka")
        count = count +1
    return count
