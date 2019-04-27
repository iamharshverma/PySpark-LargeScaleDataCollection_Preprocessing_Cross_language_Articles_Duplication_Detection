import pymongo
import json
from pathlib import Path

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

root = str(get_project_root())

input_data_path = root + "/Data/"

db = pymongo.MongoClient("mongodb://localhost:27017/")["BDMA_PROJ"]["news_raw"]
raw_data = open(input_data_path + "news_raw.txt", "r")
line = raw_data.readline()
i = 1
while line:
    if len(line) > 0:
        db.insert_one(json.loads(line))
        print("Wrote", i, "documents")
        i += 1
    line = raw_data.readline()


def export_raw_data_mongo(input_data_path, input_data_file_name ,db_name , collection_name):
    db = pymongo.MongoClient("mongodb://localhost:27017/")[db_name][collection_name]
    raw_data = open(input_data_path + input_data_file_name, "r")
    line = raw_data.readline()
    i = 1
    while line:
        if len(line) > 0:
            db.insert_one(json.loads(line))
            print("Wrote", i, "documents")
            i += 1
        line = raw_data.readline()

    return i
