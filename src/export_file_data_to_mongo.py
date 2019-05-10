import pymongo
import json
from pathlib import Path

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

def export_raw_data_mongo(input_data_path, input_data_file_name ,db_name , collection_name):
    db = pymongo.MongoClient("mongodb://localhost:27017/")[db_name][collection_name]
    raw_data = open(input_data_path + input_data_file_name, "r" ,encoding='utf-8' )
    line = raw_data.readline()
    i = 1
    while line:
        if len(line) > 0:
            db.insert_one(json.loads(line))
            print("Wrote", i, "documents")
            i += 1
        line = raw_data.readline()

    return i
