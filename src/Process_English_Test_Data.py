import src.export_file_data_to_mongo as export_file_data_to_mongo
import src.mongo_raw_data_util as mongo_raw_data_util
from pathlib import Path

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

root = str(get_project_root())

# Code to Duump Eroot = str(get_project_root())
english_file_output_path_with_name = root + "/Data/" + "english_raw.txt"
input_data_path = root + "/Data/english_articles_test_data"
mongo_raw_data_util.english_extract_json_files_and_push_Data_to_file(input_data_path, english_file_output_path_with_name)

# Code to Export English Raw Data from File to Mongo
# Generated File to MongoDB
input_data_path = root + "/Data/"
db_name = "BDMA_PROJ"
input_file_name = "english_raw.txt"
collection_name = "news_raw_english"

rows_inserted = export_file_data_to_mongo.export_raw_data_mongo(input_data_path,input_file_name , db_name , collection_name)
print("number of English raw data rows inserted in mongodb ", rows_inserted)