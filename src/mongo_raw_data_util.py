import pymongo
import io, json
import glob
from pathlib import Path
import datetime

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent


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

def extract_json_files_and_push_Data_to_file(dir_path, output_data_file_name, current_date_articles_only=False):
    all_dir_path = dir_path + "/*"

    all_folders = glob.glob(all_dir_path)
    print("All Folders : ", all_folders)

    all_json_files = []
    for folder in all_folders:
        folder = folder + "/*"
        files = glob.glob(folder)
        for file in files:
            if (file.endswith('.json')):
                all_json_files.append(file)

    print("total Json Files in all directories :", len(all_json_files))

    null_json_count = 0
    total_processed_json = 0

    #Current-Date-Processing
    now = datetime.datetime.now()
    month = now.month
    if (len(str(month)) == 1):
        month = "0" + str(month)

    day = now.day
    if (len(str(day)) == 1):
        day = day-1
        day = "0" + str(day)


    date_time_curr_date = datetime.datetime(now.year , int(month) , int(day))

    with open(output_data_file_name, 'w', encoding='utf-8') as file_writer:
        for json_file in all_json_files:
            print(json_file)

            with open(json_file, 'r', encoding='latin1') as f:
                try:
                    data = json.load(f)
                    date_published = data["date_publish"]
                    date_modify = data["date_modify"]

                    if(date_published == None):
                        date_published = "0000-00-00 00:00:00"

                    if(date_modify == None):
                        date_modify = "0000-00-00 00:00:00"

                    # Comparing the dates will return either True or False
                    # Format Date in Mongo : date : 2019-05-03 02:52:49
                    date_published_data_list = date_published.split("-")
                    date_modified_data_list = date_modify.split("-")

                    date_time_published_date = datetime.datetime(int(date_published_data_list[0]), int(date_published_data_list[1]), int(date_published_data_list[2]))
                    date_time_modified_date = datetime.datetime(int(date_modified_data_list[0]), int(date_modified_data_list[1]), int(date_modified_data_list[2]))

                    # Consider only Current Date Articles Check Handled here
                    if(current_date_articles_only):
                        if(date_time_published_date > date_time_curr_date or date_time_modified_date > date_time_curr_date):
                            file_writer.write(json.dumps(data, ensure_ascii=False))
                            file_writer.write("\n")
                            total_processed_json = total_processed_json + 1
                    else:
                        file_writer.write(json.dumps(data, ensure_ascii=False))
                        file_writer.write("\n")
                        total_processed_json = total_processed_json + 1

                except:
                    print("error decoding json :", f)
                    null_json_count = null_json_count + 1
                    pass

    print("Processed JSON from files :", str(total_processed_json))
    print("Total null jsons in file :", str(null_json_count))
    print("Successfully Dumped data to file: ", output_data_file_name)

def push_spanish_duplication_data_to_mongo(url, duplication_urls_list, push_to_mongo):
    dat = {
        'url': url,
        'duplicate_url_list': duplication_urls_list,
        'is_duplicate' : True
    }

    if(push_to_mongo):
        db = pymongo.MongoClient("mongodb://localhost:27017/")["BDMA_PROJ"]["spanish_duplicate_articles"]
        db.insert_one(dat)

def push_english_duplication_data_to_mongo(url, duplication_urls_list, push_to_mongo):
    dat = {
        'url': url,
        'duplicate_url_list': duplication_urls_list,
        'is_duplicate' : True
    }
    if(push_to_mongo):
        db = pymongo.MongoClient("mongodb://localhost:27017/")["BDMA_PROJ"]["english_duplicate_articles"]
        db.insert_one(dat)



def english_extract_json_files_and_push_Data_to_file(dir_path, output_data_file_name):
    all_dir_path = dir_path + "/*"
    files = glob.glob(all_dir_path)
    print("All Folders : ", files)

    all_json_files = []
    for file in files:
        if (file.endswith('.json')):
            all_json_files.append(file)

    print("total Json Files in all directories :", len(all_json_files))

    null_json_count = 0
    total_processed_json = 0
    with open(output_data_file_name, 'w', encoding='utf-8') as file_writer:
        for json_file in all_json_files:
            print(json_file)

            with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(data)
                    for key, value in data.items():
                        print("key: {} | value: {}".format(key, value))
                        first_value = value
                        if(key=="meta"):
                            break
                        try:
                            articles = first_value["articles"]
                            results = articles["results"]
                            print(results)
                            for result in results:
                                url = result["url"]
                                text = result["body"]
                                title = result["title"]
                                date = result["date"]
                                print(url, title, text)
                                data = {"url": url, "date" : date, "title": title, "text": text}
                                file_writer.write(json.dumps(data, ensure_ascii=False))
                                file_writer.write("\n")
                                total_processed_json = total_processed_json + 1
                        except:
                            print("ERROR while parsing")
                            null_json_count= null_json_count+1
                            pass


    print("Processed English JSON from files :", str(total_processed_json))
    print("Total null jsons in file :", str(null_json_count))
    print("Successfully Dumped and processed English Test Json data to file: ", output_data_file_name)