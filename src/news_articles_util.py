import subprocess
import datetime
import os
import shutil
import time
import signal

# Start of News-Please Function
def run_news_please_return_data_path(root_path_news_please, timeout_time):

    now = datetime.datetime.now()
    month = now.month

    if (len(str(month)) == 1):
        month = "0" + str(month)

    day = now.day
    if (len(str(day)) == 1):
        day = "0" + str(day + 1)

    filename = os.path.join(root_path_news_please, str(now.year), month, day)
    print(filename)
    try:
        shutil.rmtree(filename)
        print("Succesfully Deleted Previous Run Data")
    except:
        print("No previous Run Directory present for current Date")
        pass

    print("Starting News-Please process, data will be saved in dir : ", filename)
    process = subprocess.Popen("news-please", shell=True)
    pid = process.pid
    process.wait(timeout_time)
    time.sleep(timeout_time)

    #Killing Started Process
    n = 1
    while n < 10:
        subprocess.Popen("kill -9 " + str(pid), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 1), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 2), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 3), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 4), shell=True)
        n = n + 1
        time.sleep(2)

    print("Articles scraping Completed")
    print("News-Please process completed, data will be saved in dir : ", filename)
    return filename


def remove_old_run_data_and_return_data_path_used_by_news_please(root_path_news_please, total_timeout , remove_old_data=True):
    now = datetime.datetime.now()
    month = now.month
    if (len(str(month)) == 1):
        month = "0" + str(month)

    day = now.day
    if (len(str(day)) == 1):
        day = "0" + str(day)
        # Day issue time format fix
        #day = "0" + str(day + 1)

    filename = os.path.join(root_path_news_please, str(now.year), month, day)
    print(filename)
    if(remove_old_data):
        try:
            shutil.rmtree(filename)
            print("Succesfully Deleted Previous Run Data")
        except:
            print("No previous Run Directory present for current Date")
            pass

    print("data will be saved in dir : ", filename)
    time.sleep(total_timeout)
    return filename