import subprocess
import datetime
import os
import shutil
import time
import signal

# Set Variables
# Set Time to use script in sec
root_path_news_please= "/Users/harshverma/news-please-repo/data/"
timeout_time = 20

now = datetime.datetime.now()
month = now.month

if(len(str(month))==1):
    month = "0"+str(month)
filename = os.path.join(root_path_news_please , str(now.year) ,month , str(now.day))
print(filename)
try:
    shutil.rmtree(filename)
    print("Succesfully Deleted Previous Run Data")
except:
    print("No previous Run Directory present for current Date")
    pass

print("Starting News-Please process, data will be saved in dir : " , filename)
process = subprocess.Popen("news-please")
time.sleep(timeout_time)
pid = process.pid
os.kill(pid, signal.SIGINT)
process.kill()
process.terminate()

while process.poll() is not None:  # Force kill if process is still alive
    time.sleep(2)
    os.killpg(process.pid, signal.SIGKILL)
    process.terminate()
print(process.pid)

# Killing Started Process
n=1
while n<10:
    subprocess.Popen("kill -9 "+ str(pid), shell=True)
    subprocess.Popen("kill -9 " + str(pid+1), shell=True)
    subprocess.Popen("kill -9 " + str(pid+2), shell=True)
    subprocess.Popen("kill -9 " + str(pid+3), shell=True)
    subprocess.Popen("kill -9 " + str(pid + 4), shell=True)
    n=n+1
    time.sleep(5)

print("Articles scraping Completed")
print("News-Please process completed, data will be saved in dir : " , filename)


# Start of News-Please Function
def run_news_please_return_data_path(root_path_news_please, timeout_time = 20):

    now = datetime.datetime.now()
    month = now.month

    if (len(str(month)) == 1):
        month = "0" + str(month)
    filename = os.path.join(root_path_news_please, str(now.year), month, str(now.day))
    print(filename)
    try:
        shutil.rmtree(filename)
        print("Succesfully Deleted Previous Run Data")
    except:
        print("No previous Run Directory present for current Date")
        pass

    print("Starting News-Please process, data will be saved in dir : ", filename)
    process = subprocess.Popen("news-please")
    time.sleep(timeout_time)
    pid = process.pid
    os.kill(pid, signal.SIGINT)
    process.kill()
    process.terminate()

    while process.poll() is not None:  # Force kill if process is still alive
        time.sleep(2)
        os.killpg(process.pid, signal.SIGKILL)
        process.terminate()
    print(process.pid)

    # Killing Started Process
    n = 1
    while n < 10:
        subprocess.Popen("kill -9 " + str(pid), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 1), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 2), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 3), shell=True)
        subprocess.Popen("kill -9 " + str(pid + 4), shell=True)
        n = n + 1
        time.sleep(5)

    print("Articles scraping Completed")
    print("News-Please process completed, data will be saved in dir : ", filename)
    return filename