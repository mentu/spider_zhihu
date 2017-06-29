# -*- coding: UTF-8 -*-
 
import queue as Queue
import threading
import requests
import json
import os
import time
import random
import csv
try:
    import cookielib
except:
    import http.cookiejar as cookielib
import re
from pymongo import MongoClient

client = MongoClient('localhost',27017)
db = client.zhihu_sample_database
collection = db.questions

# 构造 Request headers
agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
headers = {
    "Host": "www.zhihu.com",
    "Referer": "https://www.zhihu.com/",
    'User-Agent': agent
}

# 使用登录cookie信息
session = requests.session()
session.cookies = cookielib.LWPCookieJar(filename='cookies')
try:
    session.cookies.load(ignore_discard=True)
except:
    print("Cookie 未能加载")


with open('sample_id.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile)
    user_list = list()
    for row in spamreader:
        url_token =  row[1]
        if "url_token" not in url_token:
            user_list.append(url_token)

class myThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        print ("Starting " + self.name)
        while True:
            try:
                geturl(self.name, self.q)
                time.sleep(1)
            except:
                break
        print ("Exiting " + self.name)
 
def geturl(threadName, q):
    url = q.get(timeout=2)

    re_token = re.search(r'v4/members/(.*?)/questions',url)
    url_token = re_token.group(1)

    offset_token = re.search(r'&offset=(.*)',url)
    offset = offset_token.group(1)  

    agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
    headers = {
        "Host" : "www.zhihu.com",
        "Referer": "https://www.zhihu.com/people/" + url_token + "/questions",
        'User-Agent': agent
    }

    try:           
        html = scrap(url, headers)
        decodejson = json.loads(html)
        decodejson['offset_num'] = offset
        decodejson['user_token'] = url_token
        decodejson['user_id'] = decodejson['data'][0]['author']['id']

        collection.insert_one(decodejson)
        print (q.qsize(), threadName, offset, "write in ", url_token)

        is_end = decodejson['paging']['is_end']
        next_page = decodejson['paging']['next']
        if not is_end:
            q.put(next_page)
    except Exception as e:
        print (e)
        error_str = '\t'.join([url_token, offset, str(e), url]) + '\r\n'
        with open('error.txt', "a+") as f:
            f.write(error_str)
            f.close()


def scrap(url, headers, num_try = 3):
    try:
        r = session.get(url, headers=headers, timeout=20)
        html = r.text
        decodejson = json.loads(html)
        data = decodejson['data']
        time.sleep(random.randint(5,6)+random.random())
    except Exception as e:
        print (html)
        print (num_try, e, url)
        if num_try >0 and str(html).find("error")>0:
            long_sleep = random.randint(120,130)+random.random()
            print ("?????????sleep 2 mins        num_try:", num_try)
            time.sleep(long_sleep)
            html = scrap(url, headers, num_try-1)
    return html

 
threadList = ["Thread-0", "Thread-1"]
queueLock = threading.Lock()
workQueue = Queue.Queue(10)
threads = []
threadID = 1
 
# 创建新线程
for tName in threadList:
    thread = myThread(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1
 
# 填充队列
for url_token in user_list:
    url1 = 'https://www.zhihu.com/api/v4/members/'
    url2 ='/questions?include=data%5B*%5D.is_normal%2Cis_collapsed%2Ccollapse_reason%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_authorized%2Cvoting%2Cis_author%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.badge%5B%3F(type%3Dbest_answerer)%5D.topics&limit=20&offset=0'
    url = url1 + url_token + url2
    workQueue.put(url)

# 等待所有线程完成
for t in threads:
    t.join()
print ("Exiting Main Thread")