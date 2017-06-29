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
from bs4 import BeautifulSoup

client = MongoClient('localhost',27017)
db = client.zhihu_sample_database

question_list = list()
for each_page in db.questions.find():
    for each_answer in each_page['data']:
        question_id = each_answer['id']
        question_created = each_answer['created']
        question_list.append([question_id, question_created])

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
    question = q.get(timeout=2)
    question_id = question[0]
    question_created = question[1]

    url1 = 'https://www.zhihu.com/question/'
    url = url1 + str(question_id) 

    agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
    headers = {
        "Host" : "www.zhihu.com",
        'User-Agent': agent
    }

    try:           
        html = scrap(url, headers)
        soup = BeautifulSoup(html,'lxml')
        tags = soup.find_all('a',class_= 'TopicLink')

        tag_list = [each.div.div.text.strip() for each in tags]
        title = soup.find('h1', class_ ='QuestionHeader-title').text.strip()
        q_descrip = soup.find('span', class_ ='RichText').text.strip()
        follow_view = soup.find_all('div', class_ ='NumberBoard-value')
        num_follow = follow_view[0].text.strip()
        num_view = follow_view[1].text.strip()
        decodejson = {
                'question_id': question_id,
                'created': question_created,
                'tag_list': tag_list,
                'title': title,
                'q_descrip': q_descrip,
                'num_follow': num_follow,
                'num_view': num_view
            }

        db.questions_tag.insert_one(decodejson)
        print (q.qsize(), threadName, "write in ", question_id)

    except Exception as e:
        print (e)
        error_str = '\t'.join([question_id, str(e), url]) + '\r\n'
        with open('error.txt', "a+") as f:
            f.write(error_str)
            f.close()


def scrap(url, headers, num_try = 3):
    try:
        r = session.get(url, headers=headers, timeout=10)
        html = r.text
        time.sleep(random.randint(5,6)+random.random())
    except Exception as e:
        print (html)
        print (num_try, e, url)
        if num_try >0:
            long_sleep = random.randint(120,130)+random.random()
            print ("?????????sleep 2 mins        num_try:", num_try)
            time.sleep(long_sleep)
            html = scrap(url, headers, num_try-1)
    return html

 
threadList = ["Thread-0", "Thread-1"]
queueLock = threading.Lock()
workQueue = Queue.Queue(1000)
threads = []
threadID = 1
 
# 创建新线程
for tName in threadList:
    thread = myThread(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1
 
# 填充队列
for url_token in question_list:
    workQueue.put(url_token)

# 等待所有线程完成
for t in threads:
    t.join()
print ("Exiting Main Thread")