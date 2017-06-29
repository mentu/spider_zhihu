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
for each_page in db.answers.find():
    for each_answer in each_page['data']:
        question_id = each_answer['question']['id']
        question_list.append(question_id)

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
    question_id = q.get(timeout=2)
    print (threadName, question_id)
    url1 = 'https://www.zhihu.com/question/'
    url = url1 + str(question_id) + '/log'

    agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
    headers = {
        "Host" : "www.zhihu.com",
        "Origin": "https://www.zhihu.com",
        "Referer": url,
        'User-Agent': agent
    }

    try:           
        html = scrap(url, headers)
        soup = BeautifulSoup(html,'lxml')
        div_list = soup.find_all('div', class_='zm-item')
        creator_name = div_list[len(div_list)-1].div.a.text
        try:
            creator_url = div_list[len(div_list)-1].div.a['href']
            creator_url_token = creator_url.split('/')[2]

            headers = {
                "Host": "www.zhihu.com",
                "Referer": "https://www.zhihu.com/people/" + creator_url_token,
                'User-Agent': agent
            }
            url1 = 'https://www.zhihu.com/api/v4/members/'
            url2 ='?include=locations%2Cemployments%2Cgender%2Ceducations%2Cbusiness%2Cvoteup_count%2Cthanked_Count%2Cfollower_count%2Cfollowing_count%2Ccover_url%2Cfollowing_topic_count%2Cfollowing_question_count%2Cfollowing_favlists_count%2Cfollowing_columns_count%2Canswer_count%2Carticles_count%2Cpins_count%2Cquestion_count%2Ccommercial_question_count%2Cfavorite_count%2Cfavorited_count%2Clogs_count%2Cmarked_answers_count%2Cmarked_answers_text%2Cmessage_thread_token%2Caccount_status%2Cis_active%2Cis_force_renamed%2Cis_bind_sina%2Csina_weibo_url%2Csina_weibo_name%2Cshow_sina_weibo%2Cis_blocking%2Cis_blocked%2Cis_following%2Cis_followed%2Cmutual_followees_count%2Cvote_to_count%2Cvote_from_count%2Cthank_to_count%2Cthank_from_count%2Cthanked_count%2Cdescription%2Chosted_live_count%2Cparticipated_live_count%2Callow_message%2Cindustry_category%2Corg_name%2Corg_homepage%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics'
            url_creator = url1 + creator_url_token + url2

            html_creator = scrap(url_creator, headers)
            decodejson = json.loads(html_creator)
            decodejson['question_id'] = question_id
        except:
            decodejson={
                'question_id':question_id,
                'name': creator_name
            }
        db.answer_question_creator2.insert_one(decodejson)
        print (q.qsize(), threadName, "write in ", question_id)

    except Exception as e:
        print (question_id, e, url)
        error_str = '\t'.join([question_id, str(e), url]) + '\r\n'
        with open('error.txt', "a+") as f:
            f.write(error_str)
            f.close()
        return


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
for question_id in question_list:
    workQueue.put(question_id)

# 等待所有线程完成
for t in threads:
    t.join()
print ("Exiting Main Thread")