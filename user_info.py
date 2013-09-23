# -*- coding: utf-8 -*-
__author__ = 'tea'
import requests
import re
import time
from redis_queue import RedisQueues
import lxml.html
import json
from crud0 import MongoCRUD
import multiprocessing
from db_test import conn_db, create_db
# def find_group(url,file,group_queue):
def find_group(url,group_queue):   #如果不需要保存至文件里面，就调用此函数
    sheet = []
    while url:
        page = requests.get(url)
        document_tree = lxml.html.fromstring(page.content)
        row = document_tree.xpath('//div[@class="pic"]/a')
        text = document_tree.xpath('//div[@class="info"]/text()')
        next = document_tree.xpath('//span[@class="next"]/link')
        if len(row) == len(text):
            length = len(row)
            for i in range(0,length):
                a = row[i]
                info1 = a.get('href')
                info2 = a.get('title')
                info3 = text[i]
                group_queue.put(info1)
                print 'info:',info1,' ',type(info1),len(info1),'   ',info2,' ',type(info1),len(info1),'   ',info3,' ',type(info1),len(info1),'\n'
                # data = (info1.encode('utf-8'),info2.encode('utf-8'),info3.encode('utf-8'))
                sheet.append((info1.encode('utf-8'),info2.encode('utf-8'),info3.encode('utf-8')))
                # file.write((info1.encode('utf-8'),info2.encode('utf-8'),info3.encode('utf-8')))
                # file.write('%s\t%s\t%s' % (info1.encode('utf-8'),info2.encode('utf-8'),info3.encode('utf-8')))
                # file.write('\n')   #如果不需要保存至文件，删掉上面一句和本句子
        if next:
            url = next[0].get('href')
        else:
            url = ''
    return sheet
def find_url(link_url, data_dbs):

    page = requests.get(link_url)
    document_tree = lxml.html.fromstring(page.content)
    all_people = document_tree.xpath('//div[@class="mod side-nav"]/p/a')
    if all_people:
        people_link = all_people[0].get('href')
        print '小组成员列表url:',people_link
        while people_link:
            peoples = []
            people_page = requests.get(people_link)
            document_tree = lxml.html.fromstring(people_page.content)
            people_href = document_tree.xpath('//ul/li/div[@class="pic"]/a')
            flag =  document_tree.xpath('//ul/li/div[@class="pic"]/a/img')
            next_flag = document_tree.xpath('//span[@class="next"]/link')
            # print  len(people_href),  len(flag)
            if people_href and len(people_href) == len(flag):
                print '该小组内总用户人数：',len(people_href)
                for i in range(0,len(people_href)):
                    url = people_href[i].get('href')
                    # print flag[i].get('alt')
                    if flag[i].get('alt').encode('utf-8') == "[已注销]":
                         print '该用户已经注销！'
                    else:
                        print '信息有效用户的待处理主页url--',url
                        people = {}
                        people['STATUE'] = 'ok'
                        people['URL'] = url
                        peoples.append(people)
                print '存入一组用户：'
                data_dbs.people_url_insert(peoples)
            if next_flag:
                people_link = next_flag[0].get('href')
            else:
                people_link = ''
    else:
        print '该小组no people!'
    return peoples
def find_people_url(link_url,people_queue):
    page = requests.get(link_url)
    document_tree = lxml.html.fromstring(page.content)
    all_people = document_tree.xpath('//div[@class="mod side-nav"]/p/a')
    if all_people:
        people_link = all_people[0].get('href')
        print '小组成员列表url:',people_link
        while people_link:
            people_page = requests.get(people_link)
            document_tree = lxml.html.fromstring(people_page.content)
            people_href = document_tree.xpath('//ul/li/div[@class="pic"]/a')
            flag =  document_tree.xpath('//ul/li/div[@class="pic"]/a/img')
            next_flag = document_tree.xpath('//span[@class="next"]/link')
            # print  len(people_href),  len(flag)
            if people_href and len(people_href) == len(flag):
                print '小组类用户人数：',len(people_href)
                for i in range(0,len(people_href)):
                    url = people_href[i].get('href')
                    # print flag[i].get('alt')
                    if flag[i].get('alt').encode('utf-8') == "[已注销]":
                         print '该用户已经注销'
                    else:
                        print '信息有效用户的待处理主页url--：',url
                        people_queue.put(url)
                        print '队列元素个数：',people_queue.length()
            if next_flag:
                people_link = next_flag[0].get('href')
            else:
                people_link = ''
    else:
        print 'no people!'


def find_people_UID(url):   #用户的UID
    print 'url----:',url
    id_s = re.compile(r'http://www.douban.com/people/(?P<word>[^/]*)/')
    id =re.search(id_s,url)
    id = id.group('word') #用户id号，都是是数字
    id_info = (id.encode('utf-8'),)
    # print 'id_info--', id_info
    return id.encode('utf-8')

def find_group_UID(url):   #用户的UID
    # print 'url----:',url
    id_s = re.compile(r'http://www.douban.com/group/(?P<word>[^/]*)/')
    id =re.search(id_s,url)
    if id:
        id = id.group('word') #用户id号，都是是数字
        return id
    else:
        return 'error_info'


def parse_group_detail(url):  #用户关注的小组
    note_groups = []
    # print '--url--',url
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    group_into = document_tree.xpath('//dl[@class="ob "]/dt/a')
    # print '2--',len(group_into)
    for i in group_into:
        url_group = i.get('href')
        # print '3--',url_group,type(url_group)
        # print '4--',find_group_UID(url_group)
        note_groups.append(find_group_UID(url_group))
    return note_groups

def find_people_group(url):  # 用户常去的小组
    # print 'id---:',id
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    # name_s = document_tree.xpath('//div[@class="user-info"]/div[@class="pl"]/text()')
    note_group = document_tree.xpath('//div[@id="group"]/h2/span/a')  #参加的小组
    if note_group:
        # print '1',note_group[0]
        url0 = note_group[0].get('href')
        return parse_group_detail(url0)
    else:
        return parse_group_detail(url)


def parse_next_movie(url): #下一页具体处理
    movies = []
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    # item = document_tree.xpath('//div[@class="grid-view"]/div[@class="item"]/div[@class="pic"]/a')
    item_title = document_tree.xpath('//div[@class="grid-view"]/div[@class="item"]/div[@class="info"]/ul/li/a/em/text()')
    item_url = document_tree.xpath('//div[@class="grid-view"]/div[@class="item"]/div[@class="info"]/ul/li/a')
    next = document_tree.xpath('//span[@class="next"]/a')
    if item_url and len(item_url) == len(item_title):
        for i in range(0,len(item_url)):
            movie =(item_url[i].get('href'),item_title[i])
            movies.append(movie)
            # print 'title1', item_title[i]

    # print 'movies',movies
    return next , movies
def parse_movie_detail(url): #搜索与用户相关的电影名字，看过的和想看的
    movie_title_all = []
    next, movie = parse_next_movie(url)  #判断是不是有下一页
    # print 'movie1',movie
    movie_title_all.extend(movie)
    if next:
        while next:
            for a in next:
                url = a.get('href')
                next, movie = parse_next_movie(url)
                movie_title_all.extend(movie)
    # print 'all',len(movie_title_all)
    return movie_title_all

def find_people_movie(url):  #用户喜欢的电影汇总入口
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    item = document_tree.xpath('//div[@id="movie"]/h2/span/a')
    movie_info = {}
    if item:
        for a in item:
            url = a.get('href')
            # print '--1--',url
            # print 'url3--',url
            w=re.compile('http://movie.douban.com/people/[^/]*/(?P<word>.*)')
            r = re.search(w,url)
            movie_level = r.group('word')
            # print '--movie_level--',movie_level
            movie_info[movie_level] = parse_movie_detail(url)
    # print 'movie_info',movie_info
    return movie_info


def parse_next_minisite(url): #先处理当前页信息，再判断是不是有下一页
    minisite = []
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    # item = document_tree.xpath('//div[@class="grid-view"]/div[@class="item"]/div[@class="pic"]/a')
    item_url = document_tree.xpath('//div[@class="photoin"]/a')
    item_title = document_tree.xpath('//div[@class="photoin"]/a/img')
    next = document_tree.xpath('//span[@class="next"]/a')
    if item_url and len(item_url) == len(item_title):
        for i in range(0,len(item_url)):
            # minisite['url'] = item_url[i].get('href')
            # minisite['title'] = item_title[i].get('title')
            minisite.append((item_url[i].get('href'),item_title[i].get('title')))
            # print 'title:', item_title[i].get('title')
    return next , minisite

def parse_minisite_detail(url):
    minisite_all = []
    next , minisite = parse_next_minisite(url)
    # minisite_all = dict(minisite_all.items() + minisite.items())
    minisite_all.extend(minisite)
    if next:
        while next:
             for a in next:
                url = a.get('href')
                # print 'url2--',url
                next, minisite = parse_next_minisite(url)
                # minisite_all = dict(minisite_all.items() + minisite.items())
                minisite_all.extend(minisite)
    #否则没下一页，不处理
    return minisite_all


def find_people_minisite(url):   #用户关注的小站
    # print 'url:',url
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    item = document_tree.xpath('//div[@id="minisite"]/h2/span/a')
    if item:
        a = item[0] #长为1的列表
        url0 = a.get('href')
        # print 'url1--',url0
        return parse_minisite_detail(url0)
    else:
        # print 'empty'
        return {}

def find_people_books(url):  #用户的书
    pass

def parse_next_online(url):
    online = []
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    # item = document_tree.xpath('//div[@class="grid-view"]/div[@class="item"]/div[@class="pic"]/a')
    item_url = document_tree.xpath('//div[@class="nof online_nof"]/h2/a')
    item_title = document_tree.xpath('//div[@class="nof online_nof"]/h2/a/text()')
    next = document_tree.xpath('//span[@class="next"]/a')
    if item_url and len(item_url) == len(item_title):
        for i in range(0,len(item_url)):
            url = item_url[i].get('href')
            title = item_title[i]
            online.append((url ,title))
            # print 'title:', title, url
        return next , online
    else:
        return [],[]
def parse_online_detail(url):
    online_all = []
    next , online = parse_next_online(url)
    online_all.extend(online)
    if next:
        while next:
             for a in next:
                url = a.get('href')
                # print '2',url
                url = 'http://www.douban.com' + url
                next, online = parse_next_online(url)
                online_all.extend(online)
    return online_all


def find_people_online(url):  #线上活动
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    item = document_tree.xpath('//div[@id="online"]/h2/span/a')
    if item:
        a = item[0]
        url = a.get('href')
        # print '1',url
        return parse_online_detail(url)
    else:
        return []

def parse_next_event(url):
    events = []
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    # item = document_tree.xpath('//div[@class="grid-view"]/div[@class="item"]/div[@class="pic"]/a')
    item = document_tree.xpath('//div[@class="info"]/div[@class="title"]/a')

    next = document_tree.xpath('//span[@class="next"]/a')
    if item:
        for a in item:
            url = a.get('href')
            title = a.get('title')
            event =(url, title)
            events.append(event)
    return next , events

def parse_event_detail(url):

    event_all = []
    events = {}
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    item = document_tree.xpath('//div[@class="tabs norm-tabs"]/a')  #活动也有分类的，已参加的 和 已过期的
    if item:
        for a in item:
            event_level_all = []
            level = a.get('data-index')  #想参加的和已参加的又分为两类
            url = a.get('href')
            next, event = parse_next_event(url)  #先处理当前页信息，再判断是不是有下一页
            # print 'movie1',movie
            event_level_all.extend(event)
            if next:
                while next:
                    for a in next:
                        url = a.get('href')
                        url = 'http://www.douban.com' + url
                        next, event = parse_next_event(url)
                        event_level_all.extend(event)
            events[level] = event_level_all
            event_all.append(events)
    return event_all

def find_people_event(url):  #同城活动
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    item = document_tree.xpath('//div[@id="event"]/h2/span/a')
    event_info = {}
    if item:
        for a in item:
            url = a.get('href')
            # print '--1--',url
            # print 'url3--',url
            w=re.compile('/people/.*/events/(?P<word>.*)')
            r = re.search(w,url)
            event_level = r.group('word')
            # print '--event_level--',event_level
            event_info[event_level] = parse_event_detail(url)
    # print 'movie_info',movie_info
    return event_info

def parse_friend_detail(url):  #用户关注的成员
    note_groups = []
    # print '--url--',url
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    group_into = document_tree.xpath('//dl[@class="obu "]/dd/a')
    # print '2--',len(group_into)
    for i in group_into:
        url_group = i.get('href')
        note_groups.append(find_group_UID(url_group))
    return note_groups

def find_people_friend(url):  #用户关注的成员
    # print 'id---:',id
    page = requests.get(url)
    document_tree = lxml.html.fromstring(page.content)
    # name_s = document_tree.xpath('//div[@class="user-info"]/div[@class="pl"]/text()')
    note_group = document_tree.xpath('//div[@id="friend"]/h2/span/a')  #参加的小组
    if note_group:
        # print '1',note_group[0]
        url0 = note_group[0].get('href')
        return parse_friend_detail(url0)

    else:
        return []
def parse_naxt():
    pass
def parse_detail():
    pass
def find_link(url,r1,r2):
    pass

def get_url_all(data_words, group_queue, data_db):  #给定关键字，搜索小组 ,得到小组成员， 存储小组信息
    # '''
    # url_people = []
    # people_all=[]
    group_queue.delete()
    url_base = 'http://www.douban.com/group/search?cat=1019&q='
    for d in data_words:
        url = url_base + d;
        find_group(url,group_queue)
    time.sleep(120)
    print 'group_queue.length()',group_queue.length()
    while group_queue.length():
        url = group_queue.get()
        print '小组url--',url
        time.sleep(60)
        print '存入一组用户信息：'
        find_url(url, data_db)
        # data_db.people_url_insert(find_url(url, data_db))
        # url_people.extend(find_url(url))
    # return url_people
def parse_people(url):
    people_info = {}
    UID = 'UID'     #用户UID
    ATTEND_GROUP = 'ATTEND_GROUP'  #用户常去的小组
    MOVIE = 'MOIVE'                 #用户的电影
    MINISITE ='MINISITE'            #用户关注的小站
    URL = 'URL'                     #用户的主页url
    EVENT = 'EVENT'                 #用户参加的同城活动
    ONLINE = 'ONLINE'               #用户的线上活动
    people_info[URL] = url
    people_info[UID] = find_people_UID(url)
    people_info[ATTEND_GROUP] = find_people_group(url)
    # print len(find_people_group(url))
    people_info[MOVIE] = find_people_movie(url)
    people_info[MINISITE] = find_people_minisite(url)
    people_info[EVENT] = find_people_event(url)
    people_info[ONLINE] = find_people_online(url)
    return people_info

def get_data(group_begin,group_queue,people_queue):  #给定关键字，搜索小组 ,得到小组成员， 存储小组信息
    # '''
    group_queue.delete()  #清空
    people_queue.delete()  #清空
    people_all=[]
    url_base = 'http://www.douban.com/group/search?cat=1019&q='
    for d in data_words:
        url = url_base + d;
        find_group(url,group_queue)
    print 'group_queue.length()',group_queue.length()
    while group_queue.length():
        url = group_queue.get()
        print '小组url--',url
        find_people_url(url, people_queue)


    print 'people_queue.length()',people_queue.length()


    i = 0
    while people_queue.length():
        i = i +1
        print '第%d个用户信息搜索ing：' % i
        people_info = {}
        # people_all = []
        url = people_queue.get()
        UID = 'UID'     #用户UID
        ATTEND_GROUP = 'ATTEND_GROUP'  #用户常去的小组
        MOVIE = 'MOIVE'                 #用户的电影
        MINISITE ='MINISITE'            #用户关注的小站
        URL = 'URL'                     #用户的主页url
        EVENT = 'EVENT'                 #用户参加的同城活动
        ONLINE = 'ONLINE'               #用户的线上活动
        people_info[URL] = url
        people_info[UID] = find_people_UID(url)
        people_info[ATTEND_GROUP] = find_people_group(url)
        # print len(find_people_group(url))
        people_info[MOVIE] = find_people_movie(url)
        people_info[MINISITE] = find_people_minisite(url)
        people_info[EVENT] = find_people_event(url)
        people_info[ONLINE] = find_people_online(url)
        # dbs.people_insert(people_info)
        # people_all.append(people_info)
    # '''
    print 'end group.length()',group_queue.length()
    print 'end people_queue.length()',people_queue.length()
    return people_all
    # rerutn people_info

def save_data():
    dbs = MongoCRUD()
    items = dbs.read_all_url()
    for item in items:
        print 'item:',item
        url = item['URL']
        _id = item['_id']
        print '更新数据：--'
        dbs.update_people_statue(_id)
        print '将爬到的用户信息，存入数据库：---ing'
        dbs.inset_one_people_info(parse_people(url))
        # peoples.append(parse_people(url))
        # time.sleep(60)

    print 'the information about all people has saved!'
    print '完成 退出'




# url = 'https://s3.amazonaws.com/codecave/tut1.html'
# url= 'http://www.douban.com/group/search?cat=1019&q=%E6%9C%88%E5%AB%82'
# data_words = ['妈妈','宝宝','怀孕','待产包','月子餐','月嫂']
if __name__ == '__main__':
    # pool = multiprocessing.Pool(processes=4)
    start = time.clock()
    dbs = MongoCRUD()
    data_words = ['妈妈','宝宝','怀孕','待产包','月子餐','月嫂','妈咪']
    data_words = ['妈妈']
    group_queue = RedisQueues('GROUP_QUEUE1')
    group_queue.delete()
    # all = get_url_all(data_words,group_queue)
    # get_url_all(data_words,group_queue,dbs)
    # print len(all)
    # print '豆瓣用户UID存入数据库'
    # dbs.people_url_insert(all)
    save_data()
    elapsed = (time.clock() - start)
    print("Time used:",elapsed)
    '''
    people_queue = RedisQueues('PEOPLE_QUEUE1')

    dbs = MongoCRUD()
    # get_data(data_words,group_queue,people_queue)
    people_all =get_data(data_words,group_queue,people_queue)
    print 'people_count--:',len(people_all)
    dbs.people_insert(people_all)

    # url = 'http://www.douban.com/people/xinjianghu/' #线上活动\同城活动
    # all = find_people_online(url)

    # print 'all', len(people_all)
'''
# '''
    # url = 'http://www.douban.com/people/48067352/' #测试UID和关注的群组
    # url = 'http://www.douban.com/people/77519858/' #测试电影


    # dbs.Index_create()
    # url_base = 'http://www.douban.com/group/20244/'
    # find_group_UID(url_base)
# '''


    #data_words = ['月嫂','妈咪','宝宝','待产包','怀孕','胎教','育儿','月子餐','婴儿','宝爸','宝妈','小宝贝','妊娠','孕妇','奶粉']

    # conn_db(db_name, 'people', people_info)

'''
    url = 'http://www.douban.com/people/tianyabushiguil/' #测试关注的小站
    # url = 'http://www.douban.com/people/77519858/'
    # find_people_group(url)
    i = 1
    UID = str(i) + 'UID'
    ATTEND_GROUP = 'ATTEND_GROUP' + str(i)
    MOVIE = str(i) + 'MOIVE'
    MINISITE = str(i) + 'MINISITE'

    people_info[UID] = find_people_UID(url)

    people_info[ATTEND_GROUP] = find_people_group(url)
    print len(find_people_group(url))
    people_info[MOVIE] = find_people_movie(url)
    people_info[MINISITE] = find_people_minisite(url)
    # print len(find_people_minisite(url))
    print people_info,'\n'
    # print 'json',json.dumps(people_info)
    people_all.append(people_info)
    print 'people_count--:',len(people_all)
    dbs = MongoCRUD()
    dbs.people_insert(people_all)
'''



# print  'sheet',sheet
