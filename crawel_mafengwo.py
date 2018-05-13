"""
作者：Justin
功能：1、爬取蚂蜂窝主页下的所有子链接
     2、把获取到的子链接存入队列和MySQL中，待需要用时再取出
     3、继续爬取子链接下的子链接，一共爬取10层
     4、网址需要去重
     5、网址都存到MySQL中，以便日后分析或提取
"""

import requests
import threading
import time
import random
import json
from lxml import etree
from pybloom_live import ScalableBloomFilter
from fake_useragent import UserAgent
from mafengwo.dbmysql import CrawlDatabaseManager


headers = {
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'www.mafengwo.cn',
            'Upgrade-Insecure-Requests': '1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}
user_agent_fake = UserAgent()
headers['User-Agent'] = user_agent_fake.random
# 下载的文件路径
dir_name = './dir_process/'
# 设置线程数
max_num_thread = 5
# 建立MySQL的实例
dbmanager = CrawlDatabaseManager(max_num_thread)


def get_page_content(url, index, depth):
    """
    获取子链接
    """
    print('Downloading {} at level {}'.format(url, depth))
    try:
        response = requests.get(url, headers=headers)
        html_page = response.text.encode('utf-8')
        filename = url[7:].replace('/', '_')
        file = open('{}{}.html'.format(dir_name, filename), 'wb+')
        file.write(html_page)
        file.close()
        dbmanager.finishurl(index)
    except requests.ConnectionError as error:
        print('HttpError : {}'.format(error))
    except IOError as error:
        print('IOError : {}'.format(error))
    except Exception as error:
        print('Exception : {}'.format(error))
        return

    html = etree.HTML(html_page.lower().decode('utf-8'))
    hrefs = html.xpath('//a')

    for href in hrefs:
        try:
            if 'href' in href.attrib:
                next_url = href.attrib['href']
                if next_url.find('javascript') != -1:
                    continue
                if next_url.startswith('http://') is False:
                    if next_url.startswith('/'):
                        next_url = 'http://www.mafengwo.cn{}'.format(next_url)
                    else:
                        continue
                if next_url[-1] == '/':
                    next_url = next_url[0:-1]
                dbmanager.enqueueurl(next_url, depth+1)

        except Exception:
            continue


def main():
    """
    主函数
    """
    url = 'http://www.mafengwo.cn/'
    depth = 0
    dbmanager.enqueueurl(url, depth)
    start_time = time.time()
    # 标记第一次爬取
    root_page = True
    threads = []

    while True:
        crawlUrl = dbmanager.dequeueurl()
        print('{} dequeue'.format(crawlUrl))

        if crawlUrl is None:
            print('No url in queue')
            for thread_process in threads:
                thread_process.join()
            break

        if root_page is True:
            get_page_content(crawlUrl['url'], crawlUrl['index'], crawlUrl['depth'])
            root_page = False
        else:
            while True:
                for thread_process in threads:
                    if not thread_process.is_alive():
                        threads.remove(thread_process)
                if len(threads) >= max_num_thread:
                    time.sleep(0.6)
                    continue
                try:
                    thread_process = threading.Thread(target=get_page_content, name=None,
                                               args=(crawlUrl['url'], crawlUrl['index'], crawlUrl['depth']))

                    threads.append(thread_process)
                    thread_process.setDaemon(True)
                    thread_process.start()
                    delay_time = random.randint(1, 3)
                    time.sleep(delay_time)
                    break

                except Exception:
                    print('Error unable to start thread')
    # print(response.status_code)
    # print(response.text)


if __name__ == "__main__":
    main()
