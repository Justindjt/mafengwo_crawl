import pymysql
import hashlib
from mysql import connector
from mysql.connector import errorcode


class CrawlDatabaseManager(object):
    MYSQL_NAME = 'mfw_crawl'
    MYSQL_IP = 'localhost'
    TABLES = {}
    TABLES['link'] = ("""CREATE TABLE `urls` (
    `index` INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `url` VARCHAR(512) NOT NULL,
    `md5` VARCHAR(512) NOT NULL UNIQUE KEY,
    `status` VARCHAR(512) NOT NULL DEFAULT 'new',
    `depth` int(11) NOT NULL,
    `queue_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `done_time` timestamp NOT NULL DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP)"""
    )

    def __init__(self, max_num_thread):
        try:
            self.max_num_thread = max_num_thread
            db_con = connector.connect(host=self.MYSQL_IP, user='root', charset='utf8', password='Deng.888')
        except connector.Error as error:
            if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something was wrong with your user name and password')
            elif error.errno == errorcode.ER_BAD_DB_ERROR:
                print('Database does not exist')
            else:
                print('Create Error {}'.format(error.msg))
            # exit(0) 无错误退出
            #  有错误退出
            exit(1)

        cursor = db_con.cursor()
        try:
            db_con.database = self.MYSQL_NAME
        except connector.Error as error:
            if error.errno == errorcode.ER_BAD_DB_ERROR:
                self.creat_database(cursor)
                db_con.database = self.MYSQL_NAME
                self.creat_table(cursor)
            else:
                print(error)
                exit(1)
        finally:
            cursor.close()
            db_con.close()

        self.dbconfig = {
            'database': self.MYSQL_NAME,
            'user': 'root',
            'host': self.MYSQL_IP,
            'password': 'Deng.888'}

    def creat_database(self, cursor):
        try:
            cursor.execute('CREATE DATABASE {} CHARSET=utf8'.format(self.MYSQL_NAME))

        except connector.Error as error:
            print('Fail create database: {}'.format(error))
            exit(1)
        else:
            print('The database create successfully')

    def creat_table(self, cursor):
        for name, link in self.TABLES.items():
            try:
                cursor.execute(link)
            except connector.Error as error:
                if error.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print('The table is already exist')
                else:
                    print('Failed to create tables: {}'.format(error.msg))
            else:
                print('Create table successfully')

    def enqueueurl(self, url, depth):
        conn = connector.connect(pool_name='mypool', pool_size=self.max_num_thread, **self.dbconfig)
        cursor = conn.cursor()
        try:
            add_url = ('INSERT INTO urls (`url`, `md5`, `depth`) VALUES (%s, %s, %s)')
            data_url = (url, hashlib.md5(url.encode('utf8')).hexdigest(), depth)
            cursor.execute(add_url, data_url)
            conn.commit()
        except connector.Error as error:
            print('enqueueurl() : {}'.format(error.msg))
            return
        finally:
            cursor.close()
            conn.close()

    def dequeueurl(self):
        conn = connector.connect(pool_name='mypool', pool_size=self.max_num_thread, **self.dbconfig)
        cursor = conn.cursor(dictionary=True)

        try:
            get_query = ("SELECT `index`,`url`,`depth` FROM `urls` WHERE status='new' ORDER BY `index` ASC LIMIT 1 FOR UPDATE")
            cursor.execute(get_query)
            row = cursor.fetchone()
            print(cursor.rowcount)
            if cursor.rowcount is -1:
                return None
            update_query = ("UPDATE urls SET `status`='downloading' WHERE `index`=%d" % (row['index']))
            cursor.execute(update_query)
            conn.commit()
            return row
        except connector.Error as error:
            print('dequeueurl() : {}'.format(error.msg))
            return None
        finally:
            cursor.close()
            conn.close()

    def finishurl(self, index):
        conn = connector.connect(pool_name='mypool', pool_size=self.max_num_thread, **self.dbconfig)
        cursor = conn.cursor()

        try:
            update_query = ("UPDATE urls SET `status`='done' WHERE `index`=%d" % (index))
            cursor.execute(update_query)
            conn.commit()
        except connector.Error as error:
            print('finishurl() : {}'.format(error.msg))
            return
        finally:
            cursor.close()
            conn.close()
# 关于开始与结束的时间，它会自己更新，不需要语句去更新


# def main():
#     CrawlDatabaseManager(5)
#
#
# if __name__ == '__main__':
#     main()