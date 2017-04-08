# -*- coding: utf-8 -*-
import pymssql
import settings


"""
use pymssql connect to the sql server
"""


class Mssql:
    def __init__(self, db=settings.DB):
        self.host = settings.HOST
        self.user = settings.HOST_USER
        self.pwd = settings.HOST_PASSWORD
        self.db = db

    def __get_connect(self):
        if not self.db:
            raise (NameError, "do not have db information")
        self.conn = pymssql.connect(
            host=self.host,
            user=self.user,
            password=self.pwd,
            database=self.db,
            charset="utf8"
        )
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "Have some Error")
        else:
            return cur

    def exec_query(self, sql):
        """
         the query will return the list, example;
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__get_connect()
        cur.execute(sql)
        res_list = cur.fetchall()

        # the db object must be closed
        self.conn.close()
        return res_list

    def exec_non_query(self, sql):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

    def exec_many_query(self, sql, param):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()
        try:
            cur.executemany(sql, param)
            self.conn.commit()
        except Exception as e:
            print e
            self.conn.rollback()

        self.conn.close()


class Mssql_253:
    def __init__(self):
        self.host = settings.HOST_253
        self.user = settings.HOST_253_USER
        self.pwd = settings.HOST_PASSWORD
        self.db = settings.DB_Product

    def __get_connect(self):
        if not self.db:
            raise (NameError, "do not have db information")
        self.conn = pymssql.connect(
            host=self.host,
            user=self.user,
            password=self.pwd,
            database=self.db,
            charset="utf8"
        )
        # self.conn.autocommit(False)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "Have some Error")
        else:
            return cur

    def exec_query(self, sql):
        """
         the query will return the list, example;
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__get_connect()
        cur.execute(sql)
        res_list = cur.fetchall()

        # the db object must be closed
        self.conn.close()
        return res_list

    def exec_non_query(self, sql):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

    def exec_many_query(self, sql, param):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()
        try:
            cur.executemany(sql, param)
            self.conn.commit()
        except Exception as e:
            print e
            self.conn.rollback()

        self.conn.close()


def init_sql():
    conn = pymssql.connect(
        host=settings.HOST,
        user=settings.HOST_253_USER,
        password=settings.HOST_PASSWORD,
        database=settings.DB_Product,
    )
    return conn


def select_date(sql_text):
    conn = Mssql()
    return conn.exec_query(sql_text)


def get_order_data(min_id, max_id):
    conn = Mssql_253()
    sql_text = "select * from T_ERP_TaoBao_OrderHistory with(nolock) where id>='%d' and id<'%d'" % (min_id, max_id)
    return conn.exec_query(sql_text)


def get_recent_order_data(begin_date, end_date):
    conn = Mssql_253()
    sql_text = """
    select a.Tid,a.BuyerNick,a.Created,b.Num,a.Status,b.OuterSkuId from V_PRT_TaoBao_Trade_2  (nolock)  a
    join V_PRT_TaoBao_Order (nolock) b on a.Tid=b.Tid
    where  a.Status='WAIT_SELLER_SEND_GOODS' and
    Created>='{0}' and Created<= '{1}' and b.OuterSkuId is not null
    """.format(begin_date, end_date)
    return conn.exec_query(sql_text)


def find_buyer_nick(tid):
    conn = Mssql_253()
    sql_text = "SELECT TOP 1 BuyerNick From T_ERP_TaoBao_TradeHistory with(nolock) WHERE Tid='%s'" % tid
    res = conn.exec_query(sql_text)
    if len(res) > 0:
        return res[0][0]
    else:
        return None


def input_order_data(data):
    conn = Mssql_253()
    insertSql = "insert into T_ERP_TaoBao_Order_CombineSubSku values(%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    conn.exec_many_query(insertSql, data)


def input_tmp_order_data(data):
    conn = Mssql_253()

    insertSql = "insert into T_TaoBao_OrderHistoryTemp values(%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    conn.exec_many_query(insertSql, data)


def get_bulk_insert_script(csv_file):
    conn = Mssql_253()

    script = """BULK INSERT T_TaoBao_OrderHistoryTemp
            FROM '{0}'
            WITH
            (
                BATCHSIZE=10000,
                FIELDTERMINATOR=',',
                ROWTERMINATOR ='\\n'
            )""".format(csv_file)
    print script
    conn.exec_non_query(script)


def init_order_temp_table(table_name):
    conn = Mssql_253()
    script = """IF EXISTS (select * from sysobjects where name='{0}')drop TABLE {0}
    SELECT * INTO {0} FROM T_ERP_TaoBao_Order_CombineSubSku WHERE 1<>1
    """.format(table_name)
    conn.exec_non_query(script)


def merge_order_table():
    conn = Mssql_253()
    script = "exec P_ERP_SynscTaoBaoOrderCombineSkuCode"
    conn.exec_non_query(script)

if __name__ == "__main__":
    print 'begin'
    #data = {'RecordType': 1, 'CategoryURL': u'http://item.taobao.com/item.htm?id=538834474818', 'CategoryId': '1121', 'CategoryName': u'\u987e\u5bb6\u5bb6\u5c45\u65d7\u8230\u5e97', 'RecordDate': "20170101"}
    #make_hot_sale_plan(data)
    #data = {'sku_code': '1121', 'item_code': "201701"}
    #update_hot_shop_plan(data)
    #res = combine_item_to_sql(data)
    #print get_sale_data_by_user('yoyo')
    #combine_item_to_T_Kingdee_Combine(data)
    print 'End'

