# -*- coding: utf-8 -*-
import pymssql
import settings
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, bindparam
import pandas as pd
from pymongo import MongoClient

"""
use pymssql connect to the sql server
"""
MIN_SALE = 2


def init_mongo_sql():
    conn = MongoClient(settings.MONGO_HOST, 27017)
    return conn


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


class Mssql_252:
    def __init__(self):
        self.host = settings.HOST_252
        self.user = settings.HOST_252_USER
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


def init_sql():
    conn = pymssql.connect(
        host=settings.HOST,
        user=settings.HOST_USER,
        password=settings.HOST_PASSWORD,
        database=settings.DB,
    )
    return conn


def init_sql_253():
    conn = pymssql.connect(
        host=settings.HOST_252,
        user=settings.HOST_252_USER,
        password=settings.HOST_PASSWORD,
        database=settings.DB_Product,
    )
    return conn


def select_date(sql_text):
    conn = Mssql()
    return conn.exec_query(sql_text)


def logo_item_to_sql(data):
    conn = Mssql()
    insertSql = "insert into T_Data_Logo(" \
                "CategoryId, PosId, PosName, LogoName, SalesCnt," \
                "SalesCntTop20Percent,SalesCntPercent,SalesMoney,SalesMoneyTop20Percent," \
                "SalesMoneyPercent, RecordType, RecordDate)" \
                "values ('%s','%s','%s','%s','%d','%s','%s','%d','%s','%s','%d','%s')" %\
                (data['categoryId'].encode('utf8'),
                 data['PosId'].encode('utf8'),
                 data['PosName'].encode('utf8'),
                 data['LogoName'].encode('utf8'),
                 data['SalesCnt'],
                 data['SalesCntTop20Percent'].encode('utf8'),
                 data['SalesCntPercent'].encode('utf8'),
                 data['SalesMoney'],
                 data['SalesMoneyTop20Percent'].encode('utf8'),
                 data['SalesMoneyPercent'].encode('utf8'),
                 data['RecordType'],
                 data['RecordDate'])
    conn.exec_non_query(insertSql)


def combine_item_to_sql(data):
    conn = Mssql()
    inserSql = "insert into T_DCR_FreeCombine (skuCode,subSkuCode, bomVersion, combineType) values " \
               "('%s', '%s', 'ver2017', 'FreeCombine')"%(
        data['sku_code'], data['item_code'])
    conn.exec_non_query(inserSql)


def test_combine_item_to_sql(data):
    conn = Mssql(db=settings.DB_Test)
    inserSql = "insert into T_Free_combine (SkuCode,ItemCode) values " \
               "('%s', '%s')"%(data['sku_code'], data['item_code'])
    conn.exec_non_query(inserSql)


def add_count_free_combine(sku_code, qty=1):
    conn = Mssql()
    sql_text = "SELECT qty, qtyBuyUser FROM T_DCR_CountFreeCombine WHERE skuCode='%s'" % sku_code
    res = conn.exec_query(sql_text)
    if len(res) > 0:
        qty_item = res[0][0] + qty
        qty_buy_user = res[0][1] + 1
        sql_text = "UPDATE T_DCR_CountFreeCombine set qty = '%d', qtyBuyUser = '%d' WHERE skuCode='%s'" % \
                   (qty_item, qty_buy_user, sku_code)
        conn.exec_non_query(sql_text)
    else:
        sql_text = "insert into T_DCR_CountFreeCombine (qty,qtyBuyUser,skuCode) values ('%d','%d','%s')" % \
                   (qty, 1, sku_code)
        conn.exec_non_query(sql_text)


def test_add_count_free_combine(sku_code, qty=1):
    conn = Mssql(db=settings.DB_Test)
    sql_text = "SELECT qty, qtyBuyUser FROM Count_Free_combine WHERE skuCode='%s'" % sku_code
    res = conn.exec_query(sql_text)
    if len(res) > 0:
        qty_item = res[0][0] + qty
        qty_buy_user = res[0][1] + 1
        sql_text = "UPDATE Count_Free_combine set qty = '%d', qtyBuyUser = '%d' WHERE skuCode='%s'" % \
                   (qty_item, qty_buy_user, sku_code)
        conn.exec_non_query(sql_text)
    else:
        sql_text = "insert into Count_Free_combine (qty,qtyBuyUser,skuCode) values ('%d','%d','%s')" % \
                   (qty, 1, sku_code)
        conn.exec_non_query(sql_text)


def find_free_combine_item(item_id):
    conn = Mssql()
    selectSql = "SELECT skuCode,subSkuCode FROM T_DCR_FreeCombine where skuCode in " \
                "(SELECT skuCode FROM T_DCR_FreeCombine WHERE subSkuCode='%s')" % item_id
    res_list = conn.exec_query(selectSql)
    if len(res_list) > 0:
        combine_item_list = {}
        sku_code_list = []
        for sku_code, item in res_list:
            if sku_code not in sku_code_list:
                sku_code_list.append(sku_code)
                combine_item_list[sku_code] = [item]
            else:
                combine_item_list[sku_code].append(item)
        return combine_item_list
    return None


def test_find_free_combine_item(item_id):
    conn = Mssql(db=settings.DB_Test)
    selectSql = "SELECT SkuCode,ItemCode FROM T_Free_combine where SkuCode in " \
                "(SELECT SkuCode FROM T_Free_combine WHERE ItemCode='%s')" % item_id
    res_list = conn.exec_query(selectSql)
    if len(res_list) > 0:
        combine_item_list = {}
        sku_code_list = []
        for sku_code, item in res_list:
            if sku_code not in sku_code_list:
                sku_code_list.append(sku_code)
                combine_item_list[sku_code] = [item]
            else:
                combine_item_list[sku_code].append(item)
        return combine_item_list
    return None


def get_sale_data_by_user(user_name):
    conn = Mssql_252()
    selectSql = "SELECT SkuCode FROM T_kingdee_SaleDataD where BuyUser='%s'" % user_name
    res_list = conn.exec_query(selectSql)
    sku_code_list = []
    if res_list > 0:
        for sku_code in res_list:
            sku_code_list.append(sku_code[0])
        return  sku_code_list
    else:
        return None


def add_free_combine(combine_table):
    conn = Mssql()
    sql_text = "insert into T_DCR_CombineSaleData values (%s,%d,%d)"
    conn.exec_many_query(sql_text, combine_table)
    # insert_data('T_DCR_CombineSaleData', combine_table, log)


def add_combine_sale_skucode_detail(combine_table):
    conn = Mssql()
    sql_text = "insert into T_DCR_CombineSaleSkuCodeDetail values (%s,%d,%d,%s,%s)"
    conn.exec_many_query(sql_text, combine_table)


def make_combine_sale_skucode_detail(log):
    conn = init_sql()
    sql_text = "SELECT CombineCode,sum(SalesQty) SalesQty,sum(BuyUserQty) BuyUserQty FROM dbo.T_DCR_CombineSaleData (nolock)" \
               " group by CombineCode having(sum(BuyUserQty)>20)"
    log.info('In the Data Processing , please wait 1 min ....')
    df = pd.io.sql.read_sql(sql_text, con=conn)

    log.info('make the T_DCR_CombineSaleSkuCodeDetail table')

    combine_sale_skucode_table = []
    combine_no = 1
    for i in range(1, len(df)):
        skucode_list = df['CombineCode'][i].split(':')

        if len(skucode_list) > 1:
            for skucode in skucode_list:
                # format : CombineCode, SalesQty, BuyUserQty, SkuCode
                combine_sale_skucode_table.append((
                    df['CombineCode'][i],
                    int(df['SalesQty'][i]),
                    int(df['BuyUserQty'][i]),
                    skucode,
                    'QZZH%d' % combine_no
                ))

            # combine_no auto add 1
            combine_no += 1
        # when the length of table larger than 1000, it output to the sql
        if len(combine_sale_skucode_table) > 1000:
            log.info('output 1000 rows to T_DCR_CombineSaleSkuCodeDetail')
            add_combine_sale_skucode_detail(combine_sale_skucode_table)
            combine_sale_skucode_table = []

    if len(combine_sale_skucode_table) > 0:
        add_combine_sale_skucode_detail(combine_sale_skucode_table)


def init_connection(table):
    # 'mysql://uid:pwd@localhost/mydb?charset=utf8'
    engine = create_engine('mssql+pymssql://%s:%s@%s/%s?charset=utf8' % (
        settings.HOST_USER,
        settings.HOST_PASSWORD,
        settings.HOST,
        settings.DB
    ))
    # 这个几个要做全局变量，如果不断创建会形成内存泄漏
    connection = engine.connect()
    metadata = sqlalchemy.schema.MetaData(bind=engine, reflect=True)
    table_schema = sqlalchemy.Table(table, metadata, autoload=True)
    Session = sessionmaker(bind=engine)
    return connection, table_schema, Session


def insert_data(connection, table_schema, Session, insert_list, log):
    log.info('Saving the data.....')
    # 创建Session:
    session = Session()

    try:
        connection.execute(table_schema.insert(), insert_list)
        session.commit()
    except Exception as e:
        log.error('Having error during the saving....')
        log.error(e)
    finally:
        session.close()


def insert_data_mongo(data, log):
    log.info('Saving the data.....')
    conn = init_mongo_sql()
    sub_combine_sale = conn.DCR.sub_combine_sale
    try:
        sub_combine_sale.insert(data)
    except Exception as e:
        log.error('Having error during the saving....')
        log.error(e)
    finally:
        conn.close()


def make_combine_sale_detail_mongo(log):
    log.info('loading the data.....')
    conn = init_mongo_sql()
    sub_combine_sale = conn.DCR.sub_combine_sale
    cursor = sub_combine_sale.aggregate(
        [
            {'$group': {
                '_id': "$CombineCode",
                'SalesQty': {"$sum": "$SalesQty"},
                'BuyUserQty': {"$sum": "$BuyUserQty"}
            }},
            {'$match': {'SalesQty': {'$gt': MIN_SALE}}}
        ]
    )
    log.info('make the T_DCR_CombineSaleSkuCodeDetail table')
    combine_sale_skucode_table = []
    combine_no = 1
    for data in cursor:
        skucode_list = data['_id'].split(':')

        if len(skucode_list) > 1:
            for skucode in skucode_list:
                # format : CombineCode, SalesQty, BuyUserQty, SkuCode
                combine_sale_skucode_table.append((
                    data['_id'],
                    data['SalesQty'],
                    data['BuyUserQty'],
                    skucode,
                    'QZZH%d' % combine_no
                ))

            # combine_no auto add 1
            combine_no += 1
        # when the length of table larger than 1000, it output to the sql
        if len(combine_sale_skucode_table) > 1000:
            log.info('output 1000 rows to T_DCR_CombineSaleSkuCodeDetail')
            add_combine_sale_skucode_detail(combine_sale_skucode_table)
            combine_sale_skucode_table = []

    print combine_sale_skucode_table
    exit(5)
    if len(combine_sale_skucode_table) > 0:
        add_combine_sale_skucode_detail(combine_sale_skucode_table)


if __name__ == "__main__":
    print 'begin'
    #make_combine_sale_detail_mongo('xx')
    
    print 'End'

