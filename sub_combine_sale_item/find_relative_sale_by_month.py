# -*- coding: utf-8 -*-
import pandas as pd
import itertools
import time
import sql
import logging

log = None
MAX_ITEM = 7

BEGIN_USER_NO = 11968


def log_init(file_name):
    """
    logging.debug('This is debug message')
    logging.info('This is info message')
    logging.warning('This is warning message')
    """
    level = logging.DEBUG
    logging.basicConfig(level=level,
                        format='%(asctime)s [line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=file_name,
                        filemode='w')
    return logging


def get_time(flage='begin', begin=None, id_time=None):
    if flage == 'begin':
        return time.clock()
    else:
        if begin != None:
            end = time.clock()
            return "%s use: %f s" % (id_time, (end - begin))
        else:
            return 'without begin'


def input_combine_product_data():
    # 整理数据
    conn = sql.init_sql_253()
    sql_text = 'SELECT skuCode SkuCode, subSkuCode ItemCode, id from T_Kingdee_Combine'
    df = pd.io.sql.read_sql(sql_text, con=conn)
    df = df.sort_values('ItemCode', ascending=True)
    return df


def input_sales_data(filename):
    # 整理数据
    df = pd.read_excel(filename)
    return df


# 从数据库里面取数据
def input_sales_data_from_db(begin_date):
    # 整理数据
    conn = sql.init_sql_253()
    sql_text = """
    select distinct SkuCode,BuyUser,SUM(Qty) Qty,year(OrderAduitTime) AduitYear
    from ProductDataHandle..T_Kingdee_SaleDataD
    where convert(nvarchar(7), OrderAduitTime,120) ='{begin_date}'
    and SkuCode not in (select SkuCode from  BSPRODUCTCENTER..T_PRT_AllProduct where Series in('赠品系列','配套系列'))
    group by SkuCode,BuyUser,year(OrderAduitTime)
    """.format(begin_date=begin_date)
    df = pd.io.sql.read_sql(sql_text, con=conn)
    return df


def find_product_combine_items(df_data, item_code):
    res = df_data[df_data['ItemCode'] == item_code]
    if len(res) > 1:
        combine_item_list = {}
        sku_code_list = []
        for i in range(0, len(res)):
            res_item = df_data[df_data['SkuCode'] == res.iloc[i]['SkuCode']]
            for i_item in range(0, len(res_item)):
                if res_item.iloc[i_item]['SkuCode'] not in sku_code_list:
                    sku_code_list.append(res_item.iloc[i_item]['SkuCode'])
                    combine_item_list[res_item.iloc[i_item]['SkuCode']] = [res_item.iloc[i_item]['ItemCode']]
                else:
                    combine_item_list[res_item.iloc[i_item]['SkuCode']].append(res_item.iloc[i_item]['ItemCode'])
        return combine_item_list
    else:
        return None


def find_mix_qty(qty_dic, item_list):
    min_qty = 1000
    for item_code in item_list:
        if min_qty > qty_dic[item_code]:
            min_qty = qty_dic[item_code]
    return min_qty


def main_function(begin_date):
    global df_combine_product
    # 数据库连接初始化
    engine, connection, table_schema = sql.init_connection('T_DCR_CombineSaleData')
    # sale data
    df_sales = input_sales_data_from_db(begin_date)
    user_buy_items_sum = df_sales.groupby('BuyUser').size()
    # 提出每个用户的购买列表
    index_user = 0
    # 制作组合列表
    combine_table = []
    log.info('there are %d sale records and %d buyer' % (len(df_sales), len(user_buy_items_sum)))
    for num_items in user_buy_items_sum:
        if index_user < BEGIN_USER_NO:
            index_user += 1
            continue
        user = user_buy_items_sum.index[index_user]
        index_user += 1
        items = df_sales[df_sales['BuyUser'] == user]

        if num_items > 1:
            # 存储产品对应的购买数量
            skuCode_qty_dic = {}

            for index_item in range(0, len(items)):
                skuCode_qty_dic[items.iloc[index_item]['SkuCode']] = int(items.iloc[index_item]['Qty'])
            # 生成用户商品所有组合
            if len(items) > MAX_ITEM:
                max_len = MAX_ITEM
            else:
                max_len = len(items)

            combine_list = []
            for i in range(1, max_len):
                tmp_list = list(itertools.combinations(items['SkuCode'], i + 1))
                combine_list += tmp_list

            free_combine_items = {}
            for item_id in range(0, num_items - 1):
                # 找出已有的产品的组合
                tmp_res = find_product_combine_items(df_combine_product, items.iloc[item_id]['SkuCode'])
                if tmp_res is not None:
                    free_combine_items.update(tmp_res)

            # 开始比较产品组合和自由组合
            for index_clist in range(0, len(combine_list)):
                clist = combine_list[index_clist]
                in_product_list = False
                for key, tmp_list in free_combine_items.items():
                    # 必须商品数量和内容一致
                    if len(tmp_list) == len(clist):
                        is_same = True
                        for sub_item in clist:
                            if sub_item not in tmp_list:
                                is_same = False
                                break
                        # 如果有相同的就不做后续处理
                        if is_same:
                            in_product_list = True
                            break

                if not in_product_list:
                    combine_code = ''
                    # 制作组合标识编码
                    for sub_item in clist:
                        combine_code += sub_item + ':'

                    combine_table.append({
                        'CombineCode': combine_code[:-1],
                        'BuyUserQty': 1,
                        'SalesQty': find_mix_qty(skuCode_qty_dic, clist)
                    })

            log.info('finish NO.%d buyer and continue...' % index_user)
            # 每完成一个用户判断一次，超过1000条数据，就更新一次数据库
            if len(combine_table) > 1000:
                # 合并到数据库
                log.info('output 1000 rows to T_DCR_CombineSaleData')
                try:
                    sql.insert_data(engine, connection, table_schema, combine_table, log)
                    # 清空列表
                    combine_table = []
                except Exception as e:
                    log.error('-----error---------')
                    log.error(e)
                    log.info('continue .....')
                # 记录时间

    # 合并到数据库
    if len(combine_table) > 0:
        sql.insert_data(engine, connection, table_schema, combine_table, log)


if __name__ == "__main__":
    # 每个月的第一天计算前一个月的潜在销售组合
    x = time.localtime(time.time())
    if x.tm_mon-1 < 10:
        month = '0'+str(x.tm_mon-3)
    else:
        month = str(x.tm_mon-1)
    date_month = '{year}-{month}'.format(year=x.tm_year, month=month)

    log = log_init('log/%s_data.log' % date_month)
    # combine product
    log.info('get the combine product data')
    df_combine_product = input_combine_product_data()

    log.info('get the sale data at %s' % date_month)
    main_function(date_month)
    # 最后把表中的skucode分离出来，做成一张表
    log.info('finish find the subcombine sale item and save the detail table')
    # sql.make_combine_sale_skucode_detail(log)
    # TODO: 最后把表中的skucode分离出来，做成一张表, run every time
    # log.info('finish find the subcombine sale item and save the detail table')

    log.info('-----------finish all, well done !---------------------')

