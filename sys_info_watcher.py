#!/usr/bin/env python
# coding=utf8
import logging
import psutil
from datetime import datetime as dt


def log_init(file_name):
    """
    logging.debug('This is debug message')
    logging.info('This is info message')
    logging.warning('This is warning message')
    """
    level = logging.DEBUG
    logging.basicConfig(level=level,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=file_name,
                        filemode='a')
    return logging

if __name__ == '__main__':

    # 设置一个日志输出文件
    today = dt.today().strftime('%Y-%m-%d')
    log = log_init('log/%s.txt' % today)

    # 获取当前运行的pid
    # p1 = psutil.Process(os.getpid())

    # 打印本机的内存信息
    # print ('直接打印内存占用： ' + (str)(psutil.virtual_memory))

    # 打印内存的占用率
    log.info('获取内存占用率： ' + str(psutil.virtual_memory().percent) + '%')

    # 本机cpu的总占用率
    log.info('打印本机cpu占用率： ' + str(psutil.cpu_percent(0)) + '%')

    # 该进程所占cpu的使用率
    # print (" 打印该进程CPU占用率: " + (str)(p1.cpu_percent(None)) + "%")

    # 直接打印进程所占内存占用率
    # print (p1.memory_percent)

    # 格式化后显示的进程内存占用率
    # print "percent: %.2f%%" % (p1.memory_percent())