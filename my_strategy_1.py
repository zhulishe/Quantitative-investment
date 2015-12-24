#coding=utf-8
__author__ = 'lisa'
import math
from datetime import datetime,date
from sklearn import linear_model  # for linear regression
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

import MySQLdb
#
# class Commission(object):
#     def __init__(self,buycost=0.001,sellcost=0.002,unit='perValue'):
#         self.buycost=buycost
#         self.sellcost=sellcost
#         self.unit=unit


conn1=MySQLdb.connect(host="192.168.0.105", user="root", passwd="fit123456", db="new_stock", charset="utf8")
conn2= MySQLdb.connect(host="192.168.0.105", user="root", passwd="fit123456", db="GARP", charset="utf8")
# # conn=MySQLdb.connect(host="host",user="root",port=16941,passwd="a8674925",db="new_stock",charset="utf8")
# cursor = conn.cursor()
# sql = ""


start = date(2007, 3, 1)  # 回测起始时间
end = date(2007, 8, 31)  # 回测结束时间

benchmark = '000300.SH'	 # 策略参考标准

# universe = ['000002.sz','000001.sz','000004.sz']	# 股票池
capital_base = 1e7     # 起始资金
short_capital = 1e7  # 做空资金
# commission = Commission(0.001,0.001)
refresh_rate = 5  # 调用频率
freq = 'd'  # 时间单位

# window_short = 20
# window_long = 120
# longest_history = window_long
# SD = 0.05

universe_code = '000300.SH'
self_defined = False
# universe_code = ['000002.SZ','000001.SZ','000004.SZ']  # 股票池
# self_defined = True
'''
000002.SH
上证A指

000300.SH
沪深300

000010.SH
上证180

000016.SH
上证50

000903.SH
中证100

399330.SZ
深证100

399312.SH
国证300

399001.SZ
深证成指

A
全部A股

000905.SH
中证500

000906.SH
中证800

000852.SH
中证1000
'''
# sql1 = "select * from new_stock_index WHERE group_code = '%s'and date_time BETWEEN '%s' and '%s' "%(universe_code,start,end)
# universe_temp = pd.read_sql(sql1,conn)
# universe_set = set([])
# for item in universe_temp['content']:
#     universe_set.update(item.split(','))
# print len(universe_set)
# all_universe = list(universe_set)

latest_universe = []
SMB = []  # 储存六个月的SMB值
HML = []
Rmt_Rf = []
Rit = []
Rf = 0.04
year = '2007'
myMonth = 3
month_count = 6

def initialize(account):					# 初始化虚拟账户状态
    # account.fund = universe[0]
    account.SD = 0.1
    account.window_short = 20
    account.window_long = 120
    account.f1 = 0.1
    account.f2 = 0.2
    account.f3 = 0.3
    # account.hangup=[]


def stop_loss(account):
    for i in account.valid_secpos.index[account.valid_secpos > 0]:
        todayHigh = account.getTodayHigh(i)
        todayLow= account.getTodayLow(i)
        todayOpen = account.getTodayOpen(i)
        stopprice = account.avgBuyprice[i]*(1-account.f1)
        upprice = account.avgBuyprice[i]*(1+account.f2)
        old_buy = account.buyPosition[i]
        if old_buy < account.days_counts:
            HighSinceBuy = account.getDailyHistory(i,account.days_counts-old_buy)['high'].max()
            if stopprice >= todayLow:
                if account.isOk2order(i):
                    #print stopprice,todayHigh,todayLow
                    if todayOpen >= stopprice:
                        if account.not_enough_cash(i, 0, stopprice):
                            print ' info, deal stocks to 0: stop deal at %s, now break' %i
                            break
                        account.order_to(i, 0, stopprice)
                    else:
                        if account.not_enough_cash(i, 0, todayOpen):
                            print ' info, deal stocks to 0: stop deal at %s, now break' %i
                            break
                        account.order_to(i, 0, todayOpen)
            #注释掉止盈的代码，f2和f3需要修改
            #if HighSinceBuy>=upprice and todayLow<HighSinceBuy*(1-account.f3):
            #    if account.isOk2order(i):
            #        account.order_to(i,0,stopprice)


def handle_data(account):
    global myMonth
    if account.days_counts == 1:
        return
    # 每月处理一次，改这里可以自定义区间
    if account.current_time.month != account.last_time.month:#　每月第一天
        account.real_deal_days += 1
        print '>>>', account.current_time, 'handle data'
        # 做空股指期货
        account.EmptyIF(account.hold_IF)
        account.EntryIF(account.short_cash)
        if self_defined:
            latest_universe = universe_code
        else:
            latest_universe = account.get_latest_universe()
        #得到的上个月的在沪深300中,但是这个月不在沪深300中的股票列表
        #然后把股票卖出去
        to_be_selled = set(account.all_universe).difference(set(latest_universe))
        for stock_id in to_be_selled:
            if account.isOk2order(stock_id):
                today_open = account.getTodayOpen(stock_id)
                #把所有在account.all_universe中但是不在latest_universe中的股票一一卖掉
                if account.not_enough_cash(stock_id, 0, today_open):
                    print ' info, deal stocks to 0: stop deal at %s, now break' %stock_id
                    break
                account.order_to(stock_id, 0, today_open)
        #处理股票
        process_stocks(account, latest_universe)
        myMonth += 1
    #止损, 每天都运行
    stop_loss(account)


def process_stocks(account, latest_universe):
    """
    # 说明,这是一个简单的测试策略,用户可以根据需求自定义策略
    # 1. 对新的股票列表进行筛选,得到这个月第一个交易日的开盘价大于上
    # 个月最后一天的收盘价, 这样就得到了一个待下单的股票列表
    # 2. 对下单的股票列表的每个元素分别计算要下单的手数


    #对新的股票列表进行筛选,得到这个月第一个交易日的开盘价大于上
    #个月最后一天的收盘价, 这样就得到了一个待下单的股票列表
    reselected_stocks = []
    for stock_id in latest_universe:
        if account.isOk2order(stock_id):
            #比较这个月第一个交易日的开盘价是否大于上个月最后一个交易日的收盘价
            if account.getTodayOpen(stock_id) > account.getDailyHistory(stock_id, 1)['close'].values[-1]:
                #把符合开盘价大于收盘价的股票放入一个新的列表
                reselected_stocks.append(stock_id)
    #卖掉account.valid_secpos中没有出现在reselected_stocks中的股票
    to_be_selled = set(account.valid_secpos.index.tolist()).difference(set(reselected_stocks))
    if account.real_deal_days != 1:
        if len(set(reselected_stocks)) != 0:
            account.lot_changed_rate[account.current_time] = \
                len(set(account.valid_secpos[account.valid_secpos>0].index.tolist())&set(reselected_stocks))/float(len(set(reselected_stocks)))
        else:
            account.lot_changed_rate[account.current_time] = 1
    for stock_id in to_be_selled:
        if account.isOk2order(stock_id):
            today_open = account.getTodayOpen(stock_id)
            if account.not_enough_cash(stock_id, 0, today_open):
                print ' info, dealed old stocks to 0: stop deal at %s, now break' %stock_id
                break
            account.order_to(stock_id, 0, today_open)
    #计算总的资产,目的是为了得到每支股票平均需要投入多少资金
    average_capital = account.dynamic_record['capital'][-1] / len买卖(reselected_stocks)
    #计算每支股票应该买卖多少手,然后进行
    deal_count = 0
    for stock_id in reselected_stocks:
        #手数向上取整
        open_price = account.getTodayOpen(stock_id)
        lot_to_be_dealed = math.ceil(average_capital / float(open_price))
        if account.not_enough_cash(stock_id, lot_to_be_dealed, open_price):
            print ' info: dealed %d stocks in reselected_stocks, total %d, stop deal at %s, now break'\
                    %(deal_count, len(reselected_stocks), stock_id)
            break
        account.order_to(stock_id, lot_to_be_dealed, open_price)
        deal_count += 1
    """
    # reselected_stock = []
    # for stock_id in latest_universe:
    #     if account.isOk2order(stock_id):
    #         trainData = account.getDailyHistory(stock_id, 90)


    """
    此函数为每个月处理一次
    找出S/L,S/M,S/H,B/L,B/M,B/H组合
    将沪深３００的股票按照每月第一天的流通市值从小到大排序，平均分成small和big两部分
    按照B/P从小到大排序，将股票分为Low,Median,High三个组合，比例分别问30%,40%,30%（由于从数据库取的是P/B，所以把P/B从高到低排序）
    形成６个组合S/L,S/M,S/H,B/L,B/M,B/H
    S/L表示既在small又在low的股票的组合
    """

    """
    **************************************************
    ****************** handle SMB ********************
    **************************************************
    """
    # test
    print"*****************************************"
    print"begin of handle SMB"
    # 按照每月第一天交易日的流通市值进个交易日行排序

    month_count = 6
    firstDay = []  # 所有股票每月第一天的相关数据
    lastDay = []  # 所有股票每月最后一天的相关数据
    day_first = 1
    day_last = 31
    while len(firstDay) == 0:
        # 要剔除空值
        sql_mv = "select distinct date_time, trade_code, open, close, mkt_cap_float from `hangyedata_new` " \
                 "where date_time='2007-0%d-0%d 00:00:00' and open is not null and close is not null and mkt_cap_float is not null"%(myMonth,day_first)
        cur = conn1.cursor()
        cur.execute(sql_mv)
        firstDay = cur.fetchall()  # return a list of tuples. If no more rows are available, it returns an empty list.
        # firstDay seems to be a tuple of tuples?

        day_first += 1
    firstDayofMonth = '%s-0%d-0%d'%(year, myMonth, day_first)

    while( len(lastDay) == 0 ):
        sql_mv = "select distinct date_time, trade_code, open, close, mkt_cap_float from `hangyedata_new` " \
                 "where date_time='2007-0%d-%d 00:00:00' and open is not null and close is not null and mkt_cap_float is not null"%(myMonth,day_last)
        cur = conn1.cursor()
        cur.execute(sql_mv)
        lastDay = cur.fetchall()  # return a list of tuples. If no more rows are available, it returns an empty list.
        day_last -= 1
    lastDayofMonth = '%s-0%d-%d'%(year, myMonth, day_last)

    # because tuple is immutable type,cannot "remove",so convert tuple to list
    # for item in firstDay:
    #     item = list(item)
    # for item in lastDay:
    #     item = list(item)
    firstDay_list = list(firstDay)
    lastDay_list = list(lastDay)

    # test
    #print "first_list:"
    #print firstDay_list
    print "before remove non_hushen,len of firstDay_list:"
    print len(firstDay_list)
    print "before remove non_hushen,len of lastDay_list:"
    print len(lastDay_list)

    # 把不是沪深300的股票剔除
    s = set(latest_universe)
    #test
    print "s is all stocks of HUSHEN 300:"
    print s
    print "len of s:"
    print len(s)
    # mv = {}  #是一个字典，存储一个股票的信息。key为trade_code,value为流通市值
    # # mv = [] # 是一个字典list，存储取到的沪深300的相关信息

    count = 0
    for a1 in firstDay_list:
        count = 0  # 注意清零
        for b1 in s:
            # test
            # print a1[1]
            # print b1[0:6]
            if a1[1] != b1[0:6]:  # a[0] is trade_code mv[a1[0]] = a1[1]  # a[1] is market value, mv：按月，沪深300股票集合
                count += 1
        if count == len(s):
            firstDay_list.remove(a1)

    count = 0
    for a2 in lastDay_list:
        count = 0
        for b2 in s:
            if a2[1] != b2[0:6]:
                count += 1
        if count == len(s):
            lastDay_list.remove(a2)

    # test
    print"after remove non-hushen,len of firstDay_list:"
    print len(firstDay_list)
    print"after remove non-hushen,len of lastDay_list:"
    print len(lastDay_list)

    # 按照每个月第一天的流通市值进行排序
    sorted_firstDay = sorted(firstDay_list, key=lambda d: d[4], reverse=False)  # 按照流通市值排序low to high,　返回sorted_firstDay是一个list of tuple
    # 将2007.i月份沪深300的股票按照市值大小分成了两组
    mvSmall = sorted_firstDay[:int(len(sorted_firstDay)/2)]
    mvBig = sorted_firstDay[int(len(sorted_firstDay)/2):int(len(sorted_firstDay))]
    mvList = []
    # mvList.append(mvSmall)
    # mvList.append(mvBig)

    """
    **************************************************
    ****************** handle HML ********************
    **************************************************
    """
    # test
    print"*****************************************"
    print"begin of handle HML"
    # 只需要取每个月第一天的pb值对股票进行排序分组
    firstDay_pb = []  # 所有股票每月第一天的相关数据
    # lastDay_pb = []  # 所有股票每月最后一天的相关数据
    # 重置两个常数
    day_first = 1
    # day_last = 31
    # while len(firstDay_pb) == 0:
    #     sql_pb = "select wind_code, date_time, pb from `a_stock_pb`, where date_time = '2007-0%d-0%d 00:00:00'"%(i, day_first)
    #     cur = conn2.cursor()
    #     cur.execute(sql_pb)
    #     firstDay_pb = cur.fetchall()
    #     day_first += 1
    #sql_pb = "select wind_code, date, pb from `a_stock_pb`, where date='%s 00:00:00'"%firstDayofMonth
    sql_pb = "select wind_code, date, pb from `a_stock_pb` where date = '%s 00:00:00'"%firstDayofMonth
    cur = conn2.cursor()
    cur.execute(sql_pb)
    firstDay_pb = cur.fetchall()

    # convert tuple to list
    firstDay_pb_list = list(firstDay_pb)

    # test
    print"before remove non_hushen,len of firstDay_pb_list:"
    print len(firstDay_pb_list)

    # 把不是沪深３００的股票剔除
    s = set(latest_universe)
    count = 0
    for a2 in firstDay_pb:
        count = 0
        for b2 in s:
            if a2[0] != b2:  # 不需要a2[0] == b2[0:6]，因为wind_code的格式是000515.SZ
                count += 1
        if count == len(s):
            firstDay_pb_list.remove(a2)
    # test
    print"after remove non_hushen,len of firstDay_pb_list:"
    print len(firstDay_pb_list)

    # 按照p/b排序high to low 返回，也即B/P从low to high,mv是一个list of tuple
    sorted_firstDay_pb = sorted(firstDay_pb_list, key=lambda d:d[2], reverse=True)
    pbLow = sorted_firstDay_pb[:int(len(sorted_firstDay_pb)*0.3)]  # 30% low
    pbMid = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.3):int(len(sorted_firstDay_pb)*0.7)]  # 40% median
    pbHigh = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.7):] # 30% high
    # pbList = []
    # pbList.append(pbLow)
    # pbList.append(pbMid)
    # pbList.append(pbHigh)
    print"end of handle HML"

    """
    ********************************************************************
    *************************** 求交得到六个组合S/L~B/H*******************
    ********************************************************************
    """
    # test
    print"******************************************"
    print"求交得到六个组合S/L~B/H"
    SH = []  # list of tuple
    SM = []
    SL = []
    BH = []
    BM = []
    BL = []
    for ms_item in mvSmall:
        for pl_item in pbLow:
            if ms_item[1] == pl_item[0][0:6]:  # 既在mvSmall又在pbLow的股票，S/L组合，根据trade_code相等来判断
                # 因为ms_item的trade_code的格式为000515，而pl_item中wind_code的
                # 格式是000515.SZ
                SL.append(ms_item)
        for pm_item in pbMid:
            if ms_item[1] == pm_item[0][0:6]:  # 既在mvSmall又在pbMid的股票，S/M组合
                SM.append(ms_item)
        for ph_item in pbHigh:
            if ms_item[1] == ph_item[0][0:6]:  # 既在mvSmall又在pbHigh的股票，S/H组合
                SH.append(ms_item)
    # SL.append(SL_monthly)
    # SM.append(SM_monthly)
    # SH.append(SH_monthly)

    for mb_item in mvBig:
        for pl_item in pbLow:
            if mb_item[1] == pl_item[0][0:6]:  # 既在mvSmall又在pbLow的股票，B/L组合
                BL.append(mb_item)  # SL存储的是每个月沪深300的S/L组合的trade_code
        for pm_item in pbMid:
            if mb_item[1] == pm_item[0][0:6]:  # 既在mvSmall又在pbMid的股票，B/M组合
                BM.append(mb_item)
        for ph_item in pbHigh:
            if mb_item[1] == ph_item[0][0:6]:  # 既在mvSmall又在pbHigh的股票，B/H组合
                BH.append(mb_item)
    # BL.append(BL_monthly)  # list of list
    # BM.append(BM_monthly)
    # BH.append(BH_monthly)
    # test
    print"end of 求交得到六个组合S/L~B/H"

    """
    ********************************************************************
    ************************* 求六个组合的月平均收益率 ********************
    ********************************************************************
    """
    # test
    print"******************************************"
    print"求六个组合的月平均收益率"
    # 组合的平均收益率＝组合内每支股票的收益率
    # 某股票月收益率＝（月最后一天收盘价－月第一天收盘价）／月第一天收盘价

    #SL
    revenueSum = 0
    for sl_item in SL:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
        closeOfFirst = sl_item[3]
        for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
            if sl_item[1] == stock[1]:
                closeOfLast = stock[3]
        sl_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
        revenueSum += sl_item_revenue
    SL_revenue = revenueSum / len(SL)

    #SM
    revenueSum = 0
    for sm_item in SM:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
        closeOfFirst = sl_item[3]
        for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
            if sm_item[1] == stock[1]:
                closeOfLast = stock[3]
        sm_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
        revenueSum += sm_item_revenue
    SM_revenue = revenueSum / len(SM)

    #SH
    revenueSum = 0
    for sh_item in SH:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
        closeOfFirst = sh_item[3]
        for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
            if sh_item[1] == stock[1]:
                closeOfLast = stock[3]
        sh_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
        revenueSum += sh_item_revenue
    SH_revenue = revenueSum / len(SH)

    #BL
    revenueSum = 0
    for bl_item in BL:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
        closeOfFirst = bl_item[3]
        for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
            if bl_item[1] == stock[1]:
                closeOfLast = stock[3]
        bl_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
        revenueSum += bl_item_revenue
    BL_revenue = revenueSum / len(BL)

    #BM
    revenueSum = 0
    for bm_item in BM:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
        closeOfFirst = bm_item[3]
        for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
            if bm_item[1] == stock[1]:
                closeOfLast = stock[3]
        bm_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
        revenueSum += bm_item_revenue
    BM_revenue = revenueSum / len(BM)

    #BH
    revenueSum = 0
    for bh_item in BH:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
        closeOfFirst = bh_item[3]
        for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
            if bh_item[1] == stock[1]:
                closeOfLast = stock[3]
        bh_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
        revenueSum += bh_item_revenue
    BH_revenue = revenueSum / len(BH)

    # test
    print"end of 求六个组合的月平均收益率"

    """
    ********************************************************************
    ************************* 求每个月的SMB和HML ************************
    ********************************************************************
    """
    #test
    print"***********************************************"
    print"begin of calculate SMB and HML"
    SMB_revenue = (SL_revenue+SM_revenue+SH_revenue)/3 - (BL_revenue+BM_revenue+BH_revenue)/3
    HML_revenue = (SH_revenue+BH_revenue)/2 - (BL_revenue+SL_revenue)/2
    SMB.append(SMB_revenue)
    HML.append(HML_revenue)

    """
    ***************************************************************************
    ************************** 计算市场收益率Rmt ********************************
    ***************************************************************************
    """
    # test
    print"*****************************************"
    print"begin of handle Rmt"
    # test
    #print account.dataIF  # dataIF is a dataframe
    ifClose_firstDay = account.dataIF[account.dataIF['Date']==firstDayofMonth]['Close']  # 每月第一天的收盘价
    ifClose_lastDay = account.dataIF[account.dataIF['Date']==lastDayofMonth]['Close']  # 每月最后一天的收盘价
    # Open=self.dataIF[self.dataIF['Date']==self.current_time] ['Open'].values[0]
    Rmt_monthly = (ifClose_lastDay - ifClose_firstDay) / ifClose_firstDay
    Rmt_minus_Rf = Rmt_monthly - Rf
    Rmt_Rf.append(Rmt_minus_Rf)
    # test
    print"end of handle Rmt"

    """
    ***************************************************************************
    ****************** 计算Rit:沪深300所有股票的个股的月平均收益率 ****************
    ***************************************************************************
    """
    # test
    print"*****************************************"
    print"begin of handle Rit"
    #earning_Rit_sum = 0
    Rit_monthly = {}  # 储存沪深300个股的月收益率,用字典：key为trade_code,value为改股票的月收益率
    for firstday_item in firstDay:  # for each stock in hushen 300
        firstday_open = firstday_item[2]
        # get close price of last day of stock firstday_item
        for lastday_item in lastDay:
            if firstday_item[1] == lastday_item[1]:
                lastday_close = lastday_item[3]
        earning_stock = (lastday_close - firstday_open) / firstday_open
        earning_stock -= Rf
        Rit_monthly[firstday_item[1]] = earning_stock  # 某月每个股票的月回报率
    Rit.append(Rit_monthly)  # Rit是6个月的每个股票的回报率，list of dict
    # test
    print"len of Rit is %d"%len(Rit)

    # test
    print"end of handle Rit"


def regression():
    median = month_count / 2
    SMB_train = SMB[0:median]
    HML_train = HML[0:median]
    Rmt_Rf_train = Rmt_Rf[0:median]
    SMB_predict = SMB[median:]
    HML_predict = HML[median:]
    Rmt_Rf_predict = Rmt_Rf[median:]

    x_train = np.array([Rmt_Rf_train,
                        SMB_train,
                        HML_train])
    # y_train = np.array(Rit_Rf.values)


