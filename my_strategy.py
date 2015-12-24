#coding=utf-8
__author__ = 'lisa'
import math
from datetime import datetime,date
from sklearn import linear_model  # for linear regression
import numpy as np
import datetime, time
import pandas as pd
from pandas import Series, DataFrame

import MySQLdb
#
# class Commission(object):
#     def __init__(self,buycost=0.001,sellcost=0.002,unit='perValue'):
#         self.buycost=buycost
#         self.sellcost=sellcost
#         self.unit=unit

# new_stock
conn1=MySQLdb.connect(host="192.168.0.105", user="root", passwd="fit123456", db="new_stock", charset="utf8")
conn2= MySQLdb.connect(host="192.168.0.105", user="root", passwd="fit123456", db="GARP", charset="utf8")
# # conn=MySQLdb.connect(host="host",user="root",port=16941,passwd="a8674925",db="new_stock",charset="utf8")
# cursor = conn.cursor()
# sql = ""


start = date(2007, 7, 31)  # 回测起始时间
end = date(2015, 1, 1)  # 回测结束时间

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

SMB = []  # 储存7个月的SMB值
HML = []
RMW = []
CMA = []
Rmt_Rf = []
Rit = []
stock_predict = {}  # 存储沪深300股票trade_code和相应的收益率
Rf = 0.04
time_interval = 7  # 6个月的数据作为train,1个月的数据作为predict


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
    # 止损, 每天都运行
    stop_loss(account)


def process_stocks(account, latest_universe):
    # 说明,这是一个简单的测试策略,用户可以根据需求自定义策略
    # 1. 对新的股票列表进行筛选,得到这个月第一个交易日的开盘价大于上
    # 个月最后一天的收盘价, 这样就得到了一个待下单的股票列表
    # 2. 对下单的股票列表的每个元素分别计算要下单的手数


    #对新的股票列表进行筛选,得到这个月第一个交易日的开盘价大于上
    #个月最后一天的收盘价, 这样就得到了一个待下单的股票列表

    reselected_stocks = []
    global SMB, HML, Rmt_Rf, Rit  # 具体函数中使用该变量时，需要事先声明 global variable，否则系统将该变量视为局部变量。
    # SMB[]~stock_predict{}为全局变量，每个月运行process_stocks时候,需要清空
    SMB = []  # 储存7个月的SMB值
    HML = []
    Rmt_Rf = []
    Rit = []
    #stock_predict.clear()  # 存储沪深300股票trade_code和相应的收益率

    predict_revenue = {}  # 存储沪深300的股票code以及其对应的预测收益率

    """
    ***** handle time *********
    """
    time = account.current_time
    train_year = time.year
    # handle month
    myMonth = time.month
    train_month = myMonth - time_interval
    if train_month > 0:
        train_month = train_month
    elif train_month <= 0:
        train_month = train_month + 12
        train_year -= 1

    """
    ***** handle factor *********
    """
    handle_factor(account, train_year, train_month, latest_universe)
    """
    ***** regression *********
    """
    regression(predict_revenue,latest_universe)

    # 将predict_revenue按照value，也即预测收益率进行排序,从大到小
    sorted_predict_revenue = sorted(predict_revenue.iteritems(), key=lambda d:d[1], reverse = True)
    # 选取前20%的股票作为待卖入的股票
    select_count = int(len(latest_universe)*0.2)
    selected_dict = sorted_predict_revenue[0:select_count] # is a list of tuples
    for item in selected_dict:
        reselected_stocks.append(item[0])
    #reselected_stocks = selected_dict.keys()

    # reselected_stocks = []
    # for stock_id in latest_universe:
    #     if account.isOk2order(stock_id):
    #         #比较这个月第一个交易日的开盘价是否大于上个月最后一个交易日的收盘价
    #         if account.getTodayOpen(stock_id) > account.getDailyHistory(stock_id, 1)['close'].values[-1]:
    #             #把符合开盘价大于收盘价的股票放入一个新的列表
    #             reselected_stocks.append(stock_id)

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
    average_capital = account.dynamic_record['capital'][-1] / len(reselected_stocks)
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
    # reselected_stock = []
    # for stock_id in latest_universe:
    #     if account.isOk2order(stock_id):
    #         trainData = account.getDailyHistory(stock_id, 90)

     # test


def handle_factor(account, train_year, train_month, latest_universe):
    global Rf, time_interval
    # test
    print"***********************************************************"
    print"begin of handle_factor"

    for i in range(0, time_interval):  # 共7个月，每次循环后年月日要做相应的递增
        # test
        print"handle_factor,---- loop %d ----"%i
        """
        此函数为每个月处理一次
        找出S/L,S/M,S/H,B/L,B/M,B/H组合
        将沪深３００的股票按照每月第一天的流通市值从小到大排序，平均分成small和big两部分
        按照B/P从小到大排序，将股票分为Low,Median,High三个组合，比例分别问30%,40%,30%（由于从数据库取的是P/B，所以把P/B从高到低排序）
        形成６个组合S/L,S/M,S/H,B/L,B/M,B/H
        S/L表示既在small又在low的股票的组合

        在三因子基础上增加两个因子oprating profitability and investment
        """

        """
        **************************************************
        ****************** handle size ********************
        **************************************************
        """
        # test
        print"*****************************************"
        print"begin of handle size"
        # 按照每月第一天交易日的流通市值进个交易日行排序
        # month_count = 6
        firstDay = []  # 所有股票每月第一天的相关数据
        lastDay = []  # 所有股票每月最后一天的相关数据
        day_first = 1
        day_last = 31
        while len(firstDay) == 0:
            # 要剔除空值
            sql_mv = "select distinct date_time, trade_code, open, close, mkt_cap_float from `hangyedata_new` " \
                     "where date_time='%d-%d-%d 00:00:00' and open is not null and close is not null and mkt_cap_float is not null"%(train_year,train_month,day_first)
            cur = conn1.cursor()
            cur.execute(sql_mv)
            firstDay = cur.fetchall()  # return a list of tuples. If no more rows are available, it returns an empty list.
            # firstDay seems to be a tuple of tuples?
            day_first += 1
        # 在已经取得firstday数据之后，判断len(firstDay)!=0之前，day_first已经+1
        day_first -= 1
        firstDayofMonth = '%d-%d-%d'%(train_year, train_month, day_first)
        # test
        print"first day of month is :%s"%firstDayofMonth

        while( len(lastDay) == 0 ):
            sql_mv = "select distinct date_time, trade_code, open, close, mkt_cap_float from `hangyedata_new` " \
                     "where date_time='%d-%d-%d 00:00:00' and open is not null and close is not null and mkt_cap_float is not null"%(train_year,train_month,day_last)
            cur = conn1.cursor()
            cur.execute(sql_mv)
            lastDay = cur.fetchall()  # return a list of tuples. If no more rows are available, it returns an empty list.
            day_last -= 1
        day_last += 1
        lastDayofMonth = '%s-%d-%d'%(train_year, train_month, day_last)
        # test
        print"last day of month is :%s"%lastDayofMonth

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

        # 把不是沪深300的股票剔除，沪深300是为current_time时的，因为
        # sql_hushen = "select * from my_stock_index WHERE group_code = '%s'and date_time = '%s' "%(account.universe_code,firstDayofMonth)
        # hushen_temp = pd.read_sql(sql_hushen,conn1)
        # hushen_set = set([])
        # for item in hushen_temp['content']:
        #     hushen_set.update(item.split(','))
        # # test
        # print "len of hushen", len(hushen_set)
        # # hushen = list(hushen_set)

        hushen_set = set(latest_universe)
        # 把不是沪深300的股票剔除
        #　test
        print "len of hushen", len(hushen_set)
        # mv = {}  #是一个字典，存储一个股票的信息。key为trade_code,value为流通市值
        # # mv = [] # 是一个字典list，存储取到的沪深300的相关信息

        count = 0
        firstDay_afterRemove = []
        lastDay__afterRemove = []
        for a1 in firstDay_list:
            count = 0  # 注意清零
            for b1 in hushen_set:
                # test
                # print a1[1]
                # print b1[0:6]
            #     if a1[1] != b1[0:6]:  # a[0] is trade_code mv[a1[0]] = a1[1]  # a[1] is market value, mv：按月，沪深300股票集合
            #         count += 1
            # if count == len(hushen_set) - 1:
            #     # firstDay_list.remove(a1)
            #     firstset.append(a1)
                if a1[1] == b1[0:6]:
                    firstDay_afterRemove.append(a1)
        count = 0
        for a2 in lastDay_list:
            count = 0
            for b2 in hushen_set:
            #     if a2[1] != b2[0:6]:
            #         count += 1
            # if count == len(hushen_set):
            #     lastDay_list.remove(a2)
                if a2[1] == b2[0:6]:
                    lastDay__afterRemove.append(a2)

        # test
        print"after remove non-hushen,len of (mv)firstDay_afterRemove:"
        # print len(firstDay_list)
        print len(firstDay_afterRemove)
        print"after remove non-hushen,len of (mv)lastDay_afterRemove:"
        print len(lastDay__afterRemove )

        # 按照每个月第一天的流通市值进行排序
        sorted_firstDay = sorted(firstDay_afterRemove, key=lambda d: d[4], reverse=False)  # 按照流通市值排序low to high,　返回sorted_firstDay是一个list of tuple
        # 将2007.i月份沪深300的股票按照市值大小分成了两组
        mvSmall = sorted_firstDay[:int(len(sorted_firstDay)/2)]
        mvBig = sorted_firstDay[int(len(sorted_firstDay)/2):int(len(sorted_firstDay))]
        mvList = []
        # mvList.append(mvSmall)
        # mvList.append(mvBig)

        """
        **************************************************
        ****************** handle B/M ********************
        **************************************************
        """
        # test
        print"*****************************************"
        print"begin of handle B/M"
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
        sql_pb = "select wind_code, date, pb from `a_stock_annue_day` where date = '%s 00:00:00'"%firstDayofMonth
        cur = conn2.cursor()
        cur.execute(sql_pb)
        firstDay_pb = cur.fetchall()

        # convert tuple to list
        firstDay_pb_list = list(firstDay_pb)

        # test
        print"before remove non_hushen,len of firstDay_pb_list:"
        print len(firstDay_pb_list)

        # 把不是沪深３００的股票剔除
        count = 0
        firstDaypb_afterRemove = []
        for a2 in firstDay_pb_list:
            count = 0
            for b2 in hushen_set:
            #     if a2[0] != b2:  # 不需要a2[0] == b2[0:6]，因为wind_code的格式是000515.SZ
            #         count += 1
            # if count == len(hushen_set):
            #     firstDay_pb_list.remove(a2)
                if a2[0] == b2:
                    firstDaypb_afterRemove.append(a2)
        # test
        print"after remove non_hushen,len of firstDaypb_afterRemove:"
        print len(firstDaypb_afterRemove)

        # 按照p/b排序high to low 返回，也即B/P从low to high,mv是一个list of tuple
        sorted_firstDay_pb = sorted(firstDaypb_afterRemove, key=lambda d:d[2], reverse=True)
        pbLow = sorted_firstDay_pb[:int(len(sorted_firstDay_pb)*0.3)]  # 30% low
        pbMid = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.3):int(len(sorted_firstDay_pb)*0.7)]  # 40% median
        pbHigh = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.7):] # 30% high
        # pbList = []
        # pbList.append(pbLow)
        # pbList.append(pbMid)
        # pbList.append(pbHigh)
        print"end of handle B/M"

        """
        **************************************************
        ****** handle profitability and investment *******
        **************************************************
        """
        # test
        print"*****************************************"
        print"begin of handle profitability"
        year_getOPData = train_year
        month_getOPData = 0
        day_getOPData = 0
        if train_month == 1 or train_month == 2 or train_month == 3:
            year_getOPData -= 1
            month_getOPData = 9
            day_getOPData = 30
            if train_year == 2007:  # 因为数据库中没有2006.9.30的数据
                month_getOPData = 12
                day_getOPData = 31
        elif train_month == 4 or train_month == 5 or train_month == 6 or train_month == 7:
            year_getOPData -= 1
            month_getOPData = 12
            day_getOPData = 31
        elif train_month == 8 or train_month == 9:
            month_getOPData = 6
            day_getOPData = 30
        elif train_month == 10 or train_month == 11 or train_month == 12:
            month_getOPData = 9
            day_getOPData = 30
        date_getOPData = '%d-%d-%d'%(year_getOPData, month_getOPData, day_getOPData)  # 在数据库中取OP数据的日期
        # 从数据库中取处date_getOPData那一天的数据
        sql_OP = "select wind_code, date, net_profit_is, tot_assets from `A_stock_lishe` where date = '%s 00:00:00'"%date_getOPData
        cur = conn2.cursor()
        cur.execute(sql_OP)
        pro_and_inv = cur.fetchall()

        # test
        print "before remove non_hushen,len of pro_and_inv:"
        print len(pro_and_inv)

        # 把不是沪深300的股票剔除
        hushen_set = set(latest_universe)
        #　test
        print "len of hushen", len(hushen_set)
        count = 0
        pro_and_inv_afterRemove = []
        for a in pro_and_inv:
            count = 0  # 注意清零
            for b in hushen_set:
                if a[0] == b:
                    pro_and_inv_afterRemove.append(a)
         # test
        print"after remove non-hushen,len of pro_and_inv_afterRemove:",len(pro_and_inv_afterRemove)

        # 按照pro进行排序
        sorted_pro = sorted(pro_and_inv_afterRemove, key=lambda d: d[2], reverse=False)
        # 股票按照pro大小分成了三组
        proWeak = sorted_firstDay_pb[:int(len(sorted_firstDay_pb)*0.3)]  # 30% weak
        proNeu = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.3):int(len(sorted_firstDay_pb)*0.7)]  # 40% neutral
        proRob = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.7):] # 30% robust

        # 按照inv进行排序
        sorted_pro = sorted(pro_and_inv_afterRemove, key=lambda d: d[2], reverse=False)
        # 股票按照inv大小分成了三组
        invCon = sorted_firstDay_pb[:int(len(sorted_firstDay_pb)*0.3)]  # 30% conservative
        invNeu = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.3):int(len(sorted_firstDay_pb)*0.7)]  # 40% neutral
        invAgg = sorted_firstDay_pb[int(len(sorted_firstDay_pb)*0.7):] # 30% aggressive

        print"end of handle profitability and investment"



        """
        ********************************************************************
        *************************** 求交得到18个组合S/L~B/H*******************
        ********************************************************************
        """
        # test
        print"******************************************"
        print"求交得到18个组合S/L~B/H"
        # size-B/M
        SH = []  # list of tuple
        SM = []
        SL = []
        BH = []
        BM = []
        BL = []
        # size-pro
        SR = []
        SN_pro = []
        SW = []
        BR = []
        BN_pro = []
        BW = []
        # size-inv
        SA = []
        SN_inv = []
        SC = []
        BA = []
        BN_inv = []
        BC = []

        # size-small
        for ms_item in mvSmall:
            # size-B/M
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
            # size-pro
            for pw_item in proWeak:
                if ms_item[1] == pw_item[0][0:6]:  # SW,pw_itme中wind_code的格式为000515.SZ，ms_item中code格式为000515
                    SW.append(ms_item)
            for pn_item in proNeu:
                if ms_item[1] == pn_item[0][0:6]:  # SN
                    SN_pro.append(ms_item)
            for pr_item in proRob:
                if ms_item[1] == pr_item[0][0:6]:  # SR
                    SR.append(ms_item)
            # size-inv
            for ic_item in invCon:
                if ms_item[1] == ic_item[0][0:6]:  # SC
                    SC.append(ms_item)
            for in_item in invNeu:
                if ms_item[1] == in_item[0][0:6]:  # SN_inv
                    SN_inv.append(ms_item)
            for ia_item in invAgg:
                if ms_item[1] == ia_item[0][0:6]:  # SA
                    SA.append(ms_item)

        # size-big
        for mb_item in mvBig:
            # size-B/M
            for pl_item in pbLow:
                if mb_item[1] == pl_item[0][0:6]:  # 既在mvSmall又在pbLow的股票，B/L组合
                    BL.append(mb_item)  # SL存储的是每个月沪深300的S/L组合的trade_code
            for pm_item in pbMid:
                if mb_item[1] == pm_item[0][0:6]:  # 既在mvSmall又在pbMid的股票，B/M组合
                    BM.append(mb_item)
            for ph_item in pbHigh:
                if mb_item[1] == ph_item[0][0:6]:  # 既在mvSmall又在pbHigh的股票，B/H组合
                    BH.append(mb_item)
            # size-pro
            for pw_item in proWeak:
                if mb_item[1] == pw_item[0][0:6]:  # SW,pw_itme中wind_code的格式为000515.SZ，ms_item中code格式为000515
                    BW.append(mb_item)
            for pn_item in proNeu:
                if mb_item[1] == pn_item[0][0:6]:  # SN
                    BN_pro.append(mb_item)
            for pr_item in proRob:
                if mb_item[1] == pr_item[0][0:6]:  # SR
                    BR.append(mb_item)
            # size-inv
            for ic_item in invCon:
                if mb_item[1] == ic_item[0][0:6]:  # SC
                    BC.append(mb_item)
            for in_item in invNeu:
                if mb_item[1] == in_item[0][0:6]:  # SN_inv
                    BN_inv.append(mb_item)
            for ia_item in invAgg:
                if mb_item[1] == ia_item[0][0:6]:  # SA
                    BA.append(mb_item)

        print"end of 求交得到18个组合S/L~B/H"

        """
        ********************************************************************
        ************************* 求18个组合的月平均收益率 ********************
        ********************************************************************
        """
        # test
        print"******************************************"
        print"求18个组合的月平均收益率"
        # 组合的平均收益率＝组合内每支股票的收益率
        # 某股票月收益率＝（月最后一天收盘价－月第一天收盘价）／月第一天收盘价

        # size-B/M
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

        # size-pro
        # SR
        revenueSum = 0
        for sr_item in SR:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = sr_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if sr_item[1] == stock[1]:
                    closeOfLast = stock[3]
            sr_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += sr_item_revenue
        SR_revenue = revenueSum / len(SR)

        # SN_pro
        revenueSum = 0
        for snpro_item in SN_pro:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = snpro_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if snpro_item[1] == stock[1]:
                    closeOfLast = stock[3]
            snpro_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += snpro_item_revenue
        SN_pro_revenue = revenueSum / len(SN_pro)

        # SW
        revenueSum = 0
        for sw_item in SW:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = sw_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if sw_item[1] == stock[1]:
                    closeOfLast = stock[3]
            sw_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += snpro_item_revenue
        SW_revenue = revenueSum / len(SW)

        # BR
        revenueSum = 0
        for br_item in BR:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = br_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if br_item[1] == stock[1]:
                    closeOfLast = stock[3]
            br_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += br_item_revenue
        BR_revenue = revenueSum / len(BR)

        # BN_pro
        revenueSum = 0
        for bnpro_item in BN_pro:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = bnpro_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if bnpro_item[1] == stock[1]:
                    closeOfLast = stock[3]
            bnpro_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += bnpro_item_revenue
        BN_pro_revenue = revenueSum / len(BN_pro)

        # BW
        revenueSum = 0
        for bw_item in BW:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = bw_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if bw_item[1] == stock[1]:
                    closeOfLast = stock[3]
            bw_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += bw_item_revenue
        BW_revenue = revenueSum / len(BW)

        # size-inv
        # SA
        revenueSum = 0
        for sa_item in SA:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = sa_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if sa_item[1] == stock[1]:
                    closeOfLast = stock[3]
            sa_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += sa_item_revenue
        SA_revenue = revenueSum / len(SA)

        # SN_inv
        for sninv_item in SN_inv:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = sninv_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if sninv_item[1] == stock[1]:
                    closeOfLast = stock[3]
            sninv_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += sninv_item_revenue
        SN_inv_revenue = revenueSum / len(SN_pro)

        # SC
        revenueSum = 0
        for sc_item in SC:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = sc_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if sc_item[1] == stock[1]:
                    closeOfLast = stock[3]
            sc_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += sc_item_revenue
        SC_revenue = revenueSum / len(SC)

        # BA
        revenueSum = 0
        for ba_item in BA:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = ba_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if ba_item[1] == stock[1]:
                    closeOfLast = stock[3]
            ba_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += ba_item_revenue
        BA_revenue = revenueSum / len(BA)

        # BN_inv
        for bninv_item in BN_inv:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = bninv_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if bninv_item[1] == stock[1]:
                    closeOfLast = stock[3]
            bninv_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += bninv_item_revenue
        BN_inv_revenue = revenueSum / len(BN_pro)

        # BC
        revenueSum = 0
        for bc_item in BC:  # sl_item is a tuple:(date_time, trade_code, open, close, mkt_cap_float)
            closeOfFirst = bc_item[3]
            for stock in lastDay_list:  # 在lastDay_list中找到相应股票的月最后一天的收盘价
                if bc_item[1] == stock[1]:
                    closeOfLast = stock[3]
            bc_item_revenue = (closeOfLast - closeOfFirst) / closeOfFirst
            revenueSum += bc_item_revenue
        BC_revenue = revenueSum / len(BC)

        # test
        print"end of 求18个组合的月平均收益率"

        """
        ********************************************************************
        ******************** 求每个月的SMB、HML、RMW、CMA ********************
        ********************************************************************
        SMB_BM=(SL+SM+SH)/3-(BL+BM+BH)/3
        SMB_OP=(SR+SN+SW)/3-(BR+BN+BW)/3
        SMB_inv=(SC+SN+SA)/3-(BC+BN+BA)/3
        SMB=(SMB_BM+SMB_OP+SMB_inv）/3
        HML=(SH+BH)/2-(BL+SL)/2
        RMW=(SR+BR)/2-(SW+BW)/2
        CMA=(SC+BC)/2-(SA+BA)/2
        """
        #test
        print"******************************************"
        print"begin of calculate SMB、HML、RMW、CMA"
        SMB_BM_revenue = (SL_revenue+SM_revenue+SH_revenue)/3 - (BL_revenue+BM_revenue+BH_revenue)/3
        SMB_OP_revenue = (SR_revenue+SN_pro_revenue+SW_revenue)/3 - (BR_revenue+BN_pro_revenue+BW_revenue)/3
        SMB_inv_revenue=(SC_revenue+SN_inv_revenue+SA_revenue)/3-(BC_revenue+BN_inv_revenue+BA_revenue)/3
        SMB_revenue=(SMB_BM_revenue+SMB_OP_revenue+SMB_inv_revenue)/3
        HML_revenue = (SH_revenue+BH_revenue)/2 - (BL_revenue+SL_revenue)/2
        RMW_revenue=(SR_revenue+BR_revenue)/2-(SW_revenue+BW_revenue)/2
        CMA_revenue=(SC_revenue+BC_revenue)/2-(SA_revenue+BA_revenue)/2
        SMB.append(SMB_revenue)
        HML.append(HML_revenue)
        RMW.append(RMW_revenue)
        CMA.append(CMA_revenue)
        # test
        print"end of calculate SMB、HML、RMW、CMA"

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
        # ifClose_firstDay = account.dataIF[account.dataIF['Date']==firstDayofMonth]['Close']  # 每月第一天的收盘价
        # ifClose_lastDay = account.dataIF[account.dataIF['Date']==lastDayofMonth]['Close']  # 每月最后一天的收盘价
        # # Open=self.dataIF[self.dataIF['Date']==self.current_time] ['Open'].values[0]
        # mydataIF = account.dataIF.set_index(['Date'])

        # sql_dataIF="select Date,Open,High,Low,Close from `if000_d1` where Date>='%s' and Date<'%s' order by Date"%(mystart,myend) #获取if000股指期货的历史数据
        # #print sql
        # mydataIF=pd.read_sql(sql,conn1)
        # if self.start<datetime.date(2010,4,16):
        #     sql="select date_time as Date,open as Open,high as High,low as Low,close as Close " \
        #         "from `000300.sh` where date_time<'2010-04-16' and date_time>='%s'"%start
        #     temp=pd.read_sql(sql,self.connect.conn)
        #     self.dataIF=temp.append(self.dataIF)

        date_firstDayofMonth = datetime.datetime.strptime(firstDayofMonth,'%Y-%m-%d').date()
        date_lastDayofMonth = datetime.datetime.strptime(lastDayofMonth,'%Y-%m-%d').date()

        # get open and close price of first day of month
        if firstDayofMonth <= '2010-4-1':  # 2010.4.1以及之前的firstday的数据在00030.sh的表中取
            sqlIF_first="select open,close from `000300.sh` where date_time = '%s'"%firstDayofMonth
            cur = conn1.cursor()
            cur.execute(sqlIF_first)
            IF_first = cur.fetchall()

        else:
            sqlIF_first="select open, close from `if000_d1` where date = '%s'"%firstDayofMonth
            cur = conn1.cursor()
            cur.execute(sqlIF_first)
            IF_first = cur.fetchall()

        # get open and close price of last day of month
        if lastDayofMonth <= '2010-4-30':
            sqlIF_last="select open,close from `000300.sh` where date_time = '%s'"%lastDayofMonth
            cur = conn1.cursor()
            cur.execute(sqlIF_last)
            IF_last = cur.fetchall()
        else:
            sqlIF_last="select open, close from `if000_d1` where date = '%s'"%lastDayofMonth
            cur = conn1.cursor()
            cur.execute(sqlIF_last)
            IF_last = cur.fetchall()

        ifClose_firstDay = IF_first[0][1]
        ifClose_lastDay = IF_last[0][1]

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

        #每次循环最后，年月做递增处理
        if train_month == 12:
            train_month = 1
            train_year += 1
        else:
            train_month += 1


def regression(predict_revenue, latest_universe):
    # test
    print"*****************************************"
    print"begin of regression"
    train_spilt = time_interval - 1 # 6个月数据用于train,1个月数据用于predict
    SMB_train = SMB[0:train_spilt]
    HML_train = HML[0:train_spilt]
    RMW_train = RMW[0:train_spilt]
    CMA_train = CMA[0:train_spilt]
    Rmt_Rf_train = Rmt_Rf[0:train_spilt]
    SMB_predict = SMB[train_spilt]
    HML_predict = HML[train_spilt]
    RMW_predict = RMW[train_spilt]
    CMA_predict = CMA[train_spilt]
    Rmt_Rf_predict = Rmt_Rf[train_spilt]

    x_train = np.array([Rmt_Rf_train,
                        SMB_train,
                        HML_train,
                        RMW_train,
                        CMA_train])
    x_train = x_train.transpose()
    # 对每支股票进行回归分析
    # Rit[{},{},{},{},{},{}]
    # 对current_time下的沪深300中的每一支股票进行回归分析
    # train
    # for stock in latest_universe:
    for i in range(0, len(latest_universe)):  # 对每一支股票
        # test
        print "into loop (in regression)",
        print "loop %d"%i
        stock_revenue_list = []
        # stock_code = stock[0:6]
        stock_code = latest_universe[i]
        stock_code_spilt = latest_universe[i][0:6]

        # # test
        # print"type of stock_code"
        # print type(stock_code)

        # 前6个月的收益率作为y_train
        if Rit[0].get(stock_code_spilt)!=None:  # 能够在Rit中找到的股票才进行回归分析。
            for i in range(0,train_spilt):
                # if Rit[i].get(stock_code)!=None
                stock_revenue = Rit[i].get(stock_code_spilt) # Rit[i] is a dict
                stock_revenue_list.append(stock_revenue)
            y_train = np.array(stock_revenue_list)
            # y_train_t = y_train.transpose()

            clf = linear_model.LinearRegression()
            clf.fit(x_train, y_train)
            print "coef_:", clf.coef_
            # print np.dot(x_train, clf.coef_)

            # predict
            x_predict = np.array([Rmt_Rf_predict,
                                  SMB_predict,
                                  HML_predict,
                                  RMW_predict,
                                  CMA_predict])
            y_predict = clf.predict(x_predict)  # type(y_predict)=numpy.ndarray
            print "y_predict:", y_predict
            predict_revenue[stock_code] = y_predict