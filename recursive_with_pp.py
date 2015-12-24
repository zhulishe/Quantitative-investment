#coding=utf-8
__author__ = 'fit tan'
########
# 递推函数
########

import numpy as np
import cPickle,memcache,mcQueue,time
import pandas as pd
from my_test import *
mc=memcache.Client(['192.168.0.103:11211'])
mq_in=mcQueue.mcQueue(mc,'input')
mq_r=mcQueue.mcQueue(mc,'result')
# 止损率列表
# rate_list = np.linspace(0,0.5,11)
# rate_list = np.linspace(0,1,41)
rate_list = np.linspace(0,0.2,21)
#rate_list = [0.1,0.125]
profit_dict = {}

class Recursive:
    def __init__(self,account, period='month',train_count=1,test_count=1):
        self.account = account
        self.period = period
        self.train_count = train_count - 1
        self.test_count = test_count
        self.last_change_month = self.account.start.month
        self.change_begin = None  # 改变止损率的训练时段的开始时间

    def set_recursive(self,rate):
        """
        修改止损参数
        :param rate: 止损参数
        :return:
        """
        self.account.f1 = rate

    def get_result(self):
        self.dad = pd.DataFrame(self.account.dynamic_record)
        for n,i in enumerate(account.timeList):
            account.last_time=account.current_time
            account.current_time=i
            account.days_counts=n+1
            if n<day_num-1:
                account.tomorrow=account.timeList[n+1]
            now_month = self.account.current_time.month
            now_year = self.account.current_time.year
            start_month = now_month
            start_year = now_year
            # self.last_change_month = (now_year - self.account.start.year) * 12 + self.account.start.month
            if now_month - self.train_count > 0:
                start_month = now_month - self.train_count
            elif now_month - self.train_count <= 0:
                month_delta = self.train_count % 12
                year_delta = self.train_count / 12
                if month_delta >= now_month:
                    start_month = 12 + now_month - month_delta
                    year_delta += 1
                    start_year = now_year - int(year_delta)
                else:
                    start_month = now_month - month_delta
                    start_year = now_year - int(year_delta)
            else:
                print 'something wrong !'
            # month_begin = self.account.getBeginOfMonth(start_year,start_month)
            self.change_begin = self.account.getBeginOfMonth(start_year,start_month)
            if self.account.tomorrow.month != now_month \
                    and (now_year - self.account.start.year) * 12 + now_month - self.account.start.month >= self.train_count \
                    and (now_year - self.account.start.year) * 12 + now_month - self.last_change_month >= self.test_count:
                '''
                月末，回测月数超过训练月数(train_count)，当前月距上次改变止损率大于测试月数(test_count)
                '''
                print self.last_change_month
                if self.account.current_time not in profit_dict:
                    self.last_change_month = (now_year - self.account.start.year) * 12 + now_month
                    profit_dict[self.account.current_time] = []
                # print 'month_begin',month_begin
                print self.change_begin
                capital_delta = self.dad[(self.dad['trade_time'] < self.account.tomorrow)
                                         & (self.dad['trade_time'] >= self.change_begin)]['capital']
                if len(capital_delta) > 0:
                    monthly_profit = capital_delta.values[-1] - capital_delta.values[0]
                    com = (self.account.f1,monthly_profit)  # 元组（止损率，训练时间段的收益）
                    profit_dict[self.account.current_time].append(com)  # 字典{止损率改变时间：[每个止损率对应一个元组com]}
                print 'profit_dict',profit_dict
                self.change_begin = self.account.tomorrow


account=Account(st.start,st.end,st.freq,st.universe_code,st.capital_base,st.benchmark,st.self_defined)  # 循环跑完所有参数
account.iniData()
st.initialize(account)
day_num=len(account.timeList)
rr = Recursive(account,period='month',train_count=1,test_count=1)  # recursive
for xx in rate_list:  # 对每一个rate进行回测整个时间段
    mq_in.push(xx)
num=0
while True:
    r=mq_r.pop()
    time.sleep(0.05)
    if r!=None:
        num+=1
        print r[0]
        rr.set_recursive(r[0])
        account.dynamic_record=cPickle.loads(r[1])
        # rr.get_performance()  # recursive
        rr.get_result()
        print account.dynamic_record['capital'],'\n',len(account.dynamic_record['capital'])
        account.dynamic_record['trade_time'] = []  # 一个参数跑完后，清空重置属性
        account.dynamic_record['cash'] = []
        account.dynamic_record["blotter"] = []
        account.dynamic_record['capital'] = []
        account.dynamic_record['IF'] = []
        account.dynamic_record['alpha_capital'] = []
        account.valid_secpos=pd.Series(0,account.all_universe)
        account.valid_secpos_price=pd.Series(0,account.all_universe)
        account.avgBuyprice=pd.Series(0,account.all_universe)
        account.buyPosition=pd.Series(0,account.all_universe)
    if num==len(rate_list):
        break

rate_dict = {}  # 找出训练的最优参数
for item in profit_dict:
    rate_dict[item] = None
    temp = []
    # print item,profit_dict[item]
    for it in profit_dict[item]:
        temp.append(it[-1])

    for it1 in profit_dict[item]:
        if max(temp) in it1:
            print it1
            rate_dict[item] = it1[0]

print 'rate_dict',rate_dict

# 递推使用前期的最佳参数
account1=Account(st.start,st.end,st.freq,st.universe_code,st.capital_base,st.benchmark,st.self_defined)
account1.iniData()
st.initialize(account1)
day_num=len(account1.timeList)
st.initialize(account1)
for n,i in enumerate(account1.timeList):
    account1.last_time=account1.current_time
    account1.current_time=i
    account1.days_counts=n+1
    if n<day_num-1:
        account1.tomorrow=account1.timeList[n+1]
    # account1.set_universe(account1.current_time)
    account1.order_temp=[]
    account1.dynamic_record['trade_time'].append(i)
    st.handle_data(account1)
    account1.dynamic_record['cash'].append(account1.cash)
    account1.dynamic_record["blotter"].append(account1.order_temp)
    account1.dynamic_record['capital'].append(account1.calculate_capital(i))
    account1.calculate_IF(i)#计算做空股指期货的动态收益
    account1.dynamic_record['alpha_capital'].append(account1.dynamic_record['capital'][-1] + account1.dynamic_record['IF'][-1])


    if account1.current_time in rate_dict:  # 在指定时间改变参数
        account1.f1 = rate_dict[account1.current_time]

d=account1.dynamic_record['capital']
x = list(account1.timeList)
fig1 = plt.figure('fig1')
plt.plot(x,d,label='%s ~ %s'%(rate_list[0],rate_list[-1]),color='green')
# plt.legend(loc = 0)
# plt.show()
# d1 = account1.dynamic_record['capital']

# account2=Account(st.start,st.end,st.freq,st.universe_code,st.capital_base)
# account2.iniData()
# st.initialize(account2)
# day_num=len(account2.timeList)
# st.initialize(account2)
# for n,i in enumerate(account2.timeList):
#     account2.last_time=account2.current_time
#     account2.current_time=i
#     account2.days_counts=n+1
#     if n<day_num-1:
#         account2.tomorrow=account2.timeList[n+1]
#     # account2.set_universe(account2.current_time)
#     account2.order_temp=[]
#     account2.dynamic_record['trade_time'].append(i)
#     st.handle_data(account2)
#     account2.dynamic_record['cash'].append(account2.cash)
#     account2.dynamic_record["blotter"].append(account2.order_temp)
#     account2.dynamic_record['capital'].append(account2.calculate_capital(i))
    # if account2.current_time in rate_dict:
    #     account2.f1 = rate_dict[account2.current_time]


d1= account1.dynamic_record['alpha_capital']
# d2 = account1.dynamic_record['IF']
# d = np.array(d1) + np.array(d2)
x = list(account1.timeList)
plt.plot(x,d1,label='alpha_capital',color='red')
plt.legend(loc = 0)

fig2 = plt.figure('fig2')
perf = mp.Performance(account1,plt)
perf.benchmark()
perf.dynamic_rate()
perf.calculate_ratio()
plt.legend(loc = 0)
#plt.show()

# print np.array(d1) - np.array(d2)