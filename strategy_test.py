#coding=utf-8
__author__ = 'linchao'
import my_test as mt
import pandas as pd
import numpy as np
import MySQLdb,datetime
import matplotlib.pyplot as plt
import my_performance as mp
from time import *
from ipyparallel import Client
import my_test
import cPickle
c=Client('ipcontroller-client.json')

start = datetime.date(2014, 1, 1)				# 回测起始时间
end  = datetime.date(2015, 4, 1)				# 回测结束时间
train_count=2
test_count=1
train_time_list=[]
test_time_list=[]
time_list=[]
head_list=[]
head=start.month-1
train_start=start.month+train_count-1
test_start=start.month+train_count+test_count-1
con=False
while 1:
    train_y=train_start/12
    test_y=test_start/12
    head_y=head/12
    head_temp=datetime.date(start.year+head_y,head%12+1,1)
    test=datetime.date(start.year+test_y,test_start%12+1,1)
    train=datetime.date(start.year+train_y,train_start%12+1,1)
    if test>=end:
        test=end
        con=True
    head_list.append(head_temp)
    train_time_list.append(train)
    test_time_list.append(test)
    time_list.append([head_temp,train,test])
    if con:
        break
    head=head+test_count
    train_start=train_start+test_count
    test_start=test_start+test_count
print time_list
c[:].push(dict(universe_code = '000300.SH',benchmark = '000300.sh',capital_base = 1e7,
               short_capital = 1e7,freq = 'd',self_defined = False))
def initialize(account):					# 初始化虚拟账户状态
    # account.fund = universe[0]
    account.SD = 0.1
    account.window_short = 20
    account.window_long = 120
    account.f1=0.1
    account.f2=0.2
    account.f3=0.3
    #account.hangup=[]
def stop_loss(account):
    for i in account.valid_secpos.index[account.valid_secpos>0]:
        todayHigh=account.getTodayHigh(i)
        todayLow=account.getTodayLow(i)
        todayOpen=account.getTodayOpen(i)
        stopprice=account.avgBuyprice[i]*(1-account.f1)
        upprice=account.avgBuyprice[i]*(1+account.f2)
        old_buy=account.buyPosition[i]
        if old_buy<account.days_counts:
            HighSinceBuy=account.getDailyHistory(i,account.days_counts-old_buy)['high'].max()
            if stopprice>=todayLow:
                if account.isOk2order(i):
                    #print stopprice,todayHigh,todayLow
                    if todayOpen>=stopprice:
                        account.order_to(i,0,stopprice)
                    else:
                        account.order_to(i,0,todayOpen)
            if HighSinceBuy>=upprice and todayLow<HighSinceBuy*(1-account.f3):
                if account.isOk2order(i):
                    account.order_to(i,0,stopprice)
def handle_data(account):
    #print account.universe,account.current_time
    if account.days_counts==1:
        return
    #print account.getIfData('Open',account.current_time)
    #print account.getBeginOfMonth(2015,5)


    #print account.current_time
    #if account.current_time.month!=account.last_time.month:
    if account.self_defined:
        latest_universe = account.universe_code
    else:
        latest_universe = account.get_latest_universe()
    num=account.cash/len(latest_universe)
    for i in latest_universe:
        if account.isOk2order(i):
            todayOpen=account.getTodayOpen(i)
            if todayOpen>account.getDailyHistory(i,1)['close'].values[-1]:
                account.order_to(i,int(num/todayOpen/100)*100,todayOpen)
    stop_loss(account)
c[:].push(dict(initialize=initialize,stop_loss=stop_loss,handle_data=handle_data))
def test(time_args):
    from my_test import *
    account=Account(time_args[1],time_args[2],freq,universe_code,capital_base,short_capital,benchmark,self_defined)
    account.iniData()
    initialize(account)
    day_num=len(account.timeList)
    for n,i in enumerate(account.timeList):
        account.last_time=account.current_time
        account.current_time=i
        account.days_counts=n+1
        if n<day_num-1:
            account.tomorrow=account.timeList[n+1]
        # account.set_universe(account.current_time)
        account.order_temp=[]
        account.dynamic_record['trade_time'].append(i)
        handle_data(account)
        # print account.SD
        account.calculate_monthly_profit()
        account.dynamic_record['cash'].append(account.cash)
        account.dynamic_record["blotter"].append(account.order_temp)
        account.dynamic_record['capital'].append(account.calculate_capital(i))
        account.dynamic_record['IF'].append(account.calculate_IF(i))  # 计算做空股指期货的动态收益
        account.dynamic_record['alpha_capital'].append(account.dynamic_record['capital'][-1] + account.dynamic_record['IF'][-1])
        account.DR.loc[i,'cash'] = account.cash
        # account.DR.loc[i,'blotter'] = cPickle(account.order_temp)  # 不可存储列表
        account.DR.loc[i,'capital'] = account.dynamic_record['capital'][-1]
        account.DR.loc[i,'IF'] = account.dynamic_record['IF'][-1]
        account.DR.loc[i,'alpha_capital'] = account.dynamic_record['alpha_capital'][-1]
    return account.dynamic_record,time_args[2],list(account.timeList.values)

r=c[:].map_sync(test,time_list)
r.sort(key=lambda x:x[1])
def calculate(data):
    xx=[]
    temp=[]
    for i in data:
        xx.append(i[-1]/i[0])
    x=np.array(xx)
    xx=x.cumprod()
    for i in xrange(len(data)):
        temp.append(np.array(data[i])*xx[i])
    all_capital=[]
    for i in temp:
        all_capital+=list(i)
    return  all_capital
temp=[]
temp1=[]
temp2=[]
for i in r:
    temp.append(i[0]['capital'])
    temp1.append(i[0]['IF'])
    temp2.append(i[0]['blotter'])
temp=calculate(temp)
temp1=calculate(temp1)
temp2=calculate(temp2)
time_l=[]
for i in r:
    time_l+=i[2]
from my_test import *
# account=Account(time_list[0][1],end,'d','000300.SH',1e7,1e7,'000010.sh',False)
account=Account(time_list[0][1],end,'d','000300.SH',1e7,1e7,['000002.sz','000001.sz','000004.sz'],True)
# universe_code = ['000002.sz','000001.sz','000004.sz']  # 股票池
# self_defined = True
account.iniData()

account.days = len(account.timeList)
account.dynamic_record['capital'] = temp
account.dynamic_record['IF'] = temp1
account.dynamic_record['alpha_capital'] = np.array(temp) + np.array(temp1)

d=account.dynamic_record['capital']
IF = account.dynamic_record['IF']
x=xrange(len(d))
x = list(account.timeList)
fig1 = plt.figure('fig1')
plt.title('#'+strftime("%Y-%m-%d %H:%M:%S")+'# '+st.start.strftime("%Y-%m-%d")+' ~ '+st.end.strftime("%Y-%m-%d")+'#capital')
plt.xlabel('time')
plt.ylabel('capital')
plt.plot(x,d,label='dynamic_capital')
plt.plot(x,IF,label='IF')
plt.legend(loc = 0)
# plt.savefig('./pictures/'+st.start.strftime("%Y-%m-%d")+'new.png',dpi = 500)

fig2 = plt.figure('fig2')
perf = mp.Performance(account,plt)
perf.benchmark()
perf.dynamic_rate()
perf.calculate_ratio()
plt.title(st.start.strftime("%Y-%m-%d")+'~'+st.end.strftime("%Y-%m-%d")+' profit')
plt.xlabel('time')
plt.ylabel('profit_percent')
plt.legend(loc=0)

fig3 = plt.figure('fig3')
perf.alpha_graph()
plt.legend(loc=0)

plt.show()
