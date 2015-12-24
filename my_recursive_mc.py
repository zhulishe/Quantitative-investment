# coding=utf-8
__author__ = 'tan linchao'

import numpy as np
import pandas as pd
from my_test import *
from datetime import *
from time import sleep
from ipyparallel import Client
import cPickle,memcache,mcQueue
mc=memcache.Client(['192.168.0.103:11211'])
mq_in=mcQueue.mcQueue(mc,'input')
mq_r=mcQueue.mcQueue(mc,'result')
# 止损率列表
# rate_list = np.linspace(0,0.5,11)
# rate_list = np.linspace(0,1,41)
rate_list = np.linspace(0,0.2,21)
# rate_list = [0.1,0.125]
basic_rate = [0,1]
#c=Client('ipcontroller-client.json')
class Recursive:
    def __init__(self,train_count=1,use_count=1):
        self.train_count = train_count
        self.use_count = use_count

    def set_ratios_list(self,ratio_list):
        self.ratios_list = ratio_list

    def get_ratio_list(self):
        try:
            return self.ratios_list
        except:
            return []

rec = Recursive(train_count=3,use_count=2)
rec.set_ratios_list(rate_list)

account=Account(st.start,st.end,st.freq,st.universe_code,st.capital_base,st.short_capital,st.benchmark,st.self_defined)
account.iniData()
st.initialize(account)
day_num=len(account.timeList)

time_list = account.timeList.values
start = time_list[0]
end = time_list[-1]
train_start_month = start.month + 1
use_start_month = start.month + rec.train_count + 1
train_mty = (train_start_month - 1)/12
use_mty = (use_start_month - 1)/12
train_start = account.getBeginOfMonth(start.year + train_mty, train_start_month - train_mty * 12)
use_start = account.getBeginOfMonth(start.year + use_mty, use_start_month - use_mty * 12)
train_list = []
use_list = []
while use_start <= end:
    train_list.append(train_start)
    use_list.append(use_start)
    train_start_month += rec.use_count
    use_start_month += rec.use_count
    train_mty = (train_start_month - 1)/12
    use_mty = (use_start_month - 1)/12
    try:
        train_start = account.getBeginOfMonth(start.year + train_mty, train_start_month - train_mty * 12)
        use_start = account.getBeginOfMonth(start.year + use_mty, use_start_month - use_mty * 12)
    except:
        print 'out of bounds'
        break
print train_list
print use_list
profit = pd.DataFrame(None,index=use_list,columns=basic_rate)
print profit


for it,item in enumerate(train_list):
    account1 = Account(item,use_list[it],st.freq,st.universe_code,st.capital_base,st.short_capital,st.benchmark,st.self_defined)
    account1.iniData()
    st.initialize(account1)
    #for i in c:
    #c[:].push(dict(start=item,end=use_list[it]))
    #c[:]['acc']=1#account1
    # rec.set_ratios_list(rate_list)
    i_rate = rec.get_ratio_list()
    for i in i_rate:
        mq_in.push([item,use_list[it],i])
    num=0
    while 1:
        if num==len(i_rate):
            break
        sleep(0.2)
        temp=mq_r.pop()
        if temp==None:
            continue
        num+=1
        account1.dynamic_record=cPickle.loads(temp[1])
        #print temp
        values = account1.dynamic_record['alpha_capital'][-1] - account1.dynamic_record['alpha_capital'][0]
        profit.loc[use_list[it],str(temp[0])] = values
        account1.dynamic_record['trade_time'] = []  # 一个参数跑完后，清空重置属性
        account1.dynamic_record['cash'] = []
        account1.dynamic_record["blotter"] = []
        account1.dynamic_record['capital'] = []
        account1.dynamic_record['IF'] = []
        account1.dynamic_record['alpha_capital'] = []
        account1.valid_secpos=pd.Series(0,account.all_universe)
        account1.valid_secpos_price=pd.Series(0,account.all_universe)
        account1.avgBuyprice=pd.Series(0,account.all_universe)
        account1.buyPosition=pd.Series(0,account.all_universe)

    print use_list[it],profit.loc[use_list[it]].idxmax()
print profit


for n,i in enumerate(account.timeList):
    account.last_time = account.current_time
    account.current_time = i
    account.days_counts = n+1
    if n < day_num-1:
        account.tomorrow = account.timeList[n+1]

    if account.current_time in use_list:  # 在日期之前改动止损率
        print 'True to change rate'
        print profit.loc[account.current_time].idxmax()
        account.f1 = float(profit.loc[account.current_time].idxmax())

    account.order_temp = []
    account.dynamic_record['trade_time'].append(i)
    st.handle_data(account)
    account.dynamic_record['cash'].append(account.cash)
    account.dynamic_record['blotter'].append(account.order_temp)
    account.dynamic_record['capital'].append(account.calculate_capital(i))
    account.calculate_IF(i)  # 计算做空股指期货的动态收益
    account.dynamic_record['alpha_capital'].append(account.dynamic_record['capital'][-1] + account.dynamic_record['IF'][-1])


d1 = account.dynamic_record['alpha_capital']
x = list(account.timeList)
plt.plot(x,d1,label='alpha_capital',color='red')
plt.legend(loc = 0)

fig2 = plt.figure('fig2')
perf = mp.Performance(account,plt)
perf.benchmark()
perf.dynamic_rate()
perf.calculate_ratio()
plt.legend(loc = 0)
plt.show()

