# coding=utf-8
__author__ = 'tan linchao'

import numpy as np
import pandas as pd
from my_test import *
from datetime import *
from ipyparallel import Client
import cPickle

# 止损率列表
# rate_list = np.linspace(0,0.5,11)
# rate_list = np.linspace(0,1,41)
rate_list = np.linspace(0,0.2,21)
# rate_list = [0.1,0.125]
c=Client('ipcontroller-client.json')
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
# 计算训练的日期区间和使用的日期区间
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
        # print 'out of bounds'
        break
print train_list
print use_list
profit = pd.DataFrame()
print profit

def compu(f):
    """
    分布式计算的要执行的函数
    :param f: 止损参数
    :return:
    """
    import copy,cPickle
    from my_test import *
    account1 = Account(f[1],f[2],st.freq,st.universe_code,st.capital_base,st.short_capital,st.benchmark,st.self_defined)
    account1.iniData()
    st.initialize(account1)
    #account1=ac#cPickle.loads(acc)
    account1.f1 = f[0]
    day_num=len(account1.timeList)
    for n1,i1 in enumerate(account1.timeList[:-1]):  # 计算到改变前一天
            account1.last_time = account1.current_time
            account1.current_time = i1
            account1.days_counts = n1+1
            if n1 < day_num-1:
                account1.tomorrow = account1.timeList[n1+1]
            account1.order_temp = []
            account1.dynamic_record['trade_time'].append(i1)
            st.handle_data(account1)
            account1.dynamic_record['cash'].append(account1.cash)
            account1.dynamic_record['blotter'].append(account1.order_temp)
            account1.dynamic_record['capital'].append(account1.calculate_capital(i1))
            # account1.calculate_IF(i1)  # 计算做空股指期货的动态收益
            account1.dynamic_record['IF'].append(account1.calculate_IF(i1))  # 计算做空股指期货的动态收益
            account1.dynamic_record['alpha_capital'].append(account1.dynamic_record['capital'][-1] + account1.dynamic_record['IF'][-1])
    return f[0],account1.dynamic_record,f


for it,item in enumerate(train_list):
    account1 = Account(item,use_list[it],st.freq,st.universe_code,st.capital_base,st.short_capital,st.benchmark,st.self_defined)
    account1.iniData()
    st.initialize(account1)
    #for i in c:
    #c[:].push(dict(start=item,end=use_list[it]))
    #c[:]['acc']=1#account1
    # rec.set_ratios_list(rate_list)
    i_rate = rec.get_ratio_list()
    in_put=[]
    for i in i_rate:
        in_put.append([i,item,use_list[it]])
    print in_put
    res=c[:].map_sync(compu,in_put)
    for i in res:
        account1.dynamic_record=i[1]
        # print i[1],'test'
        # values = i[1]['alpha_capital'][-1] - i[1]['alpha_capital'][0]
        # print i[1]['blotter']
        values = account1.dynamic_record['alpha_capital'][-1] - account1.dynamic_record['alpha_capital'][0]
        profit.loc[use_list[it],str(i[0])] = values
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

#     print use_list[it],profit.loc[use_list[it]].idxmax()
# print profit


for n,i in enumerate(account.timeList):
    account.last_time = account.current_time
    account.current_time = i
    account.days_counts = n+1
    if n < day_num-1:
        account.tomorrow = account.timeList[n+1]

    if account.current_time in use_list:  # 在日期之前改动止损率
        # print 'True to change rate'
        print profit.loc[account.current_time].idxmax()
        account.f1 = float(profit.loc[account.current_time].idxmax())

    account.order_temp = []
    account.dynamic_record['trade_time'].append(i)
    st.handle_data(account)
    account.dynamic_record['cash'].append(account.cash)
    account.dynamic_record['blotter'].append(account.order_temp)
    account.dynamic_record['capital'].append(account.calculate_capital(i))
    # account.calculate_IF(i)  # 计算做空股指期货的动态收益
    account.dynamic_record['IF'].append(account.calculate_IF(i))  # 计算做空股指期货的动态收益
    account.dynamic_record['alpha_capital'].append(account.dynamic_record['capital'][-1] + account.dynamic_record['IF'][-1])
    account.DR.loc[i,'cash'] = account.cash
    # account.DR.loc[i,'blotter'] = account.order_temp  # 不可存储列表
    account.DR.loc[i,'capital'] = account.dynamic_record['capital'][-1]
    account.DR.loc[i,'IF'] = account.dynamic_record['IF'][-1]
    account.DR.loc[i,'alpha_capital'] = account.dynamic_record['alpha_capital'][-1]


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
#plt.show()

