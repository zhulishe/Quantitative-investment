#coding=utf-8
__author__ = 'linchao'

from my_test import *
import random,memcache,mcQueue,cPickle,time
# account.iniData()
# st.initialize(account)
def empty(account):
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
def test(f):
    account=Account(f[0],f[1],st.freq,st.universe_code,st.capital_base,st.short_capital,st.benchmark,st.self_defined)
    account.iniData()
    st.initialize(account)
    account.f1=f[2]
    #account.f1=account.tt
    day_num=len(account.timeList)
    account.cash = account.capital_base
    print account.capital_base,account.cash
    for n,i in enumerate(account.timeList):
        account.last_time=account.current_time
        account.current_time=i
        account.days_counts=n+1
        if n<day_num-1:
            account.tomorrow=account.timeList[n+1]
        # account.set_universe(account.current_time)
        account.order_temp=[]
        account.dynamic_record['trade_time'].append(i)
        st.handle_data(account)
        # print account.SD
        account.dynamic_record['cash'].append(account.cash)
        account.dynamic_record["blotter"].append(account.order_temp)
        account.dynamic_record['capital'].append(account.calculate_capital(i))
        account.calculate_IF(i)#计算做空股指期货的动态收益
        account.dynamic_record['alpha_capital'].append(account.dynamic_record['capital'][-1] + account.dynamic_record['IF'][-1])
    return account.dynamic_record
mc=memcache.Client(['192.168.0.103:11211'])
mq_in=mcQueue.mcQueue(mc,'input')
mq_r=mcQueue.mcQueue(mc,'result')
mq_ins=mcQueue.mcQueue(mc,'instruction')
print "start"
con=False
while 1:
    temp=mq_ins.pop()
    if temp=='stop':
        break
    elif temp=='start':
        time.sleep(2)
        con=True
    elif temp=='test':
        time.sleep(2)
        continue
    time.sleep(0.2)
    f=mq_in.pop()
    if f!=None and con==True:
        #account.f1=f
        print "running",f
        r=test(f)
        mq_r.push([f[-1],cPickle.dumps(r)])
        #empty(account)
