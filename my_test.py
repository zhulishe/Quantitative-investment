#coding=utf-8
__author__ = 'linchao'
import collections
import my_strategy_five_2 as st #将策略作为一个模块导入
import pandas as pd
import numpy as np
import MySQLdb,datetime
import matplotlib.pyplot as plt
import my_performance as mp #这是画图的模块
# import recursive
from time import *
import cPickle

class dataImport():  #一个mysql的connect实例
    def __init__(self):
        #self.conn=MySQLdb.connect(host="5594d9822696a.gz.cdb.myqcloud.com",user="cdb_outerroot",port=16941,passwd="a8674925",db="new_stock",charset="utf8")
        self.conn=MySQLdb.connect(host="192.168.0.105",user="root",passwd="fit123456",db="new_stock",charset="utf8")
        self.cursor=self.conn.cursor()
    def __del__(self):
        self.cursor.close()
        self.conn.close()

def datetime2str(datetime):
    return datetime.strftime("%Y-%m-%d %H:%M")
def date2str(date):
    return date.strftime("%Y-%m-%d")
class order():
    """
     记录下单记录的类，每个实例表示一个下单记录。该结构在后面使用过程中觉得不太方便，可替换成pandas的DataFrame或其他结构
    """
    def __init__(self,time,code,num,price):
        self.order_time=time
        self.code=code
        self.order_num=num
        self.price=price
        self.profit = 0
class Account():
    """
    这是整个系统中最关键的类，它的变量存储着基本历史数据和交易信息，也有许多数据接口和下单函数
    """
    def __init__(self, start, end, freq, universe_code, capital_base, short_capital, benchmark_code, self_defined):
        """
        初始化函数，可参考下面的调用用法
        :param start: 下面几个参数都是策略my_strategy里面的关键参数
        :param end:
        :param freq:
        :param universe_code:
        :param capital_base:
        :param short_capital:
        :param benchmark_code:
        :param self_defined:
        :return:
        """
        self.start = start
        self.end = end
        self.universe_temp = []
        self.all_universe = []
        self.self_defined = self_defined
        self.universe_code = universe_code
        self.universe = []#当天可以交易的股票池
        self.capital_base = capital_base
        self.short_capital = short_capital
        self.freq = freq
        self.cash = capital_base
        self.short_cash = short_capital
        # self.commisson=commisson
        self.IF_entryPrice = 0 #记录做空股指期货的价格
        self.data = {} #保存所有股票的每日历史数据，开盘价，最高价等信息，为字典，key为股票代码，值为该股票的历史数据，格式为DataFrame
        self.connect=dataImport() #mysql连接实例
        self.suspend_stock=[] #停牌的股票列表
        self.ST_stock=[] #特殊处理的股票列表
        self.dynamic_record={} #股票的动态记录相关信息，每个交易日末添加新记录
        self.dynamic_record['trade_time']=[]#交易时间
        self.dynamic_record['cash']=[]#可用资金
        self.dynamic_record['capital']=[]#总价值
        self.dynamic_record['IF']=[] #做空股指期货的动态资产
        self.dynamic_record['blotter']=[]#下单记录
        self.dynamic_record['alpha_capital'] = [] #股票-做空股指期货的动态资产
        self.DR = pd.DataFrame()  # 动态记录
        self.monthly_profit = {}  # 月收益
        self.static_profit={} #静态收益，即是每次卖出股票才更新
        self.handlingFeeRate=0.0003#手续率
        self.stampTaxRate=0.001#印花税率
        self.valid_secpos={} #为一个字典,key为股票代码,value是对应手头上持有的该股票的数量
        self.order_temp = [] #临时存储当天交易日的所有交易记录，即是许多order实例
        self.avgBuyprice=None #平均买入价格，在下面的iniData函数里初始化
        self.buyPosition={} #为一个字典，key为股票代码，value是对应最近购买该股票过去的时间单位
        self.current_time=None #当前交易时间
        self.days = 0
        self.benchmark = None
        # self.benchmark_code = '000300.sh'
        self.benchmark_code = benchmark_code  # 参考基准
        self.max_value = []
        self.total_fees = 0
        self.hold_IF=0 #手头上持有的股指期货的手数
        self.dataIF=None #股指期货的历史数据
        self.is_short = False
        self.lot_changed_rate = collections.OrderedDict() #记录每个月的换手率，其实这个指标应该是放到performance那里的
        self.real_deal_days = 0 #记录实际下单的数数
        self.stock_monthly_hold_profit = collections.OrderedDict() # 记录每个月股票持仓以及盈利

    def __del__(self):
        pass
    def iniData(self):
        """
        初始化一些相关数据
        :return:
        """
        if self.freq=='d':
            start=date2str(self.start)
            end=date2str(self.end)
        sql="select DATE(date) from `history_date` where date>='%s' and date<'%s'"%(start,end)
        self.timeList=pd.read_sql(sql,self.connect.conn)['DATE(date)'] #将策略给定的起始时间内的所有历史日交易日期读到一个数组里
        self.days = self.timeList.count()
        ###############################################################
        if self.self_defined: #判断是否为自定义股票池
            self.all_universe = self.universe_code
        elif self.universe_code == 'A':
            sql = "select * from all_a_stock_codes"
            temp = pd.read_sql(sql,self.connect.conn)
            self.all_universe = temp['code']
            sql2 = "select * from my_stock_index WHERE group_code = '%s'and date_time < '%s' "%(self.universe_code,end)
            self.universe_temp = pd.read_sql(sql2,self.connect.conn)
        else:
            sql1 = "select * from my_stock_index WHERE group_code = '%s'and date_time < '%s' "%(self.universe_code,end)
            self.universe_temp = pd.read_sql(sql1,self.connect.conn)
            universe_set = set([])
            for item in self.universe_temp['content']:
                universe_set.update(item.split(','))
            print "股票池的长度", len(universe_set)
            self.all_universe = list(universe_set)
        ###############################################################
        # sql = "select close FROM `%s` WHERE date_time BETWEEN '%s' and '%s' "%(self.benchmark_code,self.start,self.end)
        # self.connect.cursor.execute(sql)
        # self.benchmark = self.connect.cursor.fetchall()
        # 修改benchmark数据格式
        sql = "select * FROM `%s` WHERE date_time >= '%s' and date_time < '%s' "%(self.benchmark_code.lower(),self.start,self.end)
        self.benchmark = pd.read_sql(sql,self.connect.conn)
        sql="select DATE(date_time),code from `suspend_stock` where date_time>='%s' and date_time<'%s'"%(start,end)
        #print sql
        self.suspend_stock=pd.read_sql(sql,self.connect.conn) #读取停牌股票列表
        sql="select DATE(date_time),code from `st_stock` where date_time>='%s' and date_time<'%s'"%(start,end)
        self.ST_stock=pd.read_sql(sql,self.connect.conn) #读取特殊处理股票列表，他跟停牌的列表一起在判断是否可以交易起作用
        for i in self.all_universe: #获取相关股票的历史数据
            sql="select * from `%s` where date_time>='%s' and date_time<'%s' order by date_time"%(i.lower(),start,end)
            d=pd.read_sql(sql,self.connect.conn)
            self.data[i]=d[d['open'].notnull()]
        #for i in self.all_universe:
        sql="select Date,Open,High,Low,Close from `if000_d1` where Date>='%s' and Date<'%s' order by Date"%(start,end) #获取if000股指期货的历史数据
        #print sql
        self.dataIF=pd.read_sql(sql,self.connect.conn)
        if self.start<datetime.date(2010,4,16):
            sql="select date_time as Date,open as Open,high as High,low as Low,close as Close " \
                "from `000300.sh` where date_time<'2010-04-16' and date_time>='%s'"%start
            temp=pd.read_sql(sql,self.connect.conn)
            self.dataIF=temp.append(self.dataIF)
            # account.dataIF['Open'][-1]
        #####初始化相关变量#####
        self.valid_secpos=pd.Series(0,self.all_universe)
        self.valid_secpos_price=pd.Series(0.0,self.all_universe)
        self.avgBuyprice=pd.Series(0.0,self.all_universe)
        self.buyPosition=pd.Series(0,self.all_universe)
        for i in self.all_universe:
            self.static_profit[i]=0
        print "加载数据成功"

    def set_universe(self,time): #该函数已弃用
        """
        type:内部函数，得到指定时间内的可交易的股票列表，并存在universe中
        inout:日期
        return:None
        """
        l=self.suspend_stock[self.suspend_stock['DATE(date_time)']==self.current_time]['code'].values
        self.universe=[]
        for i in self.all_universe:
            if i.upper() not in l and  not self.is_upORdownLimit(i):
                self.universe.append(i)

    def isOk2order(self,code): #判断该股票是否可以交易
        code = code.upper()
        l = self.suspend_stock[self.suspend_stock['DATE(date_time)'] == self.current_time]['code'].values #先判断是否停牌
        if code in l:
            return False
        elif self.is_upORdownLimit(code): #再判断是否涨停跌停
            return False
        else:
            return True

    def is_upORdownLimit(self,code):
        code = code.upper()
        """
        判断涨停跌停
        :param code:
        :return Fool:
        """
        yesterday=self.getDailyHistory(code,1)['open'].values
        today_open=self.getTodayOpen(code)
        if not today_open:  # 2015-8-28 策略调用数据问题修改
            #print "没open数据,但是是在涨跌停中判断"
            return True
        if len(yesterday)==0:
            #print "当前为第一天无法取得前一天的数据"
            return False

        l=self.ST_stock[self.ST_stock['DATE(date_time)']==self.current_time]['code'].values
        #根据是否为特殊处理的股票有不同的别判断方式
        if code in l:
            if today_open>yesterday[-1]*1.049:
                return True
            elif today_open<yesterday[-1]*0.95:
                return True
            else:
                return False
        else:
            if today_open>yesterday[-1]*1.098:
                return True
            elif today_open<yesterday[-1]*0.9:
                return True
            else:
                return False

    def getTodayOpen(self,code):
        code = code.upper()
        """
        input:股票代码,str类型
        return:该股票的当天开盘价，float类型
        """
        temp=self.data[code]
        try:
            return temp[temp['date_time']<=self.current_time].tail(1)['open'].values[-1]
        except IndexError,e:
            # print e,code+"还未上市或者数据库里在这时间段没数据"
            # exit()
            return None  # 2015-8-28 策略调用数据问题修改

    def getTodayClose(self,code):
        code = code.upper()
        """
        input:股票代码,str类型
        return:该股票的当天开盘价，float类型
        """
        temp=self.data[code]
        try:
            return temp[temp['date_time']<=self.current_time].tail(1)['close'].values[-1]
        except IndexError,e:
            # print e,code+"还未上市或者数据库里在这时间段没数据"
            # exit()
            return None  # 2015-8-28 策略调用数据问题修改

    def getTodayHigh(self,code):
        code = code.upper()
        """
        :param code:股票代码，str
        :return:该股票当天的最高价，float，注意这是未来数据，一般用来止损
        """
        temp=self.data[code]
        try:
            return temp[temp['date_time']<=self.current_time].tail(1)['high'].values[-1]
        except IndexError,e:
            # print e,code+"还未上市或者数据库里在这时间段没数据"
            # exit()
            return None

    def getTodayLow(self,code):
        code = code.upper()
        """
        :param code:股票代码，str
        :return:该股票当天的最低价，float，注意这是未来数据，一般用来止损
        """
        temp=self.data[code]
        try:
            return temp[temp['date_time']<=self.current_time].tail(1)['low'].values[-1]
        except IndexError,e:
            # print e,code+"还未上市或者数据库里在这时间段没数据"
            # exit()
            return None

    def getDailyHistory(self,code,length):
        code = code.upper()
        """
        input:股票代码，str类型；天数,int
        return:该股票在过去length天的所有历史数据，DataFrame类型
        """
        temp=self.data[code]
        return temp[temp['date_time']<self.current_time].tail(length)

    def getIfData(self,type,t):
        return self.dataIF[self.dataIF['Date']<=t].tail(1)[type].values[-1]

    def commission(self,firstCode,price,status=1,number=0):
        """
        计算交易费用
        :param firstCode: 股票代码的第一个数字
        :param price: 当前价格
        :param status: 买卖状态
        :param number: 买卖数量
        :return:买卖费用
        """
        fee = 0
        if firstCode==6:#上海股票
            scripFee = number/1000 + 1#过户费
            handlingFee = price * number *self.handlingFeeRate#手续费
            if handlingFee < 5:
                handlingFee = 5
            fee = handlingFee + scripFee
            if status == -1:
                stampTax = price * number * self.stampTaxRate
                fee += stampTax

        elif firstCode==0 or firstCode==3:#深圳股票
            handlingFee = price * number *self.handlingFeeRate#手续费
            fee = handlingFee
            if status == -1:
                stampTax = price * number * self.stampTaxRate
                fee += stampTax

        elif firstCode==5:#沪基金
            scripFee = number/1000 + 1#过户费
            handlingFee = price * number *self.handlingFeeRate#手续费
            if handlingFee < 5:
                handlingFee = 5
            fee = handlingFee + scripFee

        elif firstCode==4:#深基金
            handlingFee = price * number *self.handlingFeeRate#手续费
            fee = handlingFee

        return fee

    def calculate_capital(self,time):
        """
        调用时计算给定时间的动态资产，就是将所有股票持有手数乘以当天的收盘价之和
        :param time: 回测的时间
        :return:动态资产
        """
        for i in self.all_universe:
            if self.valid_secpos[i] == 0:
                self.valid_secpos_price[i] = 0
            else:
                temp=self.data[i]

                self.valid_secpos_price[i]=temp[temp['date_time']==self.current_time]['close'].values[0]
        temp=self.valid_secpos_price*self.valid_secpos
        return temp.sum()+self.cash

    def calculate_IF(self,time):
        """
        计算股指期货的资产
        :param time:
        :return:
        """
        try:
            if self.hold_IF==0:
                IF_temp=0
            else:
                IF_temp=(self.IF_entryPrice+self.IF_entryPrice*0.15-self.dataIF[self.dataIF['Date']==self.current_time]['Close'].values[0])*300*self.hold_IF
        except IndexError,e:
            print self.current_time,"IF数据缺失！"
            exit()
        # self.dynamic_record['IF'].append(IF_temp + self.short_cash)
        return IF_temp + self.short_cash

    def EntryIF(self,money):
        self.is_short = True
        """
        做空股指期货
        :param money:用来做空的钱
        :return:
        """
        Open=self.dataIF[self.dataIF['Date']==self.current_time]['Open'].values[0]
        self.IF_entryPrice=Open
        lot=int(money/(Open*300.0))
        fee=Open*300*6/100000.0
        self.short_cash -= lot*Open*300.0*0.15+fee
        # self.cash-=lot*Open*300.0*0.15-fee
        self.hold_IF+=lot

    def EmptyIF(self,lot):
        self.is_short = True
        """
        平几手股指期货
        :param lot:几手
        :return:
        """
        Open=self.dataIF[self.dataIF['Date']==self.current_time]['Open'].values[0]
        fee=Open*300*6/100000.0
        self.short_cash += lot*(self.IF_entryPrice-Open+self.IF_entryPrice*0.15)*300.0-fee
        # self.cash+=lot*Open*300.0*0.15+fee
        self.hold_IF-=lot

    def order(self,code,num,price):
        """
        下单函数,买多少手
        :param code: 股票代码
        :param num: 手数
        :return:
        """
        #todayOpen=self.getTodayOpen(code)
        old_num=self.valid_secpos[code]
        #print old_num*self.avgBuyprice[code],'  ',price*num,'  ',old_num+num
        new_order=order(self.current_time,code,num,price)
        self.order_temp.append(new_order)
        # .append(new_order)
        self.valid_secpos[code] += num
        if num >0:
            status = 1
        else:
            status = -1
        fee = self.commission(int(code[0]),price,status,abs(num))
        if num<0:
            self.static_profit[code]+=abs(num)*(price-self.avgBuyprice[code])#计算每只股票的静态收益
            #print self.avgBuyprice[code],price,num
        #计算股票持仓的平均价格
        if old_num+num==0:
            self.avgBuyprice[code]=0#空仓清零
        elif num>0:
            self.avgBuyprice[code]=(old_num*self.avgBuyprice[code]+price*num)/float(old_num+num)#求平均
            #print self.avgBuyprice[code],'c'
        self.cash = self.cash - num * price - fee
        self.buyPosition[code]=self.days_counts

    def order_to(self,code,num,price):
        code = code.upper()
        """
        下单函数，买或者卖到num手
        :param code:
        :param num:
        :return:
        """
        current_num=self.valid_secpos[code]
        if num==current_num or num<0:
            return
        self.order(code,num-current_num,price)

    def isBeginOfMonth(self,t):
        """
        判断是否未月的第一天
        :param t,类型datetime。date:
        :return:
        """
        temp=self.timeList[self.timeList<t].tail(1).values[0]
        #print temp.values[0]
        if temp.month!=t.month:
            return True
        else:
            return False

    def getBeginOfMonth(self,y,m):
        """
        得到某年某月的第一个交易日
        :param y: 年，int
        :param m: 月，int
        :return:datetime
        """
        t=datetime.date(y,m,1)
        return self.timeList[self.timeList>=t].head(1).values[0]

    def get_latest_universe(self):
        """
        更新股票池
        :return: 最新的股票代码对应的股票池
        """
        if self.universe_code == 'A':
            # return self.all_universe
            latest_universe = self.universe_temp[(self.universe_temp['date_time'] == self.current_time)
                                                 &(self.universe_temp['group_code'] == '%s'%self.universe_code.upper())].tail(1)['content'].values[0].split(',')
        else:
            latest_universe = self.universe_temp[(self.universe_temp['date_time'] <= self.current_time)
                                                &(self.universe_temp['group_code'] == '%s'%self.universe_code.upper())].tail(1)['content'].values[0].split(',')
        return latest_universe

    def calculate_monthly_profit(self):
        """
        计算月收益
        :return:
        """
        if self.current_time is None or self.last_time is None:
            return
        if self.current_time.month != self.last_time.month:
            y = self.last_time.year
            m = self.last_time.month
            last_begin = self.getBeginOfMonth(y,m)
            last_end = self.last_time
            last_all_profit = self.DR[last_begin:last_end]['capital']
            last = self.last_time.strftime('%Y-%m')
            self.monthly_profit[last] = last_all_profit[-1] - last_all_profit[0]
            self.monthly_profit['last_month'] = last_all_profit[-1] - last_all_profit[0]

    def calculate_stock_monthly_hold_profit(self):
        """
        记录每个月的股票的持股，收益情况
        """ 
        if self.current_time is None or self.last_time is None:
            return
        if self.current_time.month != self.last_time.month:
            profit = []
            time = [self.current_time] * len(self.valid_secpos[self.valid_secpos>0])
            for index in self.valid_secpos[self.valid_secpos>0].index:
                profit.append(self.valid_secpos[index]*(self.valid_secpos_price[index]-self.avgBuyprice[index]))
            temp = np.array([time, self.valid_secpos[self.valid_secpos>0].index.tolist(), self.valid_secpos[self.valid_secpos>0].tolist(), profit])
            column_name = [u'time ', u'index ', u'hold_lot ', u'profit ']
            self.stock_monthly_hold_profit[self.current_time] = pd.DataFrame(temp.T, columns=column_name)

    def not_enough_cash(self, stock_id, lot_te_be_dealed, open_price):
        """
        判断当前的cash可否支持这次股票交易
        """
        current_num = self.valid_secpos[stock_id]
        num = lot_te_be_dealed - current_num
        if num == 0:
            return False
        if num >0:
            status = 1
        else:
            status = -1
        fee = self.commission(int(stock_id[0]), open_price, status, abs(num))
        if num > 0 and self.cash > (num * open_price + fee): #买
            return False
        elif num < 0 and self.cash > fee: #卖
            return False
        return True


def get_earning_rate(capital_record):
    """
    计算收益率的函数
    """
    earning_rate = []
    for index, capital in enumerate(capital_record):
        if index == 0:
            earning_rate.append(0)
        else:
            earning_rate.append(capital / capital_record[0] - 1)
    return earning_rate

#####################以下为系统运行时基本操作#############################
if __name__ == '__main__':
    #将策略的一些信息用于初始化Account
    account=Account(st.start,st.end,st.freq,st.universe_code,st.capital_base,st.short_capital,st.benchmark,st.self_defined)
    #获取基本数据
    account.iniData()
    #在account里面声明用户自定义变量
    st.initialize(account)
    #总的交易日天数
    day_num=len(account.timeList)
    # rr = recursive.Recursive(account)  # recursive
    # rr.set_recursive(0.01)  # recursive set values

    for n,i in enumerate(account.timeList): #循环交易日，模拟交易
        account.last_time=account.current_time #注：第一天last_time为None
        account.current_time = i
        account.days_counts=n+1 #交易日计数器
        if n<day_num-1: #获取下一个交易日的日期
            account.tomorrow=account.timeList[n+1]
        # account.set_universe(account.current_time)
        account.order_temp=[] #新建临时保存今天交易记录的列表
        account.dynamic_record['trade_time'].append(i) #在动态记录里面添加今天的日期
        st.handle_data(account) #调用用于策略
        # print account.SD
        account.calculate_monthly_profit() #计算月收益
        account.dynamic_record['cash'].append(account.cash)
        account.dynamic_record["blotter"].append(account.order_temp)
        account.dynamic_record['capital'].append(account.calculate_capital(i))
        account.dynamic_record['IF'].append(account.calculate_IF(i))  # 计算做空股指期货的动态收益
        account.dynamic_record['alpha_capital'].append(account.dynamic_record['capital'][-1] + account.dynamic_record['IF'][-1])
        account.DR.loc[i,'cash'] = account.cash
        account.DR.loc[i,'blotter'] = cPickle.dumps(account.order_temp)  # 不可存储列表,可以序列化
        account.DR.loc[i,'capital'] = account.dynamic_record['capital'][-1]
        account.DR.loc[i,'IF'] = account.dynamic_record['IF'][-1]
        account.DR.loc[i,'alpha_capital'] = account.dynamic_record['alpha_capital'][-1]
        #记录每个月的股票的持股，收益情况
        account.calculate_stock_monthly_hold_profit()

        # rr.get_performance()  # recursive
    # print 'monthly',account.monthly_profit
    #整个策略跑完后输出所有交易记录，这一段可以删除
    for i in account.dynamic_record['blotter']:
        for j in i:
            print j.order_time,j.order_num,j.code,j.price
    ###############以下为计算指标，还有画图
    d=account.dynamic_record['capital']
    IF = account.dynamic_record['IF']
    #计算股票的收益率
    stock_rate = get_earning_rate(d)
    #计算股指期货的收益率
    IF_rate = get_earning_rate(account.dataIF['Close'].tolist())
    #alpha收益率
    stock_IF_rate = [(_s - _i) for _s, _i in zip(stock_rate, IF_rate)] 
    #把alpha收益率添加到account类的属性中
    account.alpha_rate = zip(account.timeList.tolist(), stock_IF_rate)
    #把stock收益率添加到account类的属性中
    account.stock_rate = zip(account.timeList.tolist(), stock_rate)
    # x=xrange(len(d))
    x = account.timeList.tolist()
    #fig1 = plt.figure('fig1')
    #plt.title('#'+strftime("%Y-%m-%d %H:%M:%S")+'# '+st.start.strftime("%Y-%m-%d")+' ~ '+st.end.strftime("%Y-%m-%d")+'#capital')
    #plt.xlabel('time')
    #plt.ylabel('capital')
    #plt.plot(x, d, label='dynamic_capital')
    #plt.plot(x, IF, label='IF')
    #plt.legend(loc = 0)
    # plt.savefig('./pictures/'+st.start.strftime("%Y-%m-%d")+'new.png',dpi = 500)

    fig2 = plt.figure('fig4')
    plt.title('stock, IF and alpha')
    plt.xlabel('time')
    plt.ylabel('capital')
    plt.plot(x, stock_rate, label='stock rate')
    plt.plot(x, IF_rate, label='IF rate')
    plt.plot(x, stock_IF_rate, label='alpha')
    plt.legend(loc=0)

    #fig3 = plt.figure('fig2')
    perf = mp.Performance(account,plt)
    perf.benchmark()
    perf.dynamic_rate()
    perf.calculate_ratio()
    #plt.title(st.start.strftime("%Y-%m-%d")+'~'+st.end.strftime("%Y-%m-%d")+' profit')
    #plt.xlabel('time')
    #plt.ylabel('profit_percent')
    #plt.legend(loc=0)

    #fig4 = plt.figure('fig3')
    #perf.alpha_graph()
    #plt.legend(loc=0)

    plt.show()

