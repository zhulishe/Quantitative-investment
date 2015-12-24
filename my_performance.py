# coding=utf-8
__author__ = 'tan'
import datetime
import numpy as np
import pandas as pd
import pickle
# import matplotlib.pyplot as plt


class Performance:
    def __init__(self,account,plt):
        self.account = account
        try:
            self.days = account.days
            self.account_dynamic_record = account.dynamic_record
            self.account_benchmark = account.benchmark
        except:
            self.days = None
            self.account_dynamic_record = None
            self.account_benchmark = None
        self.plt = plt

    def dynamic_rate(self):
        dynamic = self.account_dynamic_record['capital'] #股票
        pre_dynamic = self.pre_list(dynamic)
        pre_dynamic[0] = self.account.capital_base
        if dynamic[0] == 0:
            rate = [x for x in dynamic]
        else:
            rate = [x/(dynamic[0]*1.0) - 1 for x in dynamic if dynamic[0] != 0]
            temp = np.array(dynamic)/np.array(pre_dynamic)
            new_rate = [(x-1) for x in temp]
            self.account_profit = np.array(dynamic) - np.array(pre_dynamic)
        self.d_rate = rate
        x = list(self.account.timeList)
        self.daily_strategy_profit_rate = new_rate
        #self.plt.plot(x,rate,label='strategy_ratio')

    def alpha_graph(self):
        x = list(self.account.timeList)
        if self.account.is_short:
            alpha_capital = self.account_dynamic_record['alpha_capital']
            alpha_rate = [y/alpha_capital[0] - 1 for y in alpha_capital]
            self.plt.plot(x,alpha_rate,label='alpha_ratio')
        else:
            self.plt.plot(x,self.d_rate,label='alpha_ratio')

    def benchmark(self):
        close_data = self.account_benchmark['close'].values
        # print close_data
        # new_close = np.array(close_data).reshape(len(close_data))
        new_close = np.array(close_data)
        # print 'new_close',new_close
        # x = xrange(len(new_close))
        x = list(self.account.timeList)
        bench_rate = new_close/(new_close[0]*1.0) - 1  # 自开始日起的收益率
        # print x,bench_rate
        # self.daily_benchmark_profit_rate = bench_rate
        pre_close = self.pre_list(new_close)
        temp = new_close/pre_close
        new_bench_rate = [(z-1) for z in temp]
        self.bench_profit = new_bench_rate * np.array(pre_close)
        self.daily_benchmark_profit_rate = new_bench_rate  # 日收益率
        #self.plt.plot(x,bench_rate,label='benchmark_ratio')
        # ppp = new_close/new_close[0] * self.account.capital_base
        # self.plt.plot(x,ppp,label='HS300')

    def calculate_ratio(self):
        """
        计算指标
        :return: 0
        """
        days = self.days
        annualized_return_ratio = sum(self.daily_strategy_profit_rate)/days * 250 #年化收益率,股票
        benchmark_returns_ratio = sum(self.daily_benchmark_profit_rate)/days * 250#基准年化收益率
        alpha_annualized_return_ratio = self.account.alpha_rate[-1][1] / days * 250 #alpha的年化收益率
        # print 'annualized_return_ratio',annualized_return_ratio,'\nbenchmark_returns_ratio',benchmark_returns_ratio
        covs = np.cov([self.daily_strategy_profit_rate,self.daily_benchmark_profit_rate])
        # print covs
        cov_s_b = covs[0][1]
        sigma_b = covs[1][1]
        beta = cov_s_b/sigma_b  # beta值
        # free_risk_returns = self.account.capital_base * (0.030)  # 策略无风险收益
        start_free_risk_ratio = 0.030  # 策略无风险收益
        # print self.account.capital_base * annualized_return_ratio,sum(self.account_profit)
        # 年化收益（率） 并不一定等于 年收益（率）
        # annualized_returns = self.account.capital_base * annualized_return_ratio
        # benchmark_returns = self.account.capital_base * benchmark_returns_ratio
        alpha = annualized_return_ratio - 0.030 - beta * (benchmark_returns_ratio - 0.030)  # alpha值
        # alpha = annualized_returns - free_risk_returns - beta * (benchmark_returns - free_risk_returns)  # alpha值
        # print 'alpha',alpha,'beta',beta

        volatility = np.sqrt(250 * covs[0][0])
        # print 'volatility',volatility
        sharpe_ratio = (annualized_return_ratio - start_free_risk_ratio)/volatility
        # print 'sharpe_ratio',sharpe_ratio
        infor = np.array(self.daily_strategy_profit_rate) - np.array(self.daily_benchmark_profit_rate)
        information_ratio = (np.average(infor)/np.sqrt(np.cov(infor,infor)[0,1]))
        # print 'information_ratio',information_ratio
        dynamic_capital = self.account.dynamic_record['capital']
        draw_down = []
        for index, value in enumerate(dynamic_capital):
            mm = index + 1
            draw_down.append(1 - value/max(dynamic_capital[0:mm]))
        #max_draw_down = max(draw_down)

        # 得到alpha回撤，恢复等相关信息
        alpha_information = self.calculate_draw_down_and_recovery(self.account.alpha_rate)
        alpha_max_draw_down = alpha_information[0]
        alpha_max_draw_down_period = alpha_information[1]
        alpha_max_recovery_period = alpha_information[2]
        alpha_recovery_index = alpha_information[3]
        alpha_max_min_index = alpha_information[4]
        # 输出字符串
        alpha_result_draw_down = 'Alpha Draw Down start-end time:' +\
            self.account.alpha_rate[alpha_max_min_index['max_value_index']][0].strftime('%Y-%m-%d') + '-->' +\
            self.account.alpha_rate[alpha_max_min_index['min_value_index']][0].strftime('%Y-%m-%d') + '\n'
        # 判断alpha恢复的时间
        if alpha_recovery_index == -1:
            alpha_recovery_time = -1
        else:
            alpha_recovery_time = self.account.alpha_rate[alpha_recovery_index][0].strftime('%Y-%m-%d')
        # 输出字符串
        alpha_result_recovery = 'Alpha period Recovery start-end time:' +\
            self.account.alpha_rate[alpha_max_min_index['min_value_index']][0].strftime('%Y-%m-%d') + '-->' +\
            str(alpha_recovery_time) + '\n'
        # 得到stock回撤，恢复等相关信息
        stock_information = self.calculate_draw_down_and_recovery(self.account.stock_rate)
        stock_max_draw_down = stock_information[0]
        stock_max_draw_down_period = stock_information[1]
        stock_max_recovery_period = stock_information[2]
        stock_recovery_index = stock_information[3]
        stock_max_min_index = stock_information[4]
        # 输出字符串
        stock_result_draw_down = 'Stock Draw Down start-end time:' +\
                 self.account.stock_rate[stock_max_min_index['max_value_index']][0].strftime('%Y-%m-%d') + '-->' +\
                 self.account.stock_rate[stock_max_min_index['min_value_index']][0].strftime('%Y-%m-%d') + '\n'
        # 判断alpha恢复的时间
        if stock_recovery_index == -1:
            stock_recovery_time = -1
        else:
            stock_recovery_time = self.account.stock_rate[stock_recovery_index][0].strftime('%Y-%m-%d')
        # 输出字符串
        stock_result_recovery = 'Stock period Recovery start-end time:' +\
                self.account.stock_rate[stock_max_min_index['min_value_index']][0].strftime('%Y-%m-%d') + '-->' +\
                str(stock_recovery_time) + '\n'
        # 计算F值，年化收益率除以最大回撤率
        stock_F_value = annualized_return_ratio / stock_max_draw_down
        alpha_F_value = alpha_annualized_return_ratio / alpha_max_draw_down
        # 得到年度alpha收益率
        year_alpha_rate = self.yearly_rate('alpha')
        # 得到年度stock收益率
        year_stock_rate = self.yearly_rate('stock')
        # 获得股票换手率
        stock_total_lot_changed_rate, stock_year_lot_changed_rate, stock_year_month_changed_rate = \
                self.calculate_lot_changed_rate()
        # 正式写入文件
        with open('./files/performance.dat', 'a+') as f:
            f.write('--------------------综合评价-------------------------\n')
            f.write('打印时间%s\n' %datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            f.write('''回测时间：%s - %s \n基准： %s\n基准年化收益率： %s\n'''
                    %(self.account.start,
                      self.account.end,
                      self.account.benchmark_code, 
                      benchmark_returns_ratio))
            f.write('------------ stock综合评价 -----------------\n')
            f.write('策略年化收益率%s \nstock_max_draw_down %s \nstock_max_draw_down_period %s \nstock_max_recovery_period %s\nstock_F_value %s\nsotck_lot_changed_rate %s\n' 
                    %(annualized_return_ratio, 
                      stock_max_draw_down,
                      stock_max_draw_down_period,
                      stock_max_recovery_period,
                      stock_F_value,
                      stock_total_lot_changed_rate))
            f.write(stock_result_draw_down)
            f.write(stock_result_recovery)
            f.write('------------ alpha综合评价 ----------------\n')
            f.write('策略年化收益率%s \nalpha_max_draw_down %s \nalpha_max_draw_down_period %s \nalpha_max_recovery_period %s\nalpha_F_value %s\n'
                    %(alpha_annualized_return_ratio,
                      alpha_max_draw_down,
                      alpha_max_draw_down_period,
                      alpha_max_recovery_period,
                      alpha_F_value))
            f.write(alpha_result_draw_down)
            f.write(alpha_result_recovery)
            #f.write('''回测时间：%s - %s \n基准： %s \n策略年化收益率(不要): %s\n基准年化收益率： %s\nalpha: %s \nbeta: %s\n策略波动收益率： %s \n夏普率： %s \n信息率： %s\n最大Drawdown Period(自然天数,如果值为0，相关的结果可以忽略)： %s\n最大Recovery Period(自然天数) %s\n最大回撤率 %s\n'''
            #        %(alpha, beta, 
            #          volatility,
            #          sharpe_ratio,
            #          information_ratio,
            #          max_draw_down_period,
            #          max_recovery_period,
            #          max_draw_down))
            f.write('-------------------- ALPHA　每年 -------------------------\n')
            # 得到每年alpha以及stock的回撤率并把结果保存
            alpha_year_record, alpha_year_result = self.calculate_annual_draw_down_and_recovery('alpha')
            stock_year_record, stock_year_result = self.calculate_annual_draw_down_and_recovery('stock')
            for year, results in alpha_year_result.items(): #把alpha结果保存
                start = alpha_year_record[year][results['max_min_index']['max_value_index']][0]
                end = alpha_year_record[year][results['max_min_index']['min_value_index']][0]
                recovery = alpha_year_record[year][results['recovery_index']][0]
                year_max_draw_down = results['max_draw_down']
                year_max_draw_down_period = results['draw_down_period']
                year_recovery_period = results['recovery_period']
                f.write('>>> alpha 年份%s\n' %year)
                f.write('    start ' + start.strftime('%Y-%m-%d') + '--->' + ' end ' + end.strftime('%Y-%m-%d') + '\n')
                f.write('    Recovery ' + end.strftime('%Y-%m-%d') + '--->' + ' recovery ' + str(recovery) + '\n')
                f.write('    Year Max Draw Down:%f\n' %year_max_draw_down)
                f.write('    Year Max Draw Down Period:%s\n' %year_max_draw_down_period)
                f.write('    Year Recovery Period:%s\n' %year_recovery_period)
                # 保存alpha年度收益率
                f.write('    annualized alpha return rate %s\n' %year_alpha_rate[year])
                # 保存alpha每年的F_value
                f.write('    alpha_F_value:%s\n' %(year_alpha_rate[year]/year_max_draw_down))
            f.write('-------------------- STOCK　每年 -------------------------\n')
            for year, results in stock_year_result.items(): #把stock结果保存
                start = stock_year_record[year][results['max_min_index']['max_value_index']][0]
                end = stock_year_record[year][results['max_min_index']['min_value_index']][0]
                recovery = stock_year_record[year][results['recovery_index']][0]
                year_max_draw_down = results['max_draw_down']
                year_max_draw_down_period = results['draw_down_period']
                year_recovery_period = results['recovery_period']
                f.write('>>> stock 年份%s\n' %year)
                f.write('    start ' + start.strftime('%Y-%m-%d') + '--->' + ' end ' + end.strftime('%Y-%m-%d') + '\n')
                f.write('    Recovery ' + end.strftime('%Y-%m-%d') + '--->' + ' recovery ' + str(recovery) + '\n')
                f.write('    Year Max Draw Down:%f\n' %year_max_draw_down)
                f.write('    Year Max Draw Down Period:%s\n' %year_max_draw_down_period)
                f.write('    Year Recovery Period:%s\n' %year_recovery_period)
                # 保存stock年度收益率
                f.write('    annualized stock return rate %s\n' %year_stock_rate[year])
                # 保存stock每年的F_value
                f.write('    stock_F_value:%s\n' %(year_stock_rate[year]/year_max_draw_down))
                # 保存每个月以及每年的换手率
                f.write('    stock_year_lot_changed_rate:%s\n' %stock_year_lot_changed_rate[year])
                for time_value in stock_year_month_changed_rate[year]:
                    f.write('         month:%s, lot_changed_lot_rate:%s\n' %(str(time_value[0].month), time_value[1]))
            f.write('-------------------- THE END -------------------------\n')
            # 保存每个月持股，收益情况
            for time, dataframe in self.account.stock_monthly_hold_profit.items():
                dataframe.to_csv('./files/stock.csv', na_rep=' ', mode='a')

        return 0

    def pre_list(self,list):  # 前一天的数据列表
        tem = []
        for i,item in enumerate(list):
            # print i,list[i]
            it = i - 1
            if it < 0:
                tem.append(list[0])
            else:
                tem.append(list[it])
        return tem

    def calculate_draw_down_and_recovery(self, time_value_data):
        # 初始化返回值
        max_value = time_value_data[0][1]
        max_index = 1
        max_draw_down = 0
        max_min_index = {'max_value_index':0, 'min_value_index':0}
        # 对数据中的每个元素加一
        time_value_data = [(value[0], value[1]+1) for value in time_value_data]
        # 如果数据中存在小于0的情况,就返回0,表示不计算最大回撤
        for index, value in enumerate(time_value_data):
            if value[1] <= 0:
                max_draw_down, draw_down_period,\
                recovery_period, recovery_index, max_min_index = 0, 0, 0, 0, 0
                return max_draw_down, draw_down_period,\
                        recovery_period, recovery_index, max_min_index 
        # 计算最大回撤率，同时记录index
        for index, time_value in enumerate(time_value_data[1:]):
            max_value = max(max_value, time_value[1])
            if max_value == time_value[1]:
                max_index = index
            if max_value == 0: # 为了避免分母为零
                draw_down = 0
            else:
                draw_down = (max_value - time_value[1]) / max_value
            if draw_down > max_draw_down:
                max_draw_down = draw_down
                max_min_index['max_value_index'] = max_index + 1
                max_min_index['min_value_index'] = index + 1
        # 计算恢复的时间
        recovery_index = -1
        for index, time_value in enumerate(time_value_data):
            if index > max_min_index['min_value_index'] and \
               time_value[1] == max_value:
                recovery_index = index
                break
        # 计算回撤的时间以及恢复的时间
        draw_down_period = (time_value_data[max_min_index['min_value_index']][0] - \
                            time_value_data[max_min_index['max_value_index']][0]).days
        if recovery_index == -1:
            recovery_period = -1
        else:
            recovery_period = (time_value_data[recovery_index][0] -\
                              time_value_data[max_min_index['min_value_index']][0]).days
        return max_draw_down, draw_down_period, recovery_period, recovery_index, max_min_index 

    def calculate_annual_draw_down_and_recovery(self, rate_type):
        # 得到每年的年度alpha/stock回撤，恢复
        year_set = set()
        year_record = {}
        for _time in self.account.timeList:
            year_set.add(_time.year)
        year_list = sorted(year_set)
        # 按年份记录alpha
        if rate_type == 'alpha':
            for year in year_list:
                for index, time_value in enumerate(self.account.alpha_rate):
                    if time_value[0].year == year:
                        year_record.setdefault(year, [])
                        year_record[year].append((time_value[0], time_value[1]))
        # 按按年份记录stock
        elif rate_type == 'stock':
            for year in year_list:
                for index, time_value in enumerate(self.account.stock_rate):
                    if time_value[0].year == year:
                        year_record.setdefault(year, [])
                        year_record[year].append((time_value[0], time_value[1]))
        # 计算每年的回撤时间, 回撤率 ,回撤率以及恢复时间
        year_result = {}
        for year, time_value_data in year_record.items():
            results = self.calculate_draw_down_and_recovery(time_value_data)
            year_result.setdefault(year, {})
            year_result[year]['max_draw_down'] = results[0]
            year_result[year]['draw_down_period'] = results[1]
            year_result[year]['recovery_period'] = results[2]
            year_result[year]['recovery_index'] = results[3]
            year_result[year]['max_min_index'] = results[4]
        return year_record, year_result

    def yearly_rate(self, rate_type):
        # 得到每年的年度alpha/stock rate
        year_set = set()
        year_rate = {}
        year_alpha_record = {}
        for _time in self.account.timeList:
            year_set.add(_time.year)
        year_list = sorted(year_set)
        # 按年份记录alpha
        if rate_type == 'alpha':
            for year in year_list:
                for index, time_value in enumerate(self.account.alpha_rate):
                    if time_value[0].year == year:
                        year_alpha_record.setdefault(year, [])
                        year_alpha_record[year].append(time_value[1])
        elif rate_type == 'stock':
            for year in year_list:
                for index, time_value in enumerate(self.account.stock_rate):
                    if time_value[0].year == year:
                        year_alpha_record.setdefault(year, [])
                        year_alpha_record[year].append(time_value[1])
        # 最后得到每年的alpha
        for _key, _values in year_alpha_record.items():
            year_rate.setdefault(_key, [])
            year_rate[_key] = (_values[-1] + 1) / float(_values[0] + 1) - 1
        # 打印出结果
        for _key, _value in year_rate.items():
            print '年份', _key, rate_type , _value
        return year_rate

    def calculate_lot_changed_rate(self):
        #按照每年的时间把换手率保存起来
        year_set = set()
        year_month_rate = {} #记录每年每个月的换率率
        year_rate = {} #记录每年的换率率
        for _time, _value in self.account.lot_changed_rate.items():
            year_set.add(_time.year)
        for year in sorted(year_set):
            for time, rate in self.account.lot_changed_rate.items():
                if time.year == year:
                    year_month_rate.setdefault(year, [])
                    year_month_rate[year].append((time, rate))
            # 用一年内所有换手率的平均计算年换手率
            year_rate[year] = sum([_v[1] for _v in year_month_rate[year]]) / len(year_month_rate[year])
        total_rate = sum(year_rate.values()) / len(year_rate.values())
        return total_rate, year_rate, year_month_rate

