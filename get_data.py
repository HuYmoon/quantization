from jqdatasdk import *
import pandas as pd
from pandas import DataFrame
import time, threading
auth("13423691402", "HY1995827.HG")


def get_stockname():

    # 行业代码
    industry_index = list(get_industries(name='sw_l1').index)
    # 行业名称
    industry_name = list(get_industries(name='sw_l1').iloc[:, 0])
    # industry_name_csv = DataFrame(industry_name).to_csv('行业名称')

    # 所有行业的股票汇总
    global stock_name
    stock_name = []
    for industry in industry_index:
        stock = get_industry_stocks(industry_code=industry)
        for i in stock:
            stock_name.append(i)
    return stock_name


def get_data(stock_name, start_date, end_date):

    # 获取数据
    # 当天的价格(有些股票存在空值)

    try:
        global close
        close = get_price(security=stock_name[0], start_date=start_date, end_date=end_date,
                          frequency='daily', fields=None, skip_paused=False, fq='pre')['close']
        close = DataFrame(close)
        close.columns = [stock_name[0]]   # stock_name[0]返回的是一个str, str转list会变成一个一个的字符
        logout()
        count = 0
        for i in range(len(stock_name) - 1):
            if count == 0:
                auth("13423691402", "HY1995827.HG")
            close_price = get_price(security=stock_name[i + 1], start_date=start_date, end_date=end_date,
                                    frequency='daily', fields=None, skip_paused=False, fq='pre')['close']
            close_price = DataFrame(close_price)
            close_price.columns = [stock_name[i + 1]]
            close = pd.merge(close, close_price, left_index=True, right_index=True, how='outer')

            count += 1
            if count == 100:
                time.sleep(60)
                count = 0
                logout()

    finally:
        data = close.to_csv('2011年股票闭市价格.csv')


def concat_closedata():
    data2010 = pd.read_csv('2010年股票闭市价格.csv')
    data2011 = pd.read_csv('2011年股票闭市价格.csv')
    data2012 = pd.read_csv('2012年股票闭市价格.csv')
    data2013 = pd.read_csv('2013年股票闭市价格.csv')
    data2014 = pd.read_csv('2014年股票闭市价格.csv')
    data2015 = pd.read_csv('2015年股票闭市价格.csv')
    data2016 = pd.read_csv('2016年股票闭市价格.csv')
    data2017 = pd.read_csv('2017年股票闭市价格.csv')
    data2018 = pd.read_csv('2018年股票闭市价格.csv')
    data = pd.concat([data2010, data2011, data2012, data2013, data2014, data2015, data2016, data2017, data2018],
                     ignore_index=True)
    data.index = pd.date_range(start='2010-01-01', end='2018-12-31', freq='B')
    data_csv = data.to_csv('2010-2018的股票闭市价格.csv')


def main():
    stock_name = get_stockname()

    get_data(stock_name, '2011-01-01', '2011-12-31')

    # t1 = threading.Thread(target=get_data, args=(stock_name[:500],))
    # t1.start()
    #
    # t2 = threading.Thread(target=get_data, args=(stock_name[500:1000],))
    # t2.start()
    #
    # t3 = threading.Thread(target=get_data, args=(stock_name[1000:1500],))
    # t3.start()
    #
    # t4 = threading.Thread(target=get_data, args=(stock_name[1500:2000],))
    # t4.start()
    #
    # t5 = threading.Thread(target=get_data, args=(stock_name[2000:2500],))
    # t5.start()
    #
    # t6 = threading.Thread(target=get_data, args=(stock_name[2500:3000],))
    # t6.start()
    #
    # t7 = threading.Thread(target=get_data, args=(stock_name[3000:],))
    # t7.start()


if __name__ == "__main__":
    main()