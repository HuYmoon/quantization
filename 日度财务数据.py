from jqdatasdk import *
import pandas as pd
from pandas import DataFrame
import time, threading
auth("13896700518", "HY1995827.HG")


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


def get_data(stock_name, n):
    """

    :param stock_name:
    :param n: 10-18年的交易日数据量2187
    :return:
    """

    # 获取数据
    # 当天的价格(有些股票存在空值)


    try:
        global close
        q = query(valuation.code, valuation.day, valuation.market_cap).filter(indicator.code==stock_name[0])
        close = get_fundamentals_continuously(q, end_date='2018-12-31', count=n)['market_cap']
        logout()
        count = 0
        for i in range(len(stock_name) - 1):
            if count != 0:
                q = query(valuation.code, valuation.day, valuation.market_cap).filter(indicator.code == stock_name[i+1])
            if count == 0:
                auth("13896700518", "HY1995827.HG")
                q = query(valuation.code, valuation.day, valuation.market_cap).filter(indicator.code == stock_name[i+1])
            close_price = get_fundamentals_continuously(q, end_date='2018-12-31', count=n)['market_cap']
            close = pd.merge(close, close_price, left_index=True, right_index=True, how='outer')

            count += 1
            if count == 100:
                time.sleep(60)
                count = 0
                logout()

    finally:

        data = close.to_csv('2010-2018年日度财务数据.csv')


def main():
    stock_name = get_stockname()
    get_data(stock_name, 2187)

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