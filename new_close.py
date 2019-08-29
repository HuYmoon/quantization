from jqdatasdk import *
import pandas as pd
auth("13423691402", "HY1995827.HG")
import time, threading
lock = threading.Lock()

def get_stockname():

    # 行业代码
    industry_index = list(get_industries(name='sw_l1').index)
    # 行业名称
    # industry_name = list(get_industries(name='sw_l1').iloc[:, 0])
    # industry_name_csv = DataFrame(industry_name).to_csv('行业名称')

    # 所有行业的股票汇总
    global stock_name
    stock_name = []
    for industry in industry_index:
        stock = get_industry_stocks(industry_code=industry)
        for i in stock:
            stock_name.append(i)
    return stock_name


def get_data(stock_name):
    lock.acquire()
    # 获取时间节点
    month_end = pd.date_range(start='2005-04-29', end='2019-07-01', freq='BM')

    month_end = list(month_end.strftime('%Y-%m-%d'))   # 时间戳转换成列表

    # 获取数据
    # 当天的价格(有些股票存在空值)
    try:
        for i in range(1):

            global close
            close = get_price(security=stock_name[i], end_date=month_end[0], count=1,
                              frequency='daily', fields=None, skip_paused=False, fq='pre')['close']
            for times in month_end[1:]:
                close_price = get_price(security=stock_name[i], end_date=times, count=1,
                                        frequency='daily', fields=None, skip_paused=False, fq='pre')['close']
                close = pd.concat([close, close_price])
        logout()

        count = 0
        for i in range(len(stock_name) - 1):
            if count == 0:
                auth("13423691402", "HY1995827.HG")

            global close_new
            close_new = get_price(security=stock_name[i + 1], end_date=month_end[0], count=1,
                                  frequency='daily', fields=None, skip_paused=False, fq='pre')['close']
            for times in month_end[1:]:
                close_price = get_price(security=stock_name[i + 1], end_date=times, count=1,
                                        frequency='daily', fields=None, skip_paused=False, fq='pre')['close']
                close_new = pd.concat([close_new, close_price])

            close = pd.merge(close, close_new, left_index=True, right_index=True, how='outer')

            count += 1
            if count == 50:
                time.sleep(30)
                count = 0
                logout()
    finally:
        data = close.to_csv('2005-2019股票闭市价格.csv')
    lock.release()


def main():

    stock_name = get_stockname()

    t1 = threading.Thread(target=get_data, args=(stock_name[:500],))
    t1.start()

    t2 = threading.Thread(target=get_data, args=(stock_name[500:1000],))
    t2.start()

    t3 = threading.Thread(target=get_data, args=(stock_name[1000:1500],))
    t3.start()

    t4 = threading.Thread(target=get_data, args=(stock_name[1500:2000],))
    t4.start()

    t5 = threading.Thread(target=get_data, args=(stock_name[2000:2500],))
    t5.start()

    t6 = threading.Thread(target=get_data, args=(stock_name[2500:3000],))
    t6.start()

    t7 = threading.Thread(target=get_data, args=(stock_name[3000:],))
    t7.start()


if __name__ == "__main__":
    main()