import pandas as pd
from pandas import DataFrame
import numpy as np
import threading


def concat_data():
    data2009q4 = pd.read_csv('2009年第四季度每日收益+日期.csv', index_col=0)
    data2010q1 = pd.read_csv('2010年第一季度每日收益+日期.csv', index_col=0)
    data2010q2 = pd.read_csv('2010年第二季度每日收益+日期.csv', index_col=0)
    data2010q3 = pd.read_csv('2010年第三季度每日收益+日期.csv', index_col=0)
    data2010q4 = pd.read_csv('2010年第四季度每日收益+日期.csv', index_col=0)
    data2011q1 = pd.read_csv('2011年第一季度每日收益+日期.csv', index_col=0)
    data2011q2 = pd.read_csv('2011年第二季度每日收益+日期.csv', index_col=0)
    data2011q3 = pd.read_csv('2011年第三季度每日收益+日期.csv', index_col=0)
    data2011q4 = pd.read_csv('2011年第四季度每日收益+日期.csv', index_col=0)
    data2012q1 = pd.read_csv('2012年第一季度每日收益+日期.csv', index_col=0)
    data2012q2 = pd.read_csv('2012年第二季度每日收益+日期.csv', index_col=0)
    data2012q3 = pd.read_csv('2012年第三季度每日收益+日期.csv', index_col=0)
    data2012q4 = pd.read_csv('2012年第四季度每日收益+日期.csv', index_col=0)
    data2013q1 = pd.read_csv('2013年第一季度每日收益+日期.csv', index_col=0)
    data2013q2 = pd.read_csv('2013年第二季度每日收益+日期.csv', index_col=0)
    data2013q3 = pd.read_csv('2013年第三季度每日收益+日期.csv', index_col=0)
    data2013q4 = pd.read_csv('2013年第四季度每日收益+日期.csv', index_col=0)
    data2014q1 = pd.read_csv('2014年第一季度每日收益+日期.csv', index_col=0)
    data2014q2 = pd.read_csv('2014年第二季度每日收益+日期.csv', index_col=0)
    data2014q3 = pd.read_csv('2014年第三季度每日收益+日期.csv', index_col=0)
    data2014q4 = pd.read_csv('2014年第四季度每日收益+日期.csv', index_col=0)
    data2015q1 = pd.read_csv('2015年第一季度每日收益+日期.csv', index_col=0)
    data2015q2 = pd.read_csv('2015年第二季度每日收益+日期.csv', index_col=0)
    data2015q3 = pd.read_csv('2015年第三季度每日收益+日期.csv', index_col=0)
    data2015q4 = pd.read_csv('2015年第四季度每日收益+日期.csv', index_col=0)
    data2016q1 = pd.read_csv('2016年第一季度每日收益+日期.csv', index_col=0)
    data2016q2 = pd.read_csv('2016年第二季度每日收益+日期.csv', index_col=0)
    data2016q3 = pd.read_csv('2016年第三季度每日收益+日期.csv', index_col=0)
    data2016q4 = pd.read_csv('2016年第四季度每日收益+日期.csv', index_col=0)
    data2017q1 = pd.read_csv('2017年第一季度每日收益+日期.csv', index_col=0)
    data2017q2 = pd.read_csv('2017年第二季度每日收益+日期.csv', index_col=0)
    data2017q3 = pd.read_csv('2017年第三季度每日收益+日期.csv', index_col=0)
    data2017q4 = pd.read_csv('2017年第四季度每日收益+日期.csv', index_col=0)
    data2018q1 = pd.read_csv('2018年第一季度每日收益+日期.csv', index_col=0)
    data2018q2 = pd.read_csv('2018年第二季度每日收益+日期.csv', index_col=0)
    data2018q3 = pd.read_csv('2018年第三季度每日收益+日期.csv', index_col=0)
    data2018q4 = pd.read_csv('2018年第四季度每日收益+日期.csv', index_col=0)
    data = pd.concat([data2009q4, data2010q1, data2010q2, data2010q3, data2010q4,
                      data2011q1, data2011q2, data2011q3, data2011q4,
                      data2012q1, data2012q2, data2012q3, data2012q4,
                      data2013q1, data2013q2, data2013q3, data2013q4,
                      data2014q1, data2014q2, data2014q3, data2014q4,
                      data2015q1, data2015q2, data2015q3, data2015q4,
                      data2016q1, data2016q2, data2016q3, data2016q4,
                      data2017q1, data2017q2, data2017q3, data2017q4,
                      data2018q1, data2018q2, data2018q3, data2018q4,], ignore_index=True)
    data = DataFrame(data.iloc[:, :-1])
    # data_csv = data.to_csv('2010-2018data.csv')
    return data


def f(data):
    """
    财务数据填充
    :param data: 数组
    :return:
    """
    data_time = data[0:-1:2]   # 数组间隔切片,选出所有的日期
    data_data = data[1::2]   # 选出所有的财务指标
    # 处理财务指标的缺失值，后向填充
    data_time = data_time.fillna(method='bfill')
    data_time = data_time.fillna(method='ffill')
    data_data = data_data.fillna(method='bfill')
    data_data = data_data.fillna(method='ffill')
    data_col = []
    row_num = len(data)/2
    data_data.index = np.arange(row_num)
    data_time.index = np.arange(row_num)
    for i in range(int(row_num)):
        if i == 0:
            if data_time[i] >= '2018-12-31':
                n = len(pd.date_range(start='2010-01-01', end='2018-12-31', freq='B'))
                for m in range(n):
                    data_col.append(data_data[i])
                break
            else:
                n = len(pd.date_range(start='2010-01-01', end=data_time[i], freq='B'))
                for m in range(n):
                    data_col.append(data_data[i])

        else:
            if data_time[i] >= '2018-12-31':
                n = len(pd.date_range(start=data_time[i - 1], end='2018-12-31', freq='B'))
                for m in range(n - 1):
                    data_col.append(data_data[i])
                break
            elif data_time[i] == data_time[i-1]:
                n = 0
            else:
                n = len(pd.date_range(start=data_time[i-1], end=data_time[i], freq='B'))
                for m in range(n-1):
                    data_col.append(data_data[i])
    data = pd.Series(data_col)
    return data


def process_data(data):
    # 财务数据合并

    # 对每支股票进行填充
    new_data = data.apply(f, axis=0)
    # 输出结果
    new_data.index = pd.date_range(start='2010-01-01', end='2018-12-31', freq='B')
    new_data.columns = data.columns
    print(new_data)
    new_data_csv = new_data.to_csv('2010-2018的每日收益+日期.csv')


def main():
    # data = concat_data()
    data = pd.read_csv('2010-2018data.csv', index_col=0)
    process_data(data)

    # t1 = threading.Thread(target=process_data, args=(data.iloc[:, :500],))
    # t1.start()
    #
    # t2 = threading.Thread(target=process_data, args=(data.iloc[:, 500:1000],))
    # t2.start()
    #
    # t3 = threading.Thread(target=process_data, args=(data.iloc[:, 1000:1500],))
    # t3.start()
    #
    # t4 = threading.Thread(target=process_data, args=(data.iloc[:, 1500:2000],))
    # t4.start()
    #
    # t5 = threading.Thread(target=process_data, args=(data.iloc[:, 2000:2500],))
    # t5.start()
    #
    # t6 = threading.Thread(target=process_data, args=(data.iloc[:, 2500:3000],))
    # t6.start()
    #
    # t7 = threading.Thread(target=process_data, args=(data.iloc[:, 3000:],))
    # t7.start()


if __name__ == "__main__":
    main()
