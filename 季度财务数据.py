# encoding = utf-8
from jqdatasdk import *
import pandas as pd
import numpy as np
from pandas import DataFrame


def get_eps():
    auth("13896700518", "HY1995827.HG")

    industry_index = list(get_industries(name='sw_l1').index)
    # 所有行业的股票汇总
    stock_reg = []
    for industry in industry_index:
        stock = get_industry_stocks(industry_code=industry)
        for i in stock:
            stock_reg.append(i)
    # 获取财务数据（提取财务数据会自动删除取值为空的股票，因此每个季度返回的财务数据量不一样，使用pd.concat合并）
    q = query(indicator.code, indicator.pubDate, indicator.eps).filter(indicator.code.in_(stock_reg))
    logout()
    return q


def one_data(q):
    auth("13896700518", "HY1995827.HG")
    data0 = get_fundamentals(q, statDate='2009q4').T
    col = list(data0.iloc[0, :])
    data0 = DataFrame(DataFrame(data0.iloc[1:, :]))
    data0.columns = col
    data0_csv = data0.to_csv('2009年第四季度每日收益+日期.csv')

    data1 = get_fundamentals(q, statDate='2010q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2010年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2010q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2010年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2010q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2010年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2010q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2010年第四季度每日收益+日期.csv')
    logout()


def two_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2011q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2011年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2011q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2011年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2011q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2011年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2011q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2011年第四季度每日收益+日期.csv')
    logout()


def three_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2012q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2012年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2012q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2012年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2012q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2012年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2012q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2012年第四季度每日收益+日期.csv')
    logout()


def four_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2013q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2013年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2013q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2013年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2013q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2013年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2013q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2013年第四季度每日收益+日期.csv')
    logout()


def five_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2014q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2014年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2014q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2014年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2014q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2014年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2014q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2014年第四季度每日收益+日期.csv')
    logout()


def six_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2015q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2015年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2015q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2015年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2015q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2015年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2015q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2015年第四季度每日收益+日期.csv')
    logout()


def seven_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2016q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2016年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2016q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2016年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2016q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2016年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2016q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2016年第四季度每日收益+日期.csv')
    logout()


def eight_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2017q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2017年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2017q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2017年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2017q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2017年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2017q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2017年第四季度每日收益+日期.csv')
    logout()


def nine_data(q):
    auth("13896700518", "HY1995827.HG")
    data1 = get_fundamentals(q, statDate='2018q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(DataFrame(data1.iloc[1:, :]))
    data1.columns = col
    data1_csv = data1.to_csv('2018年第一季度每日收益+日期.csv')

    data2 = get_fundamentals(q, statDate='2018q2').T
    col = list(data2.iloc[0, :])
    data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    data2.columns = col
    data2_csv = data2.to_csv('2018年第二季度每日收益+日期.csv')

    data3 = get_fundamentals(q, statDate='2018q3').T
    col = list(data3.iloc[0, :])
    data3 = DataFrame(DataFrame(data3.iloc[1:, :]))
    data3.columns = col
    data3_csv = data3.to_csv('2018年第三季度每日收益+日期.csv')

    data4 = get_fundamentals(q, statDate='2018q4').T
    col = list(data4.iloc[0, :])
    data4 = DataFrame(DataFrame(data4.iloc[1:, :]))
    data4.columns = col
    data4_csv = data4.to_csv('2018年第四季度每日收益+日期.csv')

    logout()


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
    # print(new_data)
    new_data_csv = new_data.to_csv('2010-2018的净利润+日期.csv')


def main():
    # 确定查询字段
    q = get_eps()
    # 获取10-18年的数据
    one_data(q)
    two_data(q)
    three_data(q)
    four_data(q)
    five_data(q)
    six_data(q)
    seven_data(q)
    eight_data(q)
    nine_data(q)
    # 数据合并
    data = concat_data()
    process_data(data)  # 修改文件名


if __name__ == '__main__':
    main()
