from jqdatasdk import *
import numpy as np
import pandas as pd
from pandas import DataFrame
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from scipy.stats import pearsonr
import threading


def filter_extreme_MAD(data):
    """
    中位数去极值
    :param data: data为数组
    :return: 去极值后的数据
    """
    median_ = np.median(data)
    new_median = np.median((np.abs((data - median_))))
    max_range = median_ + 5*new_median
    min_range = median_ - 5*new_median
    data = DataFrame(data)
    data = data.iloc[:, 0].apply(change_extreme, args=(min_range, max_range))
    return data


def change_extreme(num, min, max):

    if num > max:
        num = max
    elif num < min:
        num = min
    else:
        num = num
    return num


def data_procession(data1, data2, n):
    """
    数据预处理
    :param data1: 股票闭市价格(pre+data+later)
    :param data2: 每日收益(data, 财务报表, 季度数据）, 数据组包含的股票个数少于data1
    :param n: 调仓期
    :return:
    """
    # 根据调仓期匹配交易日
    if n != 1:
        date_index = []
        date_index.append(data1.index[0])
        for i in range(data1.shape[0]-2):
            if (i+1)*n < data1.shape[0]-2:
                date_index.append(data1.index[(i+1) * n])
            else:
                break
        date_index.append(data1.index[-1])
        data1 = data1.loc[date_index, :]  # 根据索引取值，当数据量大的时候.loc可能会重复取，最好使用reindex
    data2 = data2.loc[data1.index[1:-1], :]
    print(data1.shape)
    print(data2.shape)

    # 对获取的数据进行缺失值填充
    data1 = data1.fillna(data1.mean())
    data1 = data1.dropna(axis=1)
    data2 = data2.fillna(data2.mean())
    data2 = data2.dropna(axis=1)

    # 获取所有行业股票的因子值（EP）

    # 当天的价格(有些股票存在空值)
    close = data1.iloc[1:-1, :]
    # 前一天的价格
    pre_close = data1.iloc[:-2, :]
    # 后一天的价格
    later_close = data1.iloc[2:, :]
    # 统一索引值
    later_close.index = close.index
    pre_close.index = close.index
    data2.index = close.index

    # EP, 股票名称为闭市价格和每日收益的交集
    same_col = list(set(data1.columns).intersection(data2.columns))
    factor_values = {}
    for stock_name in same_col:
        # 数据框转list,list(df.values),如果直接取list(df),返回的结果是列名
        factor_values[stock_name] = list((data2.loc[:, [stock_name]].div(close.loc[:, [stock_name]])).values)

    # 字典转化为数据框
    factor_values = DataFrame(factor_values, index=close.index)

    # 中位数去极值(apply函数，apply调用的函数需要最后返回结果）
    factor_values = factor_values.apply(filter_extreme_MAD, axis=0)

    # 标准化
    standard = StandardScaler()
    factor_values = standard.fit_transform(factor_values)   # 返回的结果是np.array
    factor_values = DataFrame(factor_values, columns=same_col)
    # print(factor_values)

    # 缺失值填充
    factor_values_arr = np.array(factor_values)
    factor_values = DataFrame(np.where(factor_values_arr, factor_values_arr, np.nan), index=data2.index,
                              columns=same_col)   # 标准化以后有些列的值全为0，不是nan
    factor_values = factor_values.fillna(factor_values.mean())
    factor_values = factor_values.dropna(axis=1)

    # 每股收益率

    # 计算T+1期的收益率
    net = later_close.sub(close).div(close)

    # 缺失值填充
    net_arr = np.array(net)
    net = DataFrame(np.where(net_arr, net_arr, np.nan), index=close.index,
                    columns=close.columns)  # 算数运算以后有些列的值全为0，不是nan
    net = net.fillna(net.mean())
    net = net.dropna(axis=1)

    # 根据调仓期同步索引值，合并列名，否则回归产生bug， 由于有些列的取值为空，所以去处理后的两个数据框列名的交集
    col = list(set(factor_values.columns).intersection(net.columns))
    net = net.loc[:, col]
    factor_values = factor_values.loc[:, col]
    net.index = factor_values.index
    # print(factor_values.shape)
    # print(net.shape)

    return factor_values, net


def regression(factor, net):
    """
    回归
    :param factor: 每股股票的因子值
    :param net: 每股股票的收益率
    :return: table_reg
    """
    factor = factor.T
    net = net.T
    tlist = []
    rlist = []
    iclist = []
    for name in factor.columns:
        reg = sm.OLS(net.loc[:, name], factor.loc[:, name])    # X,Y的长度和索引要一致
        model = reg.fit()
        tvalues = DataFrame(model.tvalues)
        weight = DataFrame(model.params)
        rvalues, pvalues = pearsonr(net.loc[:, name], factor.loc[:, name])   # 皮尔逊相关系数
        tlist.append(tvalues)
        rlist.append(weight)
        iclist.append(rvalues)
        # iclist.append(np.sqrt(model.rsquared))
    # t值
    tarr = np.array(tlist)
    rarr = np.array(rlist)
    table = {"|t|均值": np.mean(np.abs(tarr)),
             "|t|>2占比": list(np.where(np.abs(tarr) > 2, 1, 0)).count(1)/factor.shape[1],
             "t均值": np.mean(tarr),
             "t均值/t标准差": np.mean(tarr)/np.std(tarr),
             "因子收益率均值": np.mean(rarr),
             "因子收益率序列t检验": np.std(rarr)}
    table_reg = DataFrame(table, columns=["|t|均值", "|t|>2占比", "t均值", "t均值/t标准差",
                                          "因子收益率均值", "因子收益率序列t检验"], index=[0])

    # ic值
    icarr = np.array(iclist)
    icarr[np.isnan(icarr)] = 0
    # ic_csv = DataFrame(iclist).to_csv('IC值.csv')
    table_ic = {"IC序列均值": np.mean(icarr),
                "IC序列标准差": np.std(icarr),
                "IR比率": np.mean(icarr) / np.std(icarr),
                "IC>0占比": list(np.where(icarr > 0, 1, 0)).count(1) / len(icarr),
                "|IC|>0.02占比": list(np.where(np.abs(icarr) > 0.02, 1, 0)).count(1) / len(icarr)}

    table_ic = DataFrame(table_ic, columns=["IC序列均值", "IC序列标准差", "IR比率",
                                             "IC>0占比", "|IC|>0.02占比"], index=[0])
    return table_reg, table_ic




def main():
    # read_csv读入所有的列,index_col=0将第一列做为行索引
    data_close = pd.read_csv('2010-2018的净利润+日期.csv', index_col=0)
    data_stocknet = pd.read_csv('2010-2018年总市值.csv', index_col=0)
    data_stocknet = data_stocknet*100000000
    print(data_close.shape)
    print(data_stocknet.shape)
    print('数据读入完毕')
    # data_close = data_close.loc[:, ['000001.XSHE', '000002.XSHE']]
    # data_stocknet = data_stocknet.loc[:, ['000001.XSHE', '000002.XSHE']]

    factor_values, net = data_procession(data_close, data_stocknet, 1)
    # print(factor_values)
    print('数据预处理完成')
    table_reg, table_ic = regression(factor_values, net)
    print(table_reg)
    print(table_ic)
    print('回归分析已完成')


if __name__ == "__main__":
    main()