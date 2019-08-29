from jqdatasdk import *
import pandas as pd
import numpy as np
from pandas import DataFrame
auth("13423691402", "HY1995827.HG")


def get_eps():
    industry_index = list(get_industries(name='sw_l1').index)
    # 所有行业的股票汇总
    stock_reg = []
    for industry in industry_index:
        stock = get_industry_stocks(industry_code=industry)
        for i in stock:
            stock_reg.append(i)
    # 获取财务数据（提取财务数据会自动删除取值为空的股票，因此每个季度返回的财务数据量不一样，使用pd.concat合并）
    q = query(indicator.code, indicator.eps, indicator.pubDate).filter(indicator.code.in_(stock_reg))

    data1 = get_fundamentals(q, statDate='2018q1').T
    col = list(data1.iloc[0, :])
    data1 = DataFrame(data1.iloc[1:, :])   # 取了三个字段，股票代码作为列名以后还剩两行，如果是两个字段取完剩下一行需要用转置
    data1.columns = col

    data1 = DataFrame(data1).to_csv('2018第一季度+日期.csv')

    # data2 = get_fundamentals(q, statDate='2018q2').T
    # col = list(data2.iloc[0, :])
    # data2 = DataFrame(DataFrame(data2.iloc[1:, :]))
    # data2.columns = col
    # # data2 = DataFrame(data2.T).to_csv('2018第二季度.csv')
    # data3 = get_fundamentals(q, statDate='2018q3').T
    # col = list(data3.iloc[0, :])
    # data3 = DataFrame(DataFrame(data3.iloc[1, :]).T)
    # data3.columns = col
    # # data3 = DataFrame(data3.T).to_csv('2018第三季度.csv')
    #
    # data4 = get_fundamentals(q, statDate='2018q4').T
    # col = list(data4.iloc[0, :])
    # data4 = DataFrame(DataFrame(data4.iloc[1, :]).T)
    # data4.columns = col
    # # data4 = DataFrame(data4.T).to_csv('2018第四季度.csv')
    #
    #
    # # 财务数据合并
    # data = pd.concat([data1, data2, data3, data4], ignore_index=True)
    # data_csv = data.to_csv('每日收益.csv')
    logout()


def main():
    get_eps()


if __name__ == "__main__":
    main()

