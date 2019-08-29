from jqdatasdk import *
import numpy as np
import pandas as pd
from pandas import DataFrame
auth("13423691402", "HY1995827.HG")


def get_data():

    # 行业代码
    industry_index = list(get_industries(name='zjw').index)
    # 行业名称
    industry_name = list(get_industries(name='zjw').iloc[:, 0])
    industry_name_csv = DataFrame(industry_name).to_csv('行业名称')


    # 获取每个行业的股票代码
    stockpool = {}
    stock_reg = []   # 30个行业的股票汇总
    for industry in industry_index[:30]:
        stock = get_industry_stocks(industry_code=industry)
        stockpool[industry] = stock
    # 获取时间列表
    dates = pd.date_range(start='2019-01-04', end='2019-03-30', freq='7D')   # 时间戳索引

    # 获取每个行业的因子值
    factor = {}
    for industry in industry_index[:30]:
        # 财务数据
        # q = query(valuation.pe_ratio_lyr).filter(valuation.code.in_(stockpool[industry]))
        # a = get_fundamentals_continuously(q, end_date='2016-08-31', count=750)['pe_ratio_lyr']
        # print(a)
        # factor[industry] = a.loc[dates]

        # 聚宽因子库
        factor_values = get_factor_values(securities=stockpool[industry], factors=['net_working_capital'], start_date='2019-01-01',
                                          end_date='2019-03-30')['net_working_capital']
        dataa =
        factor[industry] = factor_values.loc[dates, :]

    return factor, dates


def fenceng():
    stockpool = list(get_all_securities(types=['stock'], date='2019-06-30').index)
    factor_values = get_factor_values(securities=stockpool, factors='size', start_date='2018-01-01', end_date='2019-06-30')['size']
    far = analyze_factor(factor=factor_values, start_date='2018-01-01', end_date='2019-06-30', weight_method='mktcap',
                         industry='zjw', quantiles=5, periods=(1, 5, 10), max_loss=0.2)
    far.create_full_tear_sheet(demeaned=False, group_adjust=True, by_group=True, turnover_periods=None,
                               avgretplot=(5, 15), std_bar=False)
#     factor, dates = get_data()
#     # 排序，分组
#
#     for k in factor:
#         for i in range(len(dates)):
#             # 组合列表
#             lyer = {"组合一": [],
#                     "组合二": [],
#                     "组合三": [],
#                     "组合四": [],
#                     "组合五": []}
#             # 获取每一个时间点的股票因子值
#             k_factor = factor[k].iloc[i, :]
#             k_factorT = DataFrame(k_factor.T, columns=dates[i])  # 交换行列
#             print(k_factorT)
#             # 排序
#             k_factorT_sort = k_factorT.sort_values(by=dates[i], ascending=False)
#             # 每个时间点对应的股票个数/层数,得到每个组合中的股票数
#             m = k_factorT_sort.shape[0]/5
#             lyer["组合一"].append(list(k_factorT_sort.iloc[0:m, :].index))
#             lyer["组合二"].append(list(k_factorT_sort.iloc[m:2*m, :].index))
#             lyer["组合三"].append(list(k_factorT_sort.iloc[2*m:3*m, :].index))
#             lyer["组合四"].append(list(k_factorT_sort.iloc[3*m:4*m, :].index))
#             lyer["组合五"].append(list(k_factorT_sort.iloc[4*m:5*m, :].index))
#             # 每个组合的平均收益率
#             net = {"组合一": [],
#                    "组合二": [],
#                    "组合三": [],
#                    "组合四": [],
#                    "组合五": []}
#             for k in net:
#                 net[k] =





def signal_factor_test(data, current_price, market_values):
    """
    单因子测试：回归、IC
    :param data:
    data: 时间+行业(列名industry)+因子+股票收益率(y)
    current_price： 个股流通市值,当作wls的权重
    market_values: 市值因子,IC回归的变量之一
    :return:
    table_reg: 估值因子回归测试结果
    table_ic： 估值因子IC值分析
    """
    # 1.数据处理
    industy = pd.get_dummies(data.iloc[:, 1])   # 类别特征抽取
    y = DataFrame(data.iloc[:, -1])  # 提取回归模型中的Y,先添加类别特征数据框，再加回去（让因变量在最后一列）
    yname = y.columns.values
    newdata = data.drop(["industry", yname[0]], axis=1)   # 删除行业列和Y
    newdata = pd.merge(newdata, industy, left_index=True,
                       right_index=True, how="outer")  # 数据合并(时间+因子+行业虚拟变量)
    # wls回归的数据整理
    newdata_reg = pd.merge(newdata, y, left_index=True,
                           right_index=True, how="outer")   # 数据合并(时间+因子+行业虚拟变量+y)
    newdata_wls = pd.merge(newdata_reg, current_price, left_index=True,
                           right_index=True, how="outer")   # 数据合并(时间+因子+行业虚拟变量+y+权重股票流通价值)
    # IC回归的数据整理
    newdata_mfv = pd.merge(newdata, market_values, left_index=True,
                           right_index=True, how="outer")  # 数据合并(时间+因子+行业虚拟变量+市值因子)
    newdata_ic = pd.merge(newdata_mfv, y, left_index=True,
                          right_index=True, how="outer")   # 数据合并(时间+因子+行业虚拟变量+市值因子+y)

    # 2.拟合回归方程,提取结果,生成表格
    time = data.iloc[:, 0].unique()   # 提取数据中所有的时间类型
    fnum = data.shape[1]-3   # 因子数量
    table_reg = DataFrame(np.zeros((1, 7)), columns=["因子", "|t|均值", "|t|>2占比", "t均值", "t均值/t标准差",
                                                     "因子收益率均值", "因子收益率序列t检验"])
    table_ic = DataFrame(np.zeros((1, 6)), columns=["因子", "IC序列均值", "IC序列标准差",
                                                    "IR比率", "IC>0占比", "|IC|>0.02占比"])
    fig, ax = plt.subplots(1, 2)
    for i in range(fnum):
        # 每次因子循环列表清空
        tlist = []   # t值列表
        rlist = []   # 因子收益率列表
        iclist = []   # ic值列表
        for j in range(len(time)):

            # wls,创建属于某一个因子的其中一个一个截面的所有数据集(股票+因子+虚拟变量+y+股票流通价值)
            newdata2 = newdata_wls[newdata_wls.iloc[:, 0].isin([time[j]])]
            # IC的ols,创建属于某一个因子的其中一个截面的所有数据集(股票+因子+虚拟变量+市值因子+y)
            newdata3 = newdata_ic[newdata_ic.iloc[:, 0].isin([time[j]])]
            # 创建回归方程中的自变量名字列表
            col = list(industy.columns.values)
            factor_data = DataFrame(newdata2.iloc[:, i + 1])
            global factor_name   # 全局变量
            factor_name = list(factor_data.columns.values)[0]
            col.append(factor_name)  # 单因子自变量的名字列表,list.append没有返回值,直接修改col
            # wls回归
            y = newdata2.iloc[:, -2]
            x = sm.add_constant(newdata2.loc[:, col])   # 增加常数项
            reg = sm.WLS(y, x, weights=newdata2.iloc[:, -1])   # loc是列名索引,exog增加截距
            model = reg.fit()
            # IC的ols回归
            col.append(list(DataFrame(market_values).columns.values)[0])
            x_ic = sm.add_constant(newdata3.loc[:, col])
            reg_ic = sm.WLS(y, x_ic)
            model_ic = reg_ic.fit()
            # 提取wls回归模型结果
            tvalues = DataFrame(model.tvalues).iloc[-1, :]
            weight = DataFrame(model.params)
            tlist.append(DataFrame(model.tvalues).iloc[-1, :])
            rlist.append(weight.iloc[-1, :])
            # 提取IC的ols回归模型结果
            iclist.append(np.sqrt(1-model_ic.rsquared))
        # wls结果合并
        tarr = np.array(tlist)
        rarr = np.array(rlist)
        table = {"因子": factor_name,
                 "|t|均值": np.mean(np.abs(tarr)),
                 "|t|>2占比": list(np.where(np.abs(tarr)>2, 1, 0)).count(1)/len(time),
                 "t均值": np.mean(tarr),
                 "t均值/t标准差": np.mean(tarr)/np.std(tarr),
                 "因子收益率均值": np.mean(rarr),
                 "因子收益率序列t检验": np.std(rarr)}
        table_reg0 = DataFrame(table, columns=["因子", "|t|均值", "|t|>2占比", "t均值", "t均值/t标准差",
                                               "因子收益率均值", "因子收益率序列t检验"], index=[i+1])



        # IC结果合并
        table_reg = pd.concat([table_reg, table_reg0])   # 纵向合并
        icarr = np.array(iclist)
        table1 = {"因子": factor_name,
                  "IC序列均值": np.mean(icarr),
                  "IC序列标准差": np.std(icarr),
                  "IR比率": np.mean(icarr)/np.std(icarr),
                  "IC>0占比": list(np.where(icarr>0, 1, 0)).count(1)/len(icarr),
                  "|IC|>0.02占比": list(np.where(np.abs(icarr)>0.02, 1, 0)).count(1)/len(icarr)}

        table_ic0 = DataFrame(table1, columns=["因子", "IC序列均值", "IC序列标准差", "IR比率",
                                               "IC>0占比", "|IC|>0.02占比"], index=[i+1])
        table_ic = pd.concat([table_ic, table_ic0])   # 纵向合并
    table_reg = table_reg.iloc[1:, :].set_index("因子")   # set_index 列变索引
    table_ic = table_ic.iloc[1:,:].set_index("因子")
    return table_reg, table_ic


def main():
    # pd.set_option("display.max_columns", 10)
    # data = pd.read_csv("onefactor.csv")
    # # print(type(data))
    # # factor_corr(data)
    # market = DataFrame(data.iloc[:, 0])
    # market = market.rename(columns={"time":"市值因子"})
    # t = signal_factor_test(data, market, market)   # 接收返回结果
    # print(t[0])
    # print(t[1])

    get_data()
    # fenceng()
    logout()


if __name__ == "__main__":
    main()
