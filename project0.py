# 该项目的作用：搭建一个通用的行情回测框架
import pandas as pd
import numpy as np
import re, os, csv

from package1 import Cal_index1 as ci



# 1、回测单品种趋势或震荡策略（基于行情数据；基于基本面数据）
# 1.1、计算进出场指标
# 1.1.1、以具体期货合约为例，基于rsi计算进出场指标
def rsi(delta):
    # 分类上涨和下跌
    up = delta.copy()
    down = delta.copy()
    up[up<0] = 0
    down[down>0] = 0
    
    # 计算平均上涨收益和平均下跌收益
    avg_up = up.mean()
    avg_down = abs(down.mean())
    
    # 计算rsi指标
    rs = avg_up / avg_down
    rsi = round(100 - (100 / (1 + rs)))

    return rsi
def function1_1_1(df1, period1):
    df2 = pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "settle", "volume", "turnover", "open_interest", "rsi", "index"])
    for code1 in set(df1.code):
        df3 = df1[df1.code==code1].sort_values(by="date")
        df3["rsi"] = df3["close"].diff(1)
        df3.dropna(axis=0, subset=["rsi"])
        df3["rsi"] = df3["rsi"].rolling(window=period1).apply(func=rsi)
        df2 = pd.concat([df2,df3], axis=0)

    df2["index"].loc[-(df2["rsi"].isnull()==True) & (df2["rsi"]>=75)] = 1 # -1表示做空，-2表示平仓
    df2["index"].loc[-(df2["rsi"].isnull()==True) & (df2["rsi"]<=65) & (df2["rsi"]>=60)] = 2 # -1表示做空，-2表示平仓
    df2["index"].loc[-(df2["rsi"].isnull()==True) & (df2["rsi"]<=25)] = -1 # 1表示做多，2表示平仓
    df2["index"].loc[-(df2["rsi"].isnull()==True) & (df2["rsi"]>=35) & (df2["rsi"]<=40)] = -2 # 1表示做多，2表示平仓
    df2["index"].loc[df2["index"].isnull()==True] = 0 # 0表示不行动

    return df2

# 1.1.2、以指数为例，找出历史行情的高低点
def market_data_filter(df1, period1): # 找出历史行情的高低点，df1的列名：date、price，period1为时间窗口的长度
    df1 = df1.sort_values(by="date")
    df1["result"] = df1["price"].rolling(window=period1, center=True).mean()
    df1 = df1.dropna(axis=0, subset=["result"])
    df1["high"] = df1["result"].rolling(window=period1, center=True).apply(lambda x: x.argmax() == period1//2, raw=False) # argmax()是一个内置函数，返回数组或一维数组元素中最大值对应的索引。
    df1["low"] = df1["result"].rolling(window=period1, center=True).apply(lambda x: x.argmin() == period1//2, raw=False)
    df1 = df1.dropna(axis=0, subset=["high", "low"])
    
    df2_1 = df1[["date", "result"]]
    df3 = df1[df1.high==1][["date", "result"]]
    df3.columns = ["date", "high"]
    df4 = df1[df1.low==1][["date", "result"]]
    df4.columns = ["date", "low"]
    point_num = df3.shape[0] + df4.shape[0]

    df2_2 = pd.merge(df2_1, df3, on="date", how="outer")
    df2_3 = pd.merge(df2_2, df4, on="date", how="outer")

    return [df2_3, point_num]
def function1_1_2(df1, start1, end1, step1): # 通过该函数找出market_data_filter()参数中合适的period1
    list1 = range(start1, end1, step1)
    list2 = []
    for i in list1:
        list3 = market_data_filter(df1, i)
        list2.append(list3[1])

    import matplotlib.pyplot as plt
    fig, ax1 = plt.subplots(figsize=(13, 7))
    ax1.plot(list1, list2, color=(75/255, 115/255, 165/255))
    plt.show()

    return True


# 1.2、整理好回测所用数据
# 1.2.1、以具体期货合约为例，用主力合约作为回测所用数据
def function1_2_1(df1):
    df2 = pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "settle", "volume", "turnover", "open_interest", "rsi", "index"])
    for date1 in sorted(set(df1.date)):
        df3 = df1[df1.date==date1].sort_values(by="volume", ascending=False)
        if df2.shape[0] == 0:
            df4 = df3.head(1)
        else:
            main_contract = df2.iloc[-1,0]
            if (ci.cal_date_spread(main_contract, date1)[1]) <= 1: # 如果合约离到期日小于等于1个月（不考虑天数）
                roll_df = df3[df3.code.apply(lambda x: True if ci.cal_date_spread(x, date1)[1] > 1 else False)] # 筛选出离到期日大于1个月的合约（不考虑天数）
                main_contract = roll_df.iloc[0,0]
                df4 = df3[df3.code==main_contract]
            else:
                largest_volume_contract = df3.iloc[0,0]
                if ci.compare_codes_maturity(largest_volume_contract,main_contract,date1)==1: # 如果最大交易量的合约比主力合约更晚到期
                    main_contract = largest_volume_contract
                    df4 = df3[df3.code==main_contract]
                else: # 不展期
                    df4 = df3[df3.code==main_contract]

        df2 = pd.concat([df2,df4], axis=0)

    return df2


# 1.3、计算盈亏
# 1.3.1、以具体期货合约为例
def function1_3_1(df1, account1, path1, scale1): # df1是行情数据及进出场指标，account1是初始账户权益，path1是账户数据的路径，scale1是合约规模
    # 初始化账户信息
    contract1 = ""
    price1 = 0
    open_interest1 = 0
    equity1 = account1
    pd.DataFrame(columns=["date", "contract", "price", "open_interest", "equity"]).to_csv(path1, mode="w", index=False)

    # 建立初始头寸
    n = 0
    for date1 in sorted(set(df1.date)):
        df2 = df1[df1.date==date1]
        index1 = df2.iloc[0, 11]
        if (open_interest1==0) & (index1==1): # 做多
            equity1 = account1
            contract1 = df2.iloc[0, 0]
            price1 = df2.iloc[0, 5]
            open_interest1 = round((+account1) / (price1*scale1)) # 仓位，正数表示做多
            df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
            df3.to_csv(path1, mode="a", index=False, header=False)
            n = n + 1
            break
        elif (open_interest1==0) & (index1==-1): # 做空
            equity1 = account1
            contract1 = df2.iloc[0, 0]
            price1 = df2.iloc[0, 5]
            open_interest1 = round((-account1) / (price1 * scale1)) # 仓位，负数表示做空
            df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
            df3.to_csv(path1, mode="a", index=False, header=False)
            n = n + 1
            break
        else:
            n = n + 1
            continue

    # 更新账户数据
    for date2 in sorted(set(df1.date))[n:]:
        df2 = df1[df1.date==date2]
        index1 = df2.iloc[0, 11]
        if open_interest1 != 0: # 账户持有仓位
            contract2 = df2.iloc[0, 0]
            if contract1 == contract2: # 主力合约没有变更
                if (index1==2) or (index1==-2): # 平仓
                    equity1 = open_interest1 * (df2.iloc[0, 5]-price1) * scale1 + equity1
                    date1 = date2
                    contract1 = ""
                    price1 = 0
                    open_interest1 = 0
                    df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
                    df3.to_csv(path1, mode="a", index=False, header=False)
                else: # 继续持有现有仓位
                    equity1 = open_interest1 * (df2.iloc[0, 5]-price1) * scale1 + equity1
                    date1 = date2
                    contract1 = contract2
                    price1 = df2.iloc[0, 5]
                    open_interest1 = open_interest1
                    df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
                    df3.to_csv(path1, mode="a", index=False, header=False)
            else: # 主力合约发生变更，需要强制平仓
                equity1 = equity1
                date1 = date2
                contract1 = ""
                price1 = 0
                open_interest1 = 0
                df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
                df3.to_csv(path1, mode="a", index=False, header=False)
        else: # 账户无仓位
            if index1 == 1:
                equity1 = equity1
                date1 = date2
                contract1 = contract2
                price1 = df2.iloc[0, 5]
                open_interest1 = round((+equity1) / (price1*scale1)) # +表示建立的是多头仓位
                df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
                df3.to_csv(path1, mode="a", index=False, header=False)
            elif index1 == -1:
                equity1 = equity1
                date1 = date2
                contract1 = contract2
                price1 = df2.iloc[0, 5]
                open_interest1 = round((-equity1) / (price1*scale1)) # -表示建立的是空头仓位
                df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
                df3.to_csv(path1, mode="a", index=False, header=False)
            else: # 继续保持空仓
                equity1 = equity1
                date1 = date2
                contract1 = ""
                price1 = 0
                open_interest1 = 0
                df3 = pd.DataFrame([[date1, contract1, price1, open_interest1, equity1]], columns=["date", "contract", "price", "open_interest", "equity"])
                df3.to_csv(path1, mode="a", index=False, header=False)

    return True

# 1.3.2、以指数为例
def function1_3_2(df1, path1): # df1是一个只有日期(date)、行情(price)和进出指标(index)三列的dataframe数据，代码逻辑比function1_3_1更简单，是用于对指数进行行情回测
    # 初始化账户信息
    date1 = sorted(set(df1.date))[0]
    price1 = df1[df1.date==date1].iloc[0, 1]
    equity1 = 1000
    pd.DataFrame([[date1, price1, equity1]], columns=["date", "price", "equity"]).to_csv(path1, mode="w", index=False)

    # 更新账户数据
    for date1 in sorted(set(df1.date))[1:]:
        df2 = df1[df1.date==date1]
        index1 = df2.iloc[0, 2]
        if index1 == 1: # 做多
            equity1 = df2.iloc[0, 1] / price1 * equity1
            price1 = df2.iloc[0, 1]
        elif index1 == -1: # 做空
            equity1 = price1 / df2.iloc[0, 1] * equity1
            price1 = df2.iloc[0, 1]
        elif index1 == 0: # 空仓
            equity1 = equity1
            price1 = df2.iloc[0, 1]

        df3 = pd.DataFrame([[date1, price1, equity1]], columns=["date", "price", "equity"])
        df3.to_csv(path1, mode="a", index=False, header=False)

    return True



# 2、回测横截面策略
# 2.1、计算进出场指标
# 2.1.1、以期货市场多品种会员持仓数据为例，基于(long-short)/open_interest计算进出场指标
from scipy.stats import percentileofscore
def function2_1_1(df1, period1): # df1是一个只有日期(date)、品种(code)、多方持仓(long)、空方持仓(short)、品种持仓(open_interest)和指标(rate)的dataframe数据
    df3 = pd.DataFrame()
    for code1 in sorted(set(df1.code)):
        df2 = df1[df1.code==code1]
        df2["mean_deviation"] = df2["rate"].rolling(window=period1).apply(lambda x: (x[-1]-np.mean(x))/np.mean(x), raw=True)
        df2["standard_factor"] = df2["rate"].rolling(window=period1).apply(lambda x: (x[-1]-np.mean(x))/np.std(x), raw=True)
        df2["quantile_value"] = df2["rate"].rolling(window=period1).apply(lambda x: percentileofscore(x, x[-1]), raw=True)

        df3 = pd.concat([df3, df2])

    return df3


# 2.2、整理好回测所用数据
# 2.2.1、以具体期货合约为例
def function2_2_1(df1): # df1是一个只有日期(date)、品种(code)和指标(index)的dataframe数据
    df4 = pd.DataFrame()
    for date1 in sorted(set(df1.date)):
        df2 = df1[df1.date==date1].sort_values(by="quantile_value")
        df2 = df2.dropna(axis=0, subset=["quantile_value"])
        df3 = df2.tail(1) # 用南华单个品种指数作为回测所用数据
        # df3 = df2.tail(round(df2.shape[0]/5)) # 用南华多个品种指数作为回测所用数据
        df3 = df3[["date", "code", "quantile_value"]]
        df4 = pd.concat([df4, df3])

    return df4


# 2.3、计算盈亏
# 2.3.1、单个品种持仓计算盈亏
def function2_3_1(df1): # df1是一个只有日期(date)、品种(code)和指标(index)的dataframe数据
    df2 = pd.read_excel("D:\\LearningAndWorking\\VSCode\\data\\index_data_commodity.xlsx", sheet_name="variety_open")
    s0 = sorted(set(df1.date))
    len1 = len(s0)
    s1 = s0[0:len1-2]
    s2 = s0[1:len1-1]
    s3 = s0[2:len1]
    list1 = []
    for date1,date2,date3 in zip(s1,s2,s3):
        code1 = df1[df1.date==date1].iloc[0,1]
        if code1 == "PM":
            continue
        elif date1=='2016-06-22' or date2=='2016-06-22' or date3=='2016-06-22':
            continue
        else:
            open1 = float(df2[df2.DATE==date2][code1])
            open2 = float(df2[df2.DATE==date3][code1])
            return1 = open2/open1 - 1
            list1.append([date3, code1, return1])
    df3 = pd.DataFrame(list1, columns=["date", "code", "return"])

    return df3

# 2.3.2、多个品种持仓计算盈亏
def function2_3_2(df1): # df1是一个只有日期(date)、品种(code)和指标(index)的dataframe数据
    df2 = pd.read_excel("D:\\LearningAndWorking\\VSCode\\data\\index_data_commodity.xlsx", sheet_name="variety_open")
    s0 = sorted(set(df1.date))
    len1 = len(s0)
    s1 = s0[0:len1-2]
    s2 = s0[1:len1-1]
    s3 = s0[2:len1]
    list1 = []
    for date1,date2,date3 in zip(s1,s2,s3):
        df3 = df1[df1.date==date1]
        list2 = []
        if "PM" in list(df3.code):
            continue
        elif date1=="2016-06-22" or date2=="2016-06-22" or date3=="2016-06-22":
            continue
        else:
            for code1 in df3.code:
                open1 = float(df2[df2.DATE==date2][code1])
                open2 = float(df2[df2.DATE==date3][code1])
                return1 = open2/open1 - 1
                list2.append(return1)
        return2 = sum(list2)/len(list2)
        list1.append([date3, return2])
    df4 = pd.DataFrame(list1, columns=["date", "return"])

    return df4



# 3、回测单品种跨月套利策略



# 4、回测多品种套利策略
# 4.1、计算进出场指标


# 4.2、整理好回测所用数据
# 4.2.1、以期货与期权之间的套利为例
def function4_2_1_1(code, date): # code格式如"TA"，date格式如"2020-01-01"，把满足流动性要求的期货和期权挑选出来
    df1_1 = pd.read_csv("D:\\LearningAndWorking\\VS\\data\\期货合约日级数据（2023）\\" + code + ".csv")
    df1_1 = df1_1[df1_1.date >= date]
    df2_1 = pd.read_csv("D:\\LearningAndWorking\\VS\\data\\期权合约日级数据（2023）\\" + code + "_option.csv")
    # 股指期货和股指期货期权的代码不同，可以采用以下方式处理
    # df2_1 = pd.read_csv("D:\\LearningAndWorking\\VS\\data\\期权合约日级数据（2023）\\IO_option.csv")
    # df2_1.code = df2_1.code.str.replace("^IO", "IF", regex=True)
    df2_1 = df2_1[(df2_1.date >= date) & (df2_1.volume > 0)]

    df1_3 = pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "settle", "volume", "turnover", "open_interest"])
    for date1 in sorted(set(df1_1.date)):
        df1_2 = df1_1[df1_1.date==date1].sort_values(by="open_interest", ascending=False)
        df1_3 = pd.concat([df1_3, df1_2.iloc[0:2,:]])

    df2_4 = pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "settle", "volume", "turnover", "open_interest"])
    for date1 in sorted(set(df2_1.date)):
        df2_2 = df2_1[df2_1.date==date1]
        for code1 in list(df1_3[df1_3.date==date1].code):
            df2_3 = df2_2.query("code.str.contains('^" + code1 + "', regex=True)", engine="python")
            df2_4 = pd.concat([df2_4, df2_3])
    
    df_C = df2_4.query("code.str.contains('[0-9][C][0-9]', regex=True)", engine="python")
    df_P = df2_4.query("code.str.contains('[0-9][P][0-9]', regex=True)", engine="python")

    return df1_3, df_C, df_P
def function4_2_1_2(df1_1, df2_1, df3_1): # code格式如"TA"，在process_data1()的基础上把满足无风险套利策略的期货和期权挑选出来，df1_1对应df1_3，df2_1对应df_C，df3_1对应df_P
    df5_1 = pd.DataFrame(columns=["date", "future_contract", "future_price", "K", "C_option_contract", "C_option_price", "volume1", "P_option_contract", "P_option_price", "volume2", "spread"])
    for date1 in sorted(set(df2_1.date)):
        df1_2 = df1_1[df1_1.date==date1].sort_values(by="volume", ascending=False)
        df2_2 = df2_1[df2_1.date==date1].sort_values(by="volume", ascending=False)
        df2_2 = df2_2.iloc[0:round(df2_2.shape[0]/3),:] # 取成交量前1/3的买权合约
        df3_2 = df3_1[df3_1.date==date1].sort_values(by="volume", ascending=False)
        S_minus_K = []
        for code1_1 in df2_2.code:
            code1_2 = re.findall(pattern="^[A-Z]{1,2}[0-9]{3,4}", string=code1_1)[0]
            direction1 = re.findall(pattern="([0-9])([C|P])([0-9])", string=code1_1)[0][1]
            K = re.findall(pattern="[0-9]+$", string=code1_1)[0]
            S = float(df1_2[df1_2.code==code1_2].iloc[0, 5])
            if direction1=="C":
                option_close1 = float(df2_2[df2_2.code==code1_1].iloc[0, 5])
                volume1 = float(df2_2[df2_2.code==code1_1].iloc[0, 7])
                code1_3 = code1_2 + "P" + K

                try:
                    option_close2 = float(df3_2[df3_2.code==code1_3].iloc[0, 5])
                    volume2 = float(df3_2[df3_2.code==code1_3].iloc[0, 7])
                except:
                    break
                
                spread1 = (option_close1-option_close2) - (S-float(K)) # 计算(C-P)-(S-K)
                if (spread1<0) and (code1_3 in list(df3_2.iloc[0:round(df3_2.shape[0]/3),:].code)): # 判断与符合要求的买权合约对应的卖权合约成交量是否在前1/3
                    S_minus_K.append([date1, code1_2, S, float(K), code1_1, option_close1, volume1, code1_3, option_close2, volume2, spread1])
        
        df4_1 = pd.DataFrame(S_minus_K, columns=["date", "future_contract", "future_price", "K", "C_option_contract", "C_option_price", "volume1", "P_option_contract", "P_option_price", "volume2", "spread"])
        df4_1 = df4_1.sort_values(by="spread", ascending=True)
        if df4_1.shape[0] > 0:
            df5_1 = pd.concat([df5_1, df4_1])

    return df5_1


# 4.3、计算盈亏
# 4.3.1、以期货与期权之间的套利为例
def function4_3_1():
    # 资金利用效率：((C_option_price-P_option_price) - (future_price-K)) / (future_price*scale*leverage + C_option_price - P_option_price)
    code1 = "TA"
    path1 = "D:\\LearningAndWorking\\VSCode\\python\\project3\\result.csv"

    df1 = pd.read_csv("C:\\Users\\29433\\Desktop\\result.csv")
    df1_future = pd.read_csv("D:\\LearningAndWorking\\VS\\data\\期货合约日级数据（2023）\\" + code1 + ".csv")
    df1_option = pd.read_csv("D:\\LearningAndWorking\\VS\\data\\期权合约日级数据（2023）\\" + code1 + "_option.csv")
    date_list1 = sorted(set(df1_option.date))
    date_list2 = sorted(set(df1.date))

    with open(path1, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows([["date", "state", "future", "future_price", "C_option", "C_option_price", "P_option", "P_option_price", "spread", "net_value"]])
    df2 = pd.DataFrame(columns=["date", "state", "future", "future_price", "C_option", "C_option_price", "P_option", "P_option_price", "spread", "net_value"])
    for date1 in date_list1:
        df2_future = df1_future[df1_future.date==date1]
        df2_option = df1_option[df1_option.date==date1]
        if date1 == date_list2[0]:
            state = 1
            future = df1[df1.date==date1].iloc[0, 1]
            future_price = df2_future[df2_future.code==future].iloc[0, 5]
            C_option = df1[df1.date==date1].iloc[0, 4]
            C_option_price = df2_option[df2_option.code==C_option].iloc[0, 5]
            P_option = df1[df1.date==date1].iloc[0, 7]
            P_option_price = df2_option[df2_option.code==P_option].iloc[0, 5]
            spread1 = df1.iloc[0, 10]
            net_value = 1000
            
            list1 = [date1, state, future, future_price, C_option, C_option_price, P_option, P_option_price, spread1, net_value]
            with open(path1, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows([list1])

            df3 = pd.DataFrame([list1])
            df2 = pd.concat([df2, df3])

        elif date1 > date_list2[0]:
            pre_state = df2.iloc[-1, 1]
            pre_future = df2.iloc[-1, 2]
            pre_future_price = df2.iloc[-1, 3]
            pre_C_option = df2.iloc[-1, 4]
            pre_C_option_price = df2.iloc[-1, 5]
            pre_P_option = df2.iloc[-1, 6]
            pre_P_option_price = df2.iloc[-1, 7]
            pre_spread = df2.iloc[-1, 8]
            pre_net_value = df2.iloc[-1, 9]
            if pre_state == 1: # 已有持仓
                future_price = df2_future[df2_future.code==pre_future].iloc[0, 5]
                C_option_price = df2_option[df2_option.code==pre_C_option].iloc[0, 5]
                P_option_price = df2_option[df2_option.code==pre_P_option].iloc[0, 5]
                K = float(re.findall(pattern="[0-9]+$", string=pre_C_option)[0])
                spread1 = (C_option_price-P_option_price) - (future_price-K) # 计算(C-P)-(S-K)
                net_value = (((pre_future_price-future_price)+(pre_P_option_price-P_option_price)+(C_option_price-pre_C_option_price)) / (pre_future_price-pre_P_option_price+pre_C_option_price) + 1) * pre_net_value
                if date1 in date_list2: # 有需要判断的建仓信号出现
                    state = 1
                    spread2 = df1[df1.date==date1].iloc[0, 10]
                    if (spread1>spread2) or (ci.cal_date_spread(pre_future, date1)[2] <= 30): # 如果新的建仓信号利润更大或者已有持仓已临近到期，就平旧仓建新仓
                        list1 = [date1, state, future, future_price, C_option, C_option_price, P_option, P_option_price, spread1, net_value]
                        
                        future = df1[df1.date==date1].iloc[0, 1]
                        future_price = df2_future[df2_future.code==future].iloc[0, 5]
                        C_option = df1[df1.date==date1].iloc[0, 4]
                        C_option_price = df2_option[df2_option.code==C_option].iloc[0, 5]
                        P_option = df1[df1.date==date1].iloc[0, 7]
                        P_option_price = df2_option[df2_option.code==P_option].iloc[0, 5]

                        list2 = [date1, state, future, future_price, C_option, C_option_price, P_option, P_option_price, spread2, net_value]
                        with open(path1, "a", newline="") as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerows([list1, list2])

                        df3 = pd.DataFrame([list1, list2])
                        df2 = pd.concat([df2, df3])

                    else:
                        list1 = [date1, state, pre_future, future_price, pre_C_option, C_option_price, pre_P_option, P_option_price, spread1, net_value]
                        with open(path1, "a", newline="") as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerows([list1])

                        df3 = pd.DataFrame([list1])
                        df2 = pd.concat([df2, df3])

                else:
                    if (spread1>=0) or (ci.cal_date_spread(pre_future, date1)[2] <=30): # 如果利润已经取得或者已有持仓已临近到期，就平仓
                        state = 0

                        list1 = [date1, state, pre_future, future_price, pre_C_option, C_option_price, pre_P_option, P_option_price, spread1, net_value]
                        with open(path1, "a", newline="") as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerows([list1])

                        df3 = pd.DataFrame([list1])
                        df2 = pd.concat([df2, df3])

                    else:
                        list1 = [date1, pre_state, pre_future, future_price, pre_C_option, C_option_price, pre_P_option, P_option_price, spread1, net_value]
                        with open(path1, "a", newline="") as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerows([list1])
                        
                        df3 = pd.DataFrame([list1])
                        df2 = pd.concat([df2, df3])

            else:
                if date1 in date_list2: # 有需要判断的建仓信号出现
                    state = 1
                    future = df1[df1.date==date1].iloc[0, 1]
                    future_price = df2_future[df2_future.code==future].iloc[0, 5]
                    C_option = df1[df1.date==date1].iloc[0, 4]
                    C_option_price = df2_option[df2_option.code==C_option].iloc[0, 5]
                    P_option = df1[df1.date==date1].iloc[0, 7]
                    P_option_price = df2_option[df2_option.code==P_option].iloc[0, 5]
                    spread1 = df1[df1.date==date1].iloc[0, 10]

                    list1 = [date1, state, future, future_price, C_option, C_option_price, P_option, P_option_price, spread1, net_value]
                    with open(path1, "a", newline="") as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows([list1])

                    df3 = pd.DataFrame([list1])
                    df2 = pd.concat([df2, df3])
                
                else:
                    state = 0
                    future = ""
                    future_price = 0
                    C_option = ""
                    C_option_price = 0
                    P_option = ""
                    P_option_price = 0
                    spread1 = 0
                    net_value = df2.iloc[-1, 9]

                    list1 = [date1, state, future, future_price, C_option, C_option_price, P_option, P_option_price, spread1, net_value]
                    with open(path1, "a", newline="") as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows([list1])

                    df3 = pd.DataFrame([list1])
                    df2 = pd.concat([df2, df3])

    return True



if __name__ == "__main__":
    pass


