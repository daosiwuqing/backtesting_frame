﻿# 该项目的作用：构建南华综合指数（当日判定是否展期，并在收盘时展期）
import threading as th
import pandas as pd
import zipfile as zp
import datetime as dt
import csv, io

from package1 import Cal_index1 as ci



# 获取所用合约的所有行情数据
# 数据源为日频数据
def get_market_data1(index_name1, weight_date_list1): # index_name1指数名称，weight_date_list1回测涉及到的权重调整日期
    codes_list1 = []
    for weight_date1 in weight_date_list1[:-1]:
        weight1 = ci.get_weight(index_name1, weight_date1)
        codes_list1.extend(list(weight1.index))
    
    df1 = pd.DataFrame()
    for code1 in set(codes_list1):
        file_name1 = code1 + ".csv"
        df2 = pd.read_csv("D:\\LearningAndWorking\\VS\\data\\期货合约日级数据（2023）\\" + file_name1)
        df1 = pd.concat([df1, df2])
    
    return df1

# 数据源为高频数据
def get_market_data2(index_name1, weight_date_list1, time1): # index_name1指数名称，weight_date_list1回测涉及到的权重调整日期，time1时间点
    codes_list1 = []
    for weight_date1 in weight_date_list1[:-1]:
        weight1 = ci.get_weight(index_name1, weight_date1)
        codes_list1.extend(list(weight1.index))

    df1 = pd.DataFrame()
    with zp.ZipFile("D:\\LearningAndWorking\\VS\\data\\全部期货合约一分钟级数据（23_12_21-24_04_26）.zip") as zf1:
        for code1 in set(codes_list1):
            content1 = zf1.read(code1 + ".csv")
            text1 = content1.decode(encoding = "utf-8")
            df2 = pd.read_csv(io.StringIO(text1))
            df2 = df2[df2.time == time1]

            df2 = df2.drop(columns="time")
            df2[["date", "code"]] = df2[["code", "date"]]
            df2 = df2.rename(columns={"date":"code", "code":"date"})

            df1 = pd.concat([df1, df2])

    return df1



# 计算第一个交易日的指数
def calculate_function1(df1, df2, df3, weight1, date1, path1): # df1当前行情数据，df2历史行情数据，df3历史指数数据，weight1权重信息，date1日期，path1数据输出路径
    df1 = df1[df1["date"]==date1]

    # 纳入指数计算的品种的基本信息
    df4 = ci.add_contract_infor(weight1)
    df4.columns = ["weight", "scale", "margin_ratio"]

    # 更新指数信息
    list1 = []
    for code1 in df4.index:        
        # 品种基本信息
        weight = float(df4.loc[code1,["weight"]]) # 品种权重

        # 对应品种的行情信息
        df5 = df1.query("code.str.contains('^" + code1 + "[0-9]{3,4}', regex=True)", engine="python")
        df5 = df5.sort_values(by="open_interest", ascending=False) # 按持仓量进行排序
        
        # 当日指数信息
        roll_state = 0 # 展期状态
        main_contract = df5.iloc[0,0] # 主力合约
        main_contract_price = df5.iloc[0,5] # 主力合约价格
        largest_open_interest_contract = main_contract # 主力合约就是持仓量最大的合约
        second_contract = "" # 次主力合约
        second_contract_price = 0 # 次主力合约价格
        r1 = 0 * weight
        list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])

    # 数据输出
    df6 = pd.DataFrame(list1)
    df6.columns = ["date", "code", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "return"]
    index = 1000 # 初始指数点位
    df6["index"] = index * (1 + sum(df6["return"])) # 指数点位
    list2 = df6.values.tolist()
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(list2)

    # 更新变量
    if df2.shape[0] > 600:
        df2 = pd.concat([df2.iloc[1:,:], df1])
        df3 = pd.concat([df3.iloc[1:,:], df6])
    else:
        df2 = pd.concat([df2, df1])
        df3 = pd.concat([df3, df6])

    return df2, df3



# 计算第二、三个交易日的账户净值
def calculate_function2(df1, df2, df3, weight1, date1, path1): # df1当前行情数据，df2历史行情数据，df3历史指数数据，weight1权重信息，date1日期，path1数据输出路径
    df1 = df1[df1["date"]==date1]
    
    # 纳入指数计算的品种的基本信息
    df4 = ci.add_contract_infor(weight1)
    df4.columns = ["weight", "scale", "margin_ratio"]
    
    # 更新指数信息
    index = df3.iloc[-1,9]
    list1 = []
    for code1 in df4.index:
        # 前一交易日的持仓信息
        pre_main_contract = df3[df3["code"]==code1].iloc[-1,2]
        pre_main_contract_price = df3[df3["code"]==code1].iloc[-1,3]
        
        # 品种基本信息
        weight = float(df4.loc[code1,["weight"]]) # 品种权重
        
        # 对应品种的行情信息
        df5 = df1.query("code.str.contains('^" + code1 + "[0-9]{3,4}', regex=True)", engine="python")
        df5 = df5.sort_values(by="open_interest", ascending=False) # 按持仓量进行排序
        
        # 当日持仓信息
        roll_state = 0 # 展期状态
        main_contract = pre_main_contract # 主力合约
        main_contract_price = df5[df5["code"]==main_contract].iloc[0,5] # 主力合约价格
        largest_open_interest_contract = df5.iloc[0,0] # 持仓量最大的合约
        second_contract = "" # 次主力合约
        second_contract_price = 0 # 次主力合约价格
        r1 = (main_contract_price/pre_main_contract_price - 1) * weight

        list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])

    # 数据输出
    df6 = pd.DataFrame(list1)
    df6.columns = ["date", "code", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "return"]
    index = index * (1 + sum(df6["return"])) # 指数点位
    df6["index"] = index
    list2 = df6.values.tolist()
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(list2)

    # 更新变量
    if df2.shape[0] > 600:
        df2 = pd.concat([df2.iloc[1:,:], df1])
        df3 = pd.concat([df3.iloc[1:,:], df6])
    else:
        df2 = pd.concat([df2, df1])
        df3 = pd.concat([df3, df6])

    return df2, df3



# 计算非权重调整日的账户净值
def calculate_weight_constant_function(df1, df2, df3, weight1, date1, path1): # df1当前行情数据，df2历史行情数据，df3历史指数数据，weight1权重信息，date1日期，path1数据输出路径
    df1 = df1[df1["date"]==date1]

    # 纳入指数计算的品种的基本信息
    df4 = ci.add_contract_infor(weight1)
    df4.columns = ["weight", "scale", "margin_ratio"]

    # 更新指数信息
    index = df3.iloc[-1,9]
    list1 = []
    for code1 in df4.index:
        # 前一个交易日的持仓信息
        pre_roll_state = df3[df3["code"]==code1].iloc[-1,4]
        pre_main_contract = df3[df3["code"]==code1].iloc[-1,2]
        pre_main_contract_price = df3[df3["code"]==code1].iloc[-1,3]
        pre_largest_open_interest_contract = df3[df3["code"]==code1].iloc[-1,7]
        pre_second_contract = df3[df3["code"]==code1].iloc[-1,5]
        pre_second_contract_price = df3[df3["code"]==code1].iloc[-1,6]

        # 品种基本信息
        weight = float(df4.loc[code1,["weight"]]) # 品种权重
        
        # 对应品种的行情信息
        df5 = df1.query("code.str.contains('^" + code1 + "[0-9]{3,4}', regex=True)", engine="python")
        df5 = df5.sort_values(by="open_interest", ascending=False) # 按持仓量进行排序

        if pre_roll_state == 0:
            # 当日持仓信息
            main_contract = pre_main_contract # 主力合约
            main_contract_price = df5[df5["code"]==pre_main_contract].iloc[0,5] # 主力合约价格
            largest_open_interest_contract = df5.iloc[0,0] # 持仓量最大的合约
            if (ci.cal_date_spread(main_contract, date1)[1]) <= 1: # 如果合约离到期日小于等于1个月（不考虑天数）
                roll_df = df5[df5.code.apply(lambda x: True if ci.cal_date_spread(x, date1)[1] > 1 else False)] # 筛选出离到期日大于1个月的合约（不考虑天数）
                roll_state = 1 # 展期状态
                second_contract = roll_df.iloc[0,0]
                second_contract_price = df5[df5["code"]==second_contract].iloc[0,5]
            else:
                if ci.compare_codes_maturity(pre_largest_open_interest_contract,pre_main_contract,date1)==1 and len(set(list(df3[df3["code"]==code1].iloc[-3:,7])))==1: # 如果最大持仓量的合约比主力合约更晚到期且最大持仓量的合约连续三天是同一合约
                    roll_state = 1 # 展期状态
                    second_contract = pre_largest_open_interest_contract
                    second_contract_price = df5[df5["code"]==second_contract].iloc[0,5]
                else: # 不展期
                    roll_state = 0
                    second_contract = "" # 次主力合约
                    second_contract_price = 0 # 次主力合约价格
            r1 = (main_contract_price/pre_main_contract_price - 1) * weight
            list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])
        elif pre_roll_state == 1:
            main_contract = pre_main_contract
            main_contract_price = df5[df5["code"]==pre_main_contract].iloc[0,5] # 主力合约价格
            largest_open_interest_contract = df5.iloc[0,0] # 持仓量最大的合约
            roll_state = 2 # 展期状态
            second_contract = pre_second_contract # 次主力合约
            second_contract_price = df5[df5["code"]==pre_second_contract].iloc[0,5] # 次主力合约价格
            r1 = ((main_contract_price/pre_main_contract_price - 1) * 0.8 + (second_contract_price/pre_second_contract_price -1) * 0.2) * weight
            list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])
        elif pre_roll_state == 2:
            main_contract = pre_main_contract
            main_contract_price = df5[df5["code"]==pre_main_contract].iloc[0,5] # 主力合约价格
            largest_open_interest_contract = df5.iloc[0,0] # 持仓量最大的合约
            roll_state = 3 # 展期状态
            second_contract = pre_second_contract # 次主力合约
            second_contract_price = df5[df5["code"]==pre_second_contract].iloc[0,5] # 次主力合约价格
            r1 = ((main_contract_price/pre_main_contract_price - 1) * 0.6 + (second_contract_price/pre_second_contract_price -1) * 0.4) * weight
            list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])
        elif pre_roll_state == 3:
            main_contract = pre_main_contract
            main_contract_price = df5[df5["code"]==pre_main_contract].iloc[0,5] # 主力合约价格
            largest_open_interest_contract = df5.iloc[0,0] # 持仓量最大的合约
            roll_state = 4 # 展期状态
            second_contract = pre_second_contract # 次主力合约
            second_contract_price = df5[df5["code"]==pre_second_contract].iloc[0,5] # 次主力合约价格
            r1 = ((main_contract_price/pre_main_contract_price - 1) * 0.4 + (second_contract_price/pre_second_contract_price -1) * 0.6) * weight
            list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])
        elif pre_roll_state == 4:
            main_contract = pre_second_contract
            main_contract_price = df5[df5["code"]==main_contract].iloc[0,5] # 主力合约价格
            largest_open_interest_contract = df5.iloc[0,0] # 持仓量最大的合约
            roll_state = 5 # 展期状态
            second_contract = "" # 次主力合约
            second_contract_price = 0 # 次主力合约价格
            r1 = ((df5[df5["code"]==pre_main_contract].iloc[0,5]/pre_main_contract_price - 1) * 0.2 + (main_contract_price/pre_second_contract_price -1) * 0.8) * weight
            list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])
        elif pre_roll_state == 5:
            main_contract = pre_main_contract
            main_contract_price = df5[df5["code"]==pre_main_contract].iloc[0,5] # 主力合约价格
            largest_open_interest_contract = df5.iloc[0,0] # 持仓量最大的合约
            roll_state = 0 # 展期状态
            second_contract = "" # 次主力合约
            second_contract_price = 0 # 持仓量最大的合约
            r1 = (main_contract_price/pre_main_contract_price - 1) * weight
            list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])
    
    # 数据输出
    df6 = pd.DataFrame(list1)
    df6.columns = ["date", "code", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "return"]
    index = index * (1 + sum(df6["return"])) # 指数点位
    df6["index"] = index
    list2 = df6.values.tolist()
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(list2)
    
    # 更新变量
    if df2.shape[0] > 600:
        df2 = pd.concat([df2.iloc[1:,:], df1])
        df3 = pd.concat([df3.iloc[1:,:], df6])
    else:
        df2 = pd.concat([df2, df1])
        df3 = pd.concat([df3, df6])

    return df2, df3



# 计算权重调整日的账户净值
def calculate_weight_change_function(df1, df2, df3, weight1, date1, path1): # df1当前行情数据，df2历史行情数据，df3历史指数数据，weight1权重信息，date1日期，path1数据输出路径
    df1 = df1[df1["date"]==date1]
    
    # 纳入指数计算的品种的基本信息
    df4 = ci.add_contract_infor(weight1)
    df4.columns = ["weight", "scale", "margin_ratio"]
    
    # 计算当日指数信息，因为权重调整日的历史持仓品种和将要持仓品种可能不同，此处与其它函数计算的代码略有不同
    index = df3.iloc[-1,9]
    r1_list = []
    for code1 in df3[df3["date"]==date1].code:
        # 前一个交易日的指数信息
        pre_roll_state = df3[df3["code"]==code1].iloc[-1,4]
        pre_main_contract = df3[df3["code"]==code1].iloc[-1,2]
        pre_main_contract_price = df3[df3["code"]==code1].iloc[-1,3]
        pre_second_contract = df3[df3["code"]==code1].iloc[-1,5]
        pre_second_contract_price = df3[df3["code"]==code1].iloc[-1,6]
        
        weight = float(df4.loc[code1,["weight"]]) # 合约规模

        # 对应品种的行情信息
        df5 = df1.query("code.str.contains('^" + code1 + "[0-9]{3,4}', regex=True)", engine="python")
        df5 = df5.sort_values(by="open_interest", ascending=False) # 按持仓量进行排序

        # 当日指数信息
        if pre_roll_state == 1:
            r1_list.append(((df5[df5["code"]==pre_main_contract].iloc[0,5]/pre_main_contract_price - 1) * 0.8 + (df5[df5["code"]==pre_second_contract].iloc[0,5]/pre_second_contract_price - 1) * 0.2) * weight)
        elif pre_roll_state == 2:
            r1_list.append(((df5[df5["code"]==pre_main_contract].iloc[0,5]/pre_main_contract_price - 1) * 0.6 + (df5[df5["code"]==pre_second_contract].iloc[0,5]/pre_second_contract_price - 1) * 0.4) * weight)
        elif pre_roll_state == 3:
            r1_list.append(((df5[df5["code"]==pre_main_contract].iloc[0,5]/pre_main_contract_price - 1) * 0.4 + (df5[df5["code"]==pre_second_contract].iloc[0,5]/pre_second_contract_price - 1) * 0.6) * weight)
        elif pre_roll_state == 4:
            r1_list.append(((df5[df5["code"]==pre_main_contract].iloc[0,5]/pre_main_contract_price - 1) * 0.2 + (df5[df5["code"]==pre_second_contract].iloc[0,5]/pre_second_contract_price - 1) * 0.8) * weight)
        else:
            r1_list.append((df5[df5["code"]==pre_main_contract].iloc[0,5]/pre_main_contract_price - 1) * weight)
    
    # 更新指数信息
    list1 = []
    for code1 in df4.index:
        # 品种基本信息
        weight = float(df4.loc[code1,["weight"]]) # 品种权重
        
        # 对应品种的行情信息
        df5 = df1.query("code.str.contains('^" + code1 + "[0-9]{3,4}', regex=True)", engine="python")
        df5 = df5.sort_values(by="open_interest", ascending=False) # 按持仓量进行排序

        # 当日持仓信息
        main_contract = df5.iloc[0,0] # 主力合约
        main_contract_price = df5.iloc[0,5] # 主力合约价格
        largest_open_interest_contract = main_contract # 主力合约就是持仓量最大的合约
        roll_state = 0 # 展期状态
        second_contract = "" # 次主力合约
        second_contract_price = 0 # 次主力合约价格
        r1 = 0 * weight
        list1.append([date1, code1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, r1])

    # 数据输出
    df6 = pd.DataFrame(list1)
    df6.columns = ["date", "code", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "return"]
    index = index * (1 + sum(r1_list)) # 指数点位
    df6["index"] = index
    list2 = df6.values.tolist()
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(list2)
    
    # 更新变量
    if df2.shape[0] > 600:
        df2 = pd.concat([df2.iloc[1:,:], df1])
        df3 = pd.concat([df3.iloc[1:,:], df6])
    else:
        df2 = pd.concat([df2, df1])
        df3 = pd.concat([df3, df6])

    return df2, df3



def main(index_name1, weight_date_list1, path1):
    df = get_market_data1(index_name1, weight_date_list1) # 获取所用合约的所有行情数据
    date_list1 = sorted(set(df[df.date >= weight_date_list1[0]]["date"])) # 生成一个回测用的日期流

    # 初始化部分参数并计算前三个交易日的账户净值
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    weight1 = ci.get_weight(index_name1, weight_date_list1[0])
    df2, df3 = calculate_function1(df[df.date==date_list1[0]], df2, df3, weight1, date_list1[0], path1)
    df2, df3 = calculate_function2(df[df.date==date_list1[1]], df2, df3, weight1, date_list1[1], path1)
    df2, df3 = calculate_function2(df[df.date==date_list1[2]], df2, df3, weight1, date_list1[2], path1)

    n1 = 1
    for date1 in date_list1[3:]:
        df1 = df[df.date==date1]
        if dt.datetime.strptime(date1, "%Y-%m-%d") >= dt.datetime.strptime(weight_date_list1[n1], "%Y-%m"): # 判断是否需要调整权重
            weight1 = ci.get_weight(index_name1, weight_date_list1[n1]) # 更新权重信息
            df2, df3 = calculate_weight_change_function(df1, df2, df3, weight1, date1, path1)
            n1 = n1 + 1
        else:
            df2, df3 = calculate_weight_constant_function(df1, df2, df3, weight1, date1, path1)



if __name__ == "__main__":
    index_name1 = "综合指数" # 指数名称
    # 历史权重调整的日期，格式为"YYYY-mm"，最后还要补充一个下一次调整权重的日期
    weight_date_list1 = ["2004-06", "2005-06", "2006-06", "2007-06", "2008-06", "2009-06", "2010-06", "2011-06", "2012-06", 
                         "2012-09", "2013-06", "2014-06", "2015-03", "2015-06", "2016-06", "2017-06", "2018-06", "2019-06", 
                         "2020-06", "2021-06", "2022-06", "2022-09", "2023-06", "2024-06"]
    path1 = "D:\\LearningAndWorking\\VSCode\\python\\project2\\" + index_name1 + ".csv" # 数据输出路径

    list1 = ["date", "code", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "return", "index"]
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list1)

    main(index_name1, weight_date_list1, path1)


