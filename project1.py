# 该项目的作用：构建南华单品种指数（当日判定是否展期，并在收盘时展期）
import threading as th
import pandas as pd
import zipfile as zp
import io, csv

from package1 import Cal_index1 as ci



# 获取所用合约的所有行情数据
# 数据源为日频数据
def get_market_data1(code1): # code1是品种代码
    file_name1 = code1 + ".csv"
    df1 = pd.read_csv("D:\\LearningAndWorking\\VSCode\\data\\期货合约日级数据（2023）\\" + file_name1)
    
    return df1

# 数据源为高频数据
def get_market_data2(code1, time1): # code1是品种代码，time1是时间点，格式为"15:00:00"
    with zp.ZipFile("D:\\LearningAndWorking\\VSCode\\data\\全部期货合约一分钟级数据（2005-2023）.zip") as zf1:
        content1 = zf1.read(code1 + ".csv")
        text1 = content1.decode(encoding = "utf-8")
        df1 = pd.read_csv(io.StringIO(text1))
        df1 = df1[df1.time == time1]

        df1 = df1.drop(columns="time")
        df1[["date", "code"]] = df1[["code", "date"]]
        df1 = df1.rename(columns={"date":"code", "code":"date"})

    return df1



# 展期计算函数：df1当前行情数据，df2历史行情数据，df3历史指数数据，date1日期，path1数据输出路径，kind展期类型（0为正常展期，1强制展期）
def roll_calculate_function(df1, df2, df3, date1, path1, kind=0):
    # 1、进行展期
    if df3.iloc[-1,3] == 0:
        if kind == 0:
            pre_main_contract = df3.iloc[-1,1]
            pre_main_contract_price = df3.iloc[-1,2]
            pre_largest_open_interest_contract = df3.iloc[-1,6]
            pre_index = df3.iloc[-1,7]

            roll_state = 1
            main_contract = pre_main_contract
            main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
            largest_open_interest_contract = df1.iloc[0,0]

            # 由于金融期货的特殊性，特写该段代码，也适用于商品期货的强制展期
            alternative_second_contract1 = df1.iloc[1,0]
            alternative_second_contract2 = df1.iloc[2,0]
            if ci.compare_codes_maturity(main_contract,pre_largest_open_interest_contract,date1) == 0:
                second_contract = pre_largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,largest_open_interest_contract,date1) == 0:
                second_contract = largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,alternative_second_contract1,date1) == 0:
                second_contract = alternative_second_contract1
            else:
                second_contract = alternative_second_contract2    

            pre_second_contract_price = df2[df2["code"]==second_contract].iloc[-1,5]
            second_contract_price = df1[df1["code"]==second_contract].iloc[0,5]
            index = pre_index*((main_contract_price/pre_main_contract_price)*0.8 + (second_contract_price/pre_second_contract_price)*0.2)
        else:
            pre_main_contract = df3.iloc[-1,1]
            pre_main_contract_price = df3.iloc[-1,2]
            pre_largest_open_interest_contract = df3.iloc[-1,6]
            pre_index = df3.iloc[-1,7]

            roll_state = 1
            main_contract = pre_main_contract
            main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
            largest_open_interest_contract = df1.iloc[0,0]

            # 由于金融期货的特殊性，特写该段代码，也适用于商品期货的强制展期
            alternative_second_contract1 = df1.iloc[1,0]
            alternative_second_contract2 = df1.iloc[2,0]
            if ci.compare_codes_maturity(main_contract,pre_largest_open_interest_contract,date1) == 0:
                second_contract = pre_largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,largest_open_interest_contract,date1) == 0:
                second_contract = largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,alternative_second_contract1,date1) == 0:
                second_contract = alternative_second_contract1
            else:
                second_contract = alternative_second_contract2  

            pre_second_contract_price = df2[df2["code"]==second_contract].iloc[-1,5]
            second_contract_price = df1[df1["code"]==second_contract].iloc[0,5]
            index = pre_index*((main_contract_price/pre_main_contract_price)*0.8 + (second_contract_price/pre_second_contract_price)*0.2)
    elif df3.iloc[-1,3] == 1:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_second_contract = df3.iloc[-1,4]
        pre_second_contract_price = df3.iloc[-1,5]
        pre_index = df3.iloc[-1,7]
        
        roll_state = 2
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
        largest_open_interest_contract = df1.iloc[0,0]
        second_contract = pre_second_contract
        second_contract_price = df1[df1["code"]==second_contract].iloc[0,5]
        index = pre_index*((main_contract_price/pre_main_contract_price)*0.6 + (second_contract_price/pre_second_contract_price)*0.4)
    elif df3.iloc[-1,3] == 2:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_second_contract = df3.iloc[-1,4]
        pre_second_contract_price = df3.iloc[-1,5]
        pre_index = df3.iloc[-1,7]
        
        roll_state = 3
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
        largest_open_interest_contract = df1.iloc[0,0]
        second_contract = pre_second_contract
        second_contract_price = df1[df1["code"]==second_contract].iloc[0,5]
        index = pre_index*((main_contract_price/pre_main_contract_price)*0.4 + (second_contract_price/pre_second_contract_price)*0.6)
    elif df3.iloc[-1,3] == 3:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_second_contract = df3.iloc[-1,4]
        pre_second_contract_price = df3.iloc[-1,5]
        pre_index = df3.iloc[-1,7]
        
        roll_state = 4
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
        largest_open_interest_contract = df1.iloc[0,0]
        second_contract = pre_second_contract
        second_contract_price = df1[df1["code"]==second_contract].iloc[0,5]
        index = pre_index*((main_contract_price/pre_main_contract_price)*0.2 + (second_contract_price/pre_second_contract_price)*0.8)
    elif df3.iloc[-1,3] == 4:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_second_contract = df3.iloc[-1,4]
        pre_second_contract_price = df3.iloc[-1,5]
        pre_index = df3.iloc[-1,7]
        
        roll_state = 0
        main_contract = pre_second_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
        largest_open_interest_contract = df1.iloc[0,0]
        second_contract = ""
        second_contract_price = 0
        index = pre_index*(main_contract_price/pre_second_contract_price)
    
    # 2、输出数据
    list1 = [date1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, index]
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list1)

    # 3、更新变量
    if df2.shape[0] > 30:
        df2 = pd.concat([df2.iloc[1:,:], df1])
        df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)
    else:
        df2 = pd.concat([df2, df1])
        df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)

    return df2, df3



# 正常计算函数：df1当前行情数据，df2历史行情数据，df3历史指数数据，date1日期，path1数据输出路径
def normal_calculate_function(df1, df2, df3, date1, path1):
    if df3.iloc[-1,3] > 0: # 开始展期
        df2, df3 = roll_calculate_function(df1, df2, df3, date1, path1)
    else:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_index = df3.iloc[-1,7]
        
        roll_state = 0
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
        largest_open_interest_contract = df1.iloc[0,0]
        second_contract = ""
        second_contract_price = 0
        index = pre_index*(main_contract_price/pre_main_contract_price)

        list1 = [date1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, index]
        with open(path1, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list1)

        if df2.shape[0] > 30:
            df2 = pd.concat([df2.iloc[1:,:], df1])
            df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)
        else:
            df2 = pd.concat([df2, df1])
            df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)

    return df2, df3



def main(code1, path1):
    df = get_market_data2(code1, "15:00:00")

    df2 = pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "volume", "turnover", "open_interest"])
    df3 = pd.read_csv(path1)
    for date1 in sorted(set(df.date)):
        df1 = df[df.date == date1].sort_values(by="open_interest", ascending=False) # 对date1的合约进行持仓量排序
        if df3.shape[0] < 1:
            main_contract = df1.iloc[0,0]
            main_contract_price = df1.iloc[0,5]
            roll_state = 0
            largest_open_interest_contract = df1.iloc[0,0]
            second_contract = ""
            second_contract_price = 0
            index = 1000

            list1 = [date1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, index]
            with open(path1, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(list1)
            
            if df2.shape[0] > 30:
                df2 = pd.concat([df2.iloc[1:,:], df1])
                df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)
            else:
                df2 = pd.concat([df2, df1])
                df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)

        elif df3.shape[0] < 3:
            pre_main_contract = df3.iloc[-1,1]
            pre_main_contract_price = df3.iloc[-1,2]
            pre_second_contract = df3.iloc[-1,4]
            pre_second_contract_price = df3.iloc[-1,5]
            pre_index = df3.iloc[-1,7]

            roll_state = 0
            main_contract = pre_main_contract
            main_contract_price = df1[df1["code"]==main_contract].iloc[0,5]
            largest_open_interest_contract = df1.iloc[0,0]
            second_contract = ""
            second_contract_price = 0
            index = pre_index*(main_contract_price/pre_main_contract_price)

            list1 = [date1, main_contract, main_contract_price, roll_state, second_contract, second_contract_price, largest_open_interest_contract, index]
            with open(path1, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(list1)
            
            if df2.shape[0] > 30:
                df2 = pd.concat([df2.iloc[1:,:], df1])
                df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)
            else:
                df2 = pd.concat([df2, df1])
                df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]), ignore_index=True)

        else:
            if (df3.iloc[-1,3]==0) & (ci.cal_date_spread(df3.iloc[-1,1], date1)[1]==1): # 需要强制展期时
                df2, df3 = roll_calculate_function(df1, df2, df3, date1, path1, kind=1)
            else:
                if (df3.iloc[-1,3]==0) & (len(set(df3.tail(3)["largest_open_interest_contract"]))==1) & (df3.iloc[-1,1] != df3.iloc[-1,6]) & (ci.compare_codes_maturity(df3.iloc[-1,1],df3.iloc[-1,6],date1)==0): # 当前展期状态为0，连续三天最大持仓合约是同一合约且不是主力合约，主力合约比持仓量最大的合约到期日近
                    df2, df3 = roll_calculate_function(df1, df2, df3, date1, path1)
                else:
                    df2, df3 = normal_calculate_function(df1, df2, df3, date1, path1)



if __name__ == "__main__":
    code1 = "T" # 期货品种
    path1 = "D:\\LearningAndWorking\\VSCode\\python\\project1\\" + code1 + ".csv" # 数据输出路径
    list1 = ["date", "main_contract", "main_contract_price", "roll_state", "second_contract", "second_contract_price", "largest_open_interest_contract", "index"]
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list1)
    
    main(code1, path1)


