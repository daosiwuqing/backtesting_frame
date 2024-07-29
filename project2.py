# 该项目的作用：测试实际投资中跟踪南华单品种指数时的偏离度、保证金占用情况（当日判定是否展期，并在收盘时展期）
import threading as th
import pandas as pd
import zipfile as zp
import io, csv

from package1 import Cal_index1 as ci



# 展期计算函数：df1当前行情数据，df2历史行情数据，df3历史指数数据，date1日期，path1数据输出路径，margin_ratio保证金比例，fee交易费用，scale合约规模，kind展期类型（0为正常展期，1强制展期）
def roll_calculate_function(df1, df2, df3, date1, path1, margin_ratio, fee, scale, kind=0):
    # 1、进行展期
    if df3.iloc[-1,4] == 0:
        if kind == 0:
            pre_main_contract = df3.iloc[-1,1]
            pre_main_contract_price = df3.iloc[-1,2]
            pre_largest_open_interest_contract = df3.iloc[-1,8]
            pre_second_contract = df3.iloc[-1,5]
            pre_second_contract_price = df2[df2["code"]==pre_largest_open_interest_contract].iloc[-1,6]
            pre_net_value = df3.iloc[-1,11]
            pre_open_interest1 = round((pre_net_value*0.8)/(pre_main_contract_price*scale))
            pre_open_interest2 = round((pre_net_value*0.2)/(pre_second_contract_price*scale))

            roll_state = 1
            main_contract = pre_main_contract
            main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
            largest_open_interest_contract = df1.iloc[0,2]

            # 由于金融期货的特殊性，特写该段代码，也适用于商品期货的强制展期
            alternative_second_contract1 = df1.iloc[1,2]
            alternative_second_contract2 = df1.iloc[2,2]
            if ci.compare_codes_maturity(main_contract,pre_largest_open_interest_contract,date1) == 0:
                second_contract = pre_largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,largest_open_interest_contract,date1) == 0:
                second_contract = largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,alternative_second_contract1,date1) == 0:
                second_contract = alternative_second_contract1
            else:
                second_contract = alternative_second_contract2

            second_contract_price = df1[df1["code"]==second_contract].iloc[0,6]
            net_value = pre_net_value + (pre_open_interest1*(main_contract_price-pre_main_contract_price) + pre_open_interest2*(second_contract_price-pre_second_contract_price))*scale
            open_interest1 = round((net_value*0.6)/(main_contract_price*scale))
            open_interest2 = round((net_value*0.4)/(second_contract_price*scale))
            margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
            trading_fee = abs(open_interest1-pre_open_interest1)*main_contract_price*scale*fee + abs(open_interest2-pre_open_interest2)*second_contract_price*scale*fee
            net_value = net_value - trading_fee
        else:
            pre_main_contract = df3.iloc[-1,1]
            pre_main_contract_price = df3.iloc[-1,2]
            pre_largest_open_interest_contract = df3.iloc[-1,8]
            pre_second_contract = df3.iloc[-1,5]
            pre_net_value = df3.iloc[-1,11]
            pre_open_interest1 = round((pre_net_value*0.8)/(pre_main_contract_price*scale))

            roll_state = 1
            main_contract = pre_main_contract
            main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
            largest_open_interest_contract = df1.iloc[0,2]

            # 由于金融期货的特殊性，特写该段代码，也适用于商品期货的强制展期
            alternative_second_contract1 = df1.iloc[1,2]
            alternative_second_contract2 = df1.iloc[2,2]
            if ci.compare_codes_maturity(main_contract,pre_largest_open_interest_contract,date1) == 0:
                second_contract = pre_largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,largest_open_interest_contract,date1) == 0:
                second_contract = largest_open_interest_contract
            elif ci.compare_codes_maturity(main_contract,alternative_second_contract1,date1) == 0:
                second_contract = alternative_second_contract1
            else:
                second_contract = alternative_second_contract2

            pre_second_contract_price = df2[df2["code"]==second_contract].iloc[-1,6]
            pre_open_interest2 = round((pre_net_value*0.2)/(pre_second_contract_price*scale))
            second_contract_price = df1[df1["code"]==second_contract].iloc[0,6]
            net_value = pre_net_value + (pre_open_interest1*(main_contract_price-pre_main_contract_price) + pre_open_interest2*(second_contract_price-pre_second_contract_price))*scale
            open_interest1 = round((net_value*0.6)/(main_contract_price*scale))
            open_interest2 = round((net_value*0.4)/(second_contract_price*scale))
            margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
            trading_fee = abs(open_interest1-pre_open_interest1)*main_contract_price*scale*fee + abs(open_interest2-pre_open_interest2)*second_contract_price*scale*fee
            net_value = net_value - trading_fee
    elif df3.iloc[-1,4] == 1:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_open_interest1 = df3.iloc[-1,3]
        pre_second_contract = df3.iloc[-1,5]
        pre_second_contract_price = df3.iloc[-1,6]
        pre_open_interest2 = df3.iloc[-1,7]
        pre_net_value = df3.iloc[-1,11]
        
        roll_state = 2
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
        largest_open_interest_contract = df1.iloc[0,2]
        second_contract = pre_second_contract
        second_contract_price = df1[df1["code"]==second_contract].iloc[0,6]
        net_value = pre_net_value + (pre_open_interest1*(main_contract_price-pre_main_contract_price) + pre_open_interest2*(second_contract_price-pre_second_contract_price))*scale
        open_interest1 = round((net_value*0.4)/(main_contract_price*scale))
        open_interest2 = round((net_value*0.6)/(second_contract_price*scale))
        margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
        trading_fee = abs(open_interest1-pre_open_interest1)*main_contract_price*scale*fee + abs(open_interest2-pre_open_interest2)*second_contract_price*scale*fee
        net_value = net_value - trading_fee
    elif df3.iloc[-1,4] == 2:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_open_interest1 = df3.iloc[-1,3]
        pre_second_contract = df3.iloc[-1,5]
        pre_second_contract_price = df3.iloc[-1,6]
        pre_open_interest2 = df3.iloc[-1,7]
        pre_net_value = df3.iloc[-1,11]
        
        roll_state = 3
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
        largest_open_interest_contract = df1.iloc[0,2]
        second_contract = pre_second_contract
        second_contract_price = df1[df1["code"]==second_contract].iloc[0,6]
        net_value = pre_net_value + (pre_open_interest1*(main_contract_price-pre_main_contract_price) + pre_open_interest2*(second_contract_price-pre_second_contract_price))*scale
        open_interest1 = round((net_value*0.2)/(main_contract_price*scale))
        open_interest2 = round((net_value*0.8)/(second_contract_price*scale))
        margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
        trading_fee = abs(open_interest1-pre_open_interest1)*main_contract_price*scale*fee + abs(open_interest2-pre_open_interest2)*second_contract_price*scale*fee
        net_value = net_value - trading_fee
    elif df3.iloc[-1,4] == 3:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_open_interest1 = df3.iloc[-1,3]
        pre_second_contract = df3.iloc[-1,5]
        pre_second_contract_price = df3.iloc[-1,6]
        pre_open_interest2 = df3.iloc[-1,7]
        pre_net_value = df3.iloc[-1,11]
        
        roll_state = 4
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
        largest_open_interest_contract = df1.iloc[0,2]
        second_contract = pre_second_contract
        second_contract_price = df1[df1["code"]==second_contract].iloc[0,6]
        net_value = pre_net_value + (pre_open_interest1*(main_contract_price-pre_main_contract_price) + pre_open_interest2*(second_contract_price-pre_second_contract_price))*scale
        open_interest1 = round((net_value)/(second_contract_price*scale))
        open_interest2 = 0
        margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
        trading_fee = abs(open_interest1-pre_open_interest2)*second_contract_price*scale*fee + abs(open_interest2-pre_open_interest1)*main_contract_price*scale*fee
        net_value = net_value - trading_fee
    elif df3.iloc[-1,4] == 4:
        pre_open_interest1 = df3.iloc[-1,3]
        pre_second_contract = df3.iloc[-1,5]
        pre_second_contract_price = df3.iloc[-1,6]
        pre_net_value = df3.iloc[-1,11]
        
        roll_state = 0
        main_contract = pre_second_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
        largest_open_interest_contract = df1.iloc[0,2]
        second_contract = ""
        second_contract_price = 0
        net_value = pre_net_value + pre_open_interest1*(main_contract_price-pre_second_contract_price)*scale
        open_interest1 = round((net_value)/(main_contract_price*scale))
        open_interest2 = 0
        margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
        trading_fee = abs(open_interest1-pre_open_interest1)*main_contract_price*scale*fee
        net_value = net_value - trading_fee

    # 2、输出数据
    list1 = [date1, main_contract, main_contract_price, open_interest1, roll_state, second_contract, second_contract_price, open_interest2, largest_open_interest_contract, margin, trading_fee, net_value]
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list1)

    # 3、更新变量
    if df2.shape[0] > 30:
        df2 = pd.concat([df2.iloc[1:,:], df1])
        df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)
    else:
        df2 = pd.concat([df2, df1])
        df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)

    return df2, df3



# 正常计算函数：df1当前行情数据，df2历史行情数据，df3历史指数数据，date1日期，path1数据输出路径，margin_ratio保证金比例，fee交易费用，scale合约规模
def normal_calculate_function(df1, df2, df3, date1, path1, margin_ratio, fee, scale):
    if df3.iloc[-1,4] > 0: # 开始展期
        df2, df3 = roll_calculate_function(df1, df2, df3, date1, path1, margin_ratio, fee, scale)
    else:
        pre_main_contract = df3.iloc[-1,1]
        pre_main_contract_price = df3.iloc[-1,2]
        pre_open_interest1 = df3.iloc[-1,3]
        pre_net_value = df3.iloc[-1,11]
        
        roll_state = 0
        main_contract = pre_main_contract
        main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
        largest_open_interest_contract = df1.iloc[0,2]
        second_contract = ""
        second_contract_price = 0
        net_value = pre_net_value + pre_open_interest1*(main_contract_price-pre_main_contract_price)*scale
        open_interest1 = round((net_value)/(main_contract_price*scale))
        open_interest2 = 0
        margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
        trading_fee = abs(open_interest1-pre_open_interest1)*main_contract_price*scale*fee
        net_value = net_value - trading_fee

        list1 = [date1, main_contract, main_contract_price, open_interest1, roll_state, second_contract, second_contract_price, open_interest2, largest_open_interest_contract, margin, trading_fee, net_value]
        with open(path1, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list1)

        if df2.shape[0] > 30:
            df2 = pd.concat([df2.iloc[1:,:], df1])
            df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)
        else:
            df2 = pd.concat([df2, df1])
            df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)

    return df2, df3



def main(code1, path1, time1, account, margin_ratio, fee, scale):
    filename1 = code1 + ".csv"
    df = pd.DataFrame()
    with zp.ZipFile("D:\\LearningAndWorking\\VS\\data\\全部期货合约一分钟级数据（2005-2023）.zip") as zf1:
        content1 = zf1.read(filename1)
        text1 = content1.decode(encoding="utf-8")
        df = pd.read_csv(io.StringIO(text1))
        df = df[df.time==time1]

    df2 = pd.DataFrame(columns=["date", "time", "code", "open", "high", "low", "close", "volume", "turnover", "open_interest"])
    df3 = pd.read_csv(path1)
    for date1 in sorted(set(df.date)):
        df1 = df[df.date == date1].sort_values(by="open_interest", ascending=False) # 对date1的合约进行持仓量排序
        if df3.shape[0] < 1:
            roll_state = 0
            main_contract = df1.iloc[0,2]
            main_contract_price = df1.iloc[0,6]
            largest_open_interest_contract = df1.iloc[0,2]
            second_contract = ""
            second_contract_price = 0
            open_interest2 = 0
            net_value = account
            open_interest1 = round((net_value)/(main_contract_price*scale))
            open_interest2 = 0
            margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
            trading_fee = open_interest1*main_contract_price*scale*fee
            net_value = net_value - trading_fee

            list1 = [date1, main_contract, main_contract_price, open_interest1, roll_state, second_contract, second_contract_price, open_interest2, largest_open_interest_contract, margin, trading_fee, net_value]
            with open(path1, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(list1)
            
            if df2.shape[0] > 30:
                df2 = pd.concat([df2.iloc[1:,:], df1])
                df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)
            else:
                df2 = pd.concat([df2, df1])
                df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)

        elif df3.shape[0] < 3:
            pre_main_contract = df3.iloc[-1,1]
            pre_main_contract_price = df3.iloc[-1,2]
            pre_open_interest1 = df3.iloc[-1,3]
            pre_largest_open_interest_contract = df3.iloc[-1,8]
            pre_net_value = df3.iloc[-1,11]

            roll_state = 0
            main_contract = pre_main_contract
            main_contract_price = df1[df1["code"]==main_contract].iloc[0,6]
            largest_open_interest_contract = df1.iloc[0,2]
            second_contract = ""
            second_contract_price = 0
            net_value = pre_net_value + pre_open_interest1*(main_contract_price-pre_main_contract_price)*scale
            open_interest1 = round((net_value)/(main_contract_price*scale))
            open_interest2 = 0
            margin = (open_interest1*main_contract_price + open_interest2*second_contract_price)*scale*margin_ratio
            trading_fee = abs(open_interest1-pre_open_interest1)*main_contract_price*scale*fee
            net_value = net_value - trading_fee

            list1 = [date1, main_contract, main_contract_price, open_interest1, roll_state, second_contract, second_contract_price, open_interest2, largest_open_interest_contract, margin, trading_fee, net_value]
            with open(path1, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(list1)
            
            if df2.shape[0] > 30:
                df2 = pd.concat([df2.iloc[1:,:], df1])
                df3 = df3.iloc[1:,:].append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)
            else:
                df2 = pd.concat([df2, df1])
                df3 = df3.append(pd.DataFrame([list1], columns=["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]), ignore_index=True)

        else:
            if (df3.iloc[-1,4]==0) & (ci.cal_date_spread(df3.iloc[-1,1], date1)[1]==1): # 需要强制展期时
                df2, df3 = roll_calculate_function(df1, df2, df3, date1, path1, margin_ratio, fee, scale, kind=1)
            else:
                if (df3.iloc[-1,4]==0) & (len(set(df3.tail(3)["largest_open_interest_contract"]))==1) & (df3.iloc[-1,1] != df3.iloc[-1,8]) & (ci.compare_codes_maturity(df3.iloc[-1,1],df3.iloc[-1,8],date1)==0): # 当前展期状态为0，连续三天最大持仓合约是同一合约且不是主力合约，主力合约比持仓量最大的合约到期日近
                    df2, df3 = roll_calculate_function(df1, df2, df3, date1, path1, margin_ratio, fee, scale)
                else:
                    df2, df3 = normal_calculate_function(df1, df2, df3, date1, path1, margin_ratio, fee, scale)



if __name__ == "__main__":
    code1 = "CU" # 期货品种
    path1 = "C:\\Users\\29433\\Desktop\\" + code1 + ".csv" # 数据输出路径
    time1 = "14:45:00" # 某个时间点的收盘价
    account = 10000000
    margin_ratio = 0.07
    fee = 0.0001
    scale = 10
    list1 = ["date", "main_contract", "main_contract_price", "open_interest1", "roll_state", "second_contract", "second_contract_price", "open_interest2", "largest_open_interest_contract", "margin", "trading_fee", "net_value"]
    with open(path1, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list1)
    
    main(code1, path1, time1, account, margin_ratio, fee, scale)


