# 该脚本的作用：封装常用的函数




# 该类的作用：封装在指数回测计算中常用的函数
import pandas as pd
import re
class Cal_index1(object):
    editor = "zhaobo"
    

    def __init__(self):
        self.log1 = "2024/01/23对包进行了修改"
    

    def cal_date_spread(code1, date1): # 计算date1离合约到期日差几个月（不考虑天数），code1是合约代码，date1是格式为"2012-06-30"的日期字符串
        # 确定合约到期日的年份
        if re.findall(r"(\d{3})$", code1)[0][0] == date1[3:4]:
            year1 = int(date1[0:4])
        elif (re.findall(r"(\d{3})$", code1)[0][0] == "0") & (date1[3:4] == "9"):
            year1 = int(date1[0:4]) + 1
        else:
            year1 = int(date1[0:3] + re.findall(r"(\d{3})$", code1)[0][0])
        # 确定合约到期日的月份
        month1 = int(re.findall(r"(\d{2})$", code1)[0])
        # 确定合约到期日的日期
        day1 = 15 # 此处为简化处理，假定合约都是在15日交割

        year2 = int(date1[0:4]) # 确定date1的年份
        month2 = int(date1[5:7]) # 确定date1的月份
        day2 = int(date1[8:10]) # 确定date1的日期

        years_spread = year1 - year2
        months_spread = year1*12 + month1 - (year2*12 + month2)
        days_spread = year1*365 + month1*30 + day1 - (year2*365 + month2*30 + day2)
        
        return (years_spread, months_spread, days_spread)
    
    
    def compare_codes_maturity(code1, code2, date1): # 比较两个合约哪个到期日更远，如果code1比code2远返回1，相等返回2，近返回0，date1是进行比较的时点
        a = Cal_index1.cal_date_spread(code1, date1)
        b = Cal_index1.cal_date_spread(code2, date1)
        if (a[2]) > (b[2]):
            return (1)
        elif (a[2]) == (b[2]):
            return (2)
        else:
            return (0)
    
    
    def add_contract_infor(data): # 为入选品种添加合约规模、保证金比例、手续费等合约基本数据，data是dataframe类型的数据
        scale = pd.Series({"A":10, "AG":15, "AL":5, "AP":10, "AU":1000, "B":10, "BB":500, "BC":5, "BU":10, "C":10, 
                        "CF":5, "CJ":5, "CS":10, "CU":5, "CY":5, "EB":5, "EG":10, "FB":10, "FG":20, "FU":10, "HC":10, 
                        "I":100, "J":100, "JD":5, "JM":60, "JR":20, "L":5, "LH":16, "LR":20, "LU":10, "M":10, "MA":10, 
                        "NI":1, "NR":10, "OI":10, "P":10, "PB":5, "PF":5, "PG":20, "PK":5, "PM":50, "PP":5, "RB":10, 
                        "RI":20, "RM":10, "RR":10, "RS":10, "RU":10, "SA":20, "SC":1000, "SF":5, "SI":5, "SM":5, "SN":1, 
                        "SP":10, "SR":10, "SS":5, "TA":5, "UR":10, "V":5, "WH":20, "WR":10, "Y":10, "ZC":100, "ZN":5, 
                        "AO":20, "BR":5, "LC":1, "IF":300, "IC":200, "IM":200, "IH":300, "TS":20000, "TF":10000, "T":10000, "TL":10000})
        margin = pd.Series({"A":0.08, "AG":0.09, "AL":0.08, "AP":0.1, "AU":0.08, "B":0.08, "BB":0.4, "BC":0.08, "BU":0.1, "C":0.08, 
                        "CF":0.07, "CJ":0.12, "CS":0.06, "CU":0.08, "CY":0.07, "EB":0.08, "EG":0.08, "FB":0.1, "FG":0.09, "FU":0.1, "HC":0.07, 
                        "I":0.13, "J":0.2, "JD":0.08, "JM":0.2, "JR":0.15, "L":0.07, "LH":0.12, "LR":0.15, "LU":0.1, "M":0.07, "MA":0.08, 
                        "NI":0.12, "NR":0.08, "OI":0.09, "P":0.08, "PB":0.08, "PF":0.08, "PG":0.08, "PK":0.08, "PM":0.15, "PP":0.07, "RB":0.07, 
                        "RI":0.15, "RM":0.09, "RR":0.06, "RS":0.2, "RU":0.08, "SA":0.09, "SC":0.1, "SF":0.12, "SI":0.09, "SM":0.12, "SN":0.12, 
                        "SP":0.08, "SR":0.07, "SS":0.07, "TA":0.07, "UR":0.08, "V":0.07, "WH":0.15, "WR":0.09, "Y":0.07, "ZC":0.05, "ZN":0.08, 
                        "AO":0.09, "BR":0.12, "LC":0.09, "IF":0.08, "IC":0.08, "IM":0.08, "IH":0.08, "TS":0.005, "TF":0.01, "T":0.02, "TL":0.035})
        
        result = pd.concat([data, scale, margin], axis=1, join='inner') #以data的行索引(axis=1)进行inner类型的拼接
        
        return(result)


    def get_weight(index, year_month): # 获取某个指数某个日期的权重配比，index如"综合指数"，year_month如"2020-06"
        weights = pd.read_excel(r"D:\LearningAndWorking\VS\data\南华指数系列历史权重(2023).xlsx", sheet_name=index).iloc[:-1,:]
        regex = year_month + "|代码"
        weight1 = weights.filter(regex=regex, axis=1).set_index("代码")
        weight2 = weight1[~weight1.isin([0])].dropna().iloc[:,0]
        
        return(weight2)
    

    def transform_keyword_to_code(string): # 根据所给的字符串返回相应的品种代码
        dict1 = {"A":["豆一", "黄大豆1号"], "AG":["银"], "AL":["铝"], "AP":["苹果"], "AU":["金"], "B":["豆二", "黄大豆2号"], "BB":["胶合板"], "BC":["国际铜"], 
                "BU":["沥青"], "C":["玉米"], "CF":["棉花", "一号棉"], "CJ":["红枣"], "CS":["淀粉"], "CU":["铜"], "CY":["棉纱"], "EB":["苯乙烯"], "EG":["乙二醇"], 
                "FB":["纤维板"], "FG":["玻璃"], "FU":["燃"], "HC":["卷"], "I":["铁矿"], "J":["焦炭"], "JD":["鸡蛋"], "JM":["焦煤"], "JR":["粳稻"], "L":["聚乙烯"], 
                "LH":["生猪"], "LR":["晚籼稻"], "LU":["低硫"], "M":["豆粕"], "MA":["甲醇"], "NI":["镍"], "NR":["20号胶"], "OI":["菜籽油", "菜油"], "P":["棕榈油"], 
                "PB":["铅"], "PF":["短纤"], "PG":["液化"], "PK":["花生"], "PM":["普麦"], "PP":["聚丙烯"], "RB":["螺纹"], "RI":["早籼稻"], "RM":["菜籽粕", "菜粕"], 
                "RR":["粳米"], "RS":["油菜籽", "菜籽"], "RU":["橡胶"], "SA":["纯碱"], "SC":["原油"], "SF":["硅铁"], "SI":["工业硅"], "SM":["锰硅"], "SN":["锡"], 
                "SP":["纸浆"], "SR":["白糖"], "SS":["不锈钢"], "TA":["PTA", "精对苯二甲酸"], "UR":["尿素"], "V":["聚氯乙烯", "PVC"], "WH":["强麦"], "WR":["线材"], 
                "Y":["豆油"], "ZC":["动力煤"], "ZN":["锌"], "BR":["丁二烯"], "AO":["氧化"], "LC":["碳酸锂"]} #对应品种的关键字
        dict2 = {"BC":["国际"], "CS":["淀粉"], "LU":["低硫"], "NR":["20号"], "OI":["菜籽油", "菜油"], "RM":["粕"]} #有相同关键字品种中具有特殊关键字的品种
        dict3 = {"CU":["铜"], "C":["玉米"], "FU":["燃"], "RU":["橡胶"], "RS":["油菜籽", "菜籽"]} #有相同关键字品种中剔除dict2品种后的品种

        # 定义一个函数：检查字符串是否包含字典中的值，并返回所有匹配的值，根据返回匹配的值返回值所对应的键列表
        def function1(dict1, string):
            list1_keys = []
            for key, value in dict1.items():
                if any(x in string for x in value):
                    list1_keys.append(key)
                else:
                    continue
            return(list1_keys)
        
        list1_keys = function1(dict1, string)
        if len(list1_keys) > 1: #如果存在多个键的值符合上述条件，就进行新的判断
            list2_keys = function1(dict2, string)
            if len(list2_keys) == 0:
                code_list = function1(dict3, string)
                return(code_list[0])
            elif len(list2_keys) == 1:
                return(list2_keys[0])
        elif len(list1_keys) == 1: #如果只有一个键的值符合上述条件，就返回列表中的元素
            return(list1_keys[0])




# 该类的作用：封装在期权计算中常用的函数
import pandas as pd
import numpy as np
import numba
import matplotlib.pyplot as plt
import scipy as sp
class Pri_option(object):
    editor = "zhaobo"
    


    def __init__(self):
        self.log1 = "2024/01/23对包进行了修改"



    # 1、布朗运动和伊藤引理
    # 1.1、标准布朗运动
    def standard_brownian(steps, paths, T, S0):
        dt = T / steps # 求出dt
        S_path = np.zeros((steps+1, paths)) # 创建一个矩阵，用来准备储存模拟情况
        S_path[0] = S0 # 起点设置
        rn = np.random.standard_normal(S_path.shape) # 一次性创建出需要的正态分布随机数，当然也可以写在循环里每次创建一个时刻的随机数
        for step in range(1, steps+1):
            S_path[step] = S_path[step-1] + rn[step-1]*np.sqrt(dt)
        plt.plot(S_path)
        plt.show()

        return S_path

    # S_path = standard_brownian(steps=100, paths=10, T=1, S0=0)


    # 1.2、广义的布朗运动
    def brownian(steps, paths, T, S0, a, b):
        dt = T / steps # 求出dt
        S_path = np.zeros((steps+1, paths)) # 创建一个矩阵，用来准备储存模拟情况
        S_path[0] = S0 # 起点设置
        rn = np.random.standard_normal(S_path.shape) # 一次性创建出需要的正态分布随机数，当然也可以写在循环里每次创建一个时刻的随机数
        for step in range(1, steps+1):
            S_path[step] = S_path[step-1] + a*dt + b*rn[step-1]*np.sqrt(dt) # 和标准布朗运动的区别就在这一行
        plt.plot(S_path)
        plt.show()

        return S_path

    # S_path = brownian(steps=100, paths=10, T=1, S0=0, a=5, b=2)


    # 1.3、几何布朗运动
    def geo_brownian(steps, paths, T, S0, u, sigma):
        dt = T / steps # 求出dt
        S_path = np.zeros((steps+1, paths)) # 创建一个矩阵，用来准备储存模拟情况
        S_path[0] = S0 # 起点设置
        rn = np.random.standard_normal(S_path.shape) # 一次性创建出需要的正态分布随机数，当然也可以写在循环里每次创建一个时刻的随机数
        for step in range(1, steps+1):
            S_path[step] = S_path[step-1] * np.exp((u-0.5*sigma**2)*dt + sigma*np.sqrt(dt)*rn[step]) # 和其他布朗运动的区别就在这一行
        plt.plot(S_path)
        plt.show()

        return S_path

    # S_path = geo_brownian(steps=100, paths=50, T=1, S0=100, u=0.03, sigma=0.2)



    # 2、BSM公式
    # 2.1、定价公式
    def BSM(CP, S, X, sigma, T, r, b):
        """
        Parameters
        ----------
        CP : 看涨或看跌"C"or"P".
        S : 标的价格.
        X : 行权价格.
        sigma : 波动率.
        T : 年化到期时间.
        r : 收益率.
        b : 持有成本，当b=r时，为标准的无股利模型，b=0时，为期货期权，b为r-q时，为支付股利模型，b为r-rf时为外汇期权.
        Returns
        ----------
        返回欧式期权的估值
        """
        d1 = (np.log(S/X) + (b+sigma**2/2)*T) / (sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        if CP == "C":
            value = S*np.exp((b-r)*T)*sp.stats.norm.cdf(d1) - X*np.exp(-r*T)*sp.stats.norm.cdf(d2)
        else:
            value = X*np.exp(-r*T)*sp.stats.norm.cdf(-d2) - S*np.exp((b-r)*T)*sp.stats.norm.cdf(-d1)
        
        return value

    # BSM(CP="C", S=100, X=95, sigma=0.25, T=1, r=0.03, b=0.03)


    # 2.2、二分法求解隐含波动率
    def binary(V0, CP, S, X, T, r, b, vol_est=0.2):
        """
        Parameters
        ----------
        V0 : 期权价值.
        CP : 看涨或看跌"C"or"P".
        S : 标的价格.
        X : 行权价格.
        T : 年化到期时间.
        r : 收益率.
        b : 持有成本，当b=r时，为标准的无股利模型，b=0时，为期货期权，b为r-q时，为支付股利模型，b为r-rf时为外汇期权.
        vol_est : 预计的初始波动率.
        Returns
        ----------
        返回看涨期权的隐含波动率。
        """
        start = 0  # 初始波动率下限
        end = 2 # 初始波动率上限
        e = 1 # 先给定一个值，让循环运转起来
        while abs(e) >= 0.0001: # 迭代差异的精度，根据需要调整
            try:
                val = Pri_option.BSM(CP, S, X, vol_est, T, r, b)  
            except ZeroDivisionError:
                print("期权的内在价值大于期权的价格，无法收敛出波动率，会触发除0错误！")
                break
            if val - V0 > 0: # 若计算的期权价值大于实际价值，说明使用的波动率偏大
                end = vol_est
                vol_est = (start + end) / 2    
                e = end - vol_est
            else: # 若计算的期权价值小于实际价值，说明使用的波动率偏小
                start = vol_est
                vol_est = (start + end) / 2
                e = start - vol_est
        
        return round(vol_est, 4)

    # value = BSM(CP="C", S=100, X=95, T=1, sigma=0.25, r=0.03, b=0.03) #实验一个期权的价值
    # print(value)
    # vol = binary(V0=value, CP="C", S=100, X=95, T=1, r=0.03, b=0.03)  #根据刚才实验的期权价值求一下波动率是否正确
    # print(vol)


    # 2.3 牛顿法求解隐含波动率
    def newton(V0, CP, S, X, T, r, b, vol_est=0.25, n_iter=1000):
        # n_iter表示迭代的次数
        for i in range(n_iter):
            d1 = (np.log(S/X) + (b + vol_est**2/2)*T) / (vol_est*np.sqrt(T))
            vega = S * np.exp((b-r)*T) * sp.stats.norm.pdf(d1) * T**0.5 # 计算vega
            vol_est = vol_est - (Pri_option.BSM(CP, S, X, vol_est, T, r, b) - V0) / vega  #每次迭代都重新算一下波动率
        
        return vol_est

    # vol = newton(V0, CP, S, X, T, r, b, vol_est=0.2, n_iter=1000)
    # print(vol)



    # 3、欧式期权希腊字母计算
    # 3.1、解析解下的实现
    def greeks(CP, S, X, sigma, T, r, b): # 计算greeks的函数
        """
        Parameters
        ----------
        CP : 看涨或看跌"C"or"P".
        S : 标的价格.
        X : 行权价格.
        sigma : 波动率.
        T : 年化到期时间.
        r : 收益率.
        b : 持有成本，当b=r时，为标准的无股利模型，b=0时，为期货期权，b为r-q时，为支付股利模型，b为r-rf时为外汇期权.
        Returns
        ----------
        返回欧式期权的估值和希腊字母
        """
        d1 = (np.log(S/X) + (b+sigma**2/2)*T) / (sigma*np.sqrt(T)) # 求d1
        d2 = d1 - sigma*np.sqrt(T) # 求d2

        if CP == "C":
            option_value = S*np.exp((b-r)*T)*sp.stats.norm.cdf(d1) - X*np.exp(-r*T)*sp.stats.norm.cdf(d2) # 计算期权价值
            delta = np.exp((b-r)*T) * sp.stats.norm.cdf(d1)
            gamma = np.exp((b-r)*T)*sp.stats.norm.pdf(d1) / (S*sigma*T**0.5) # 注意是pdf，概率密度函数
            vega = S * np.exp((b-r)*T) * sp.stats.norm.pdf(d1) * T**0.5 # 计算vega
            theta = -np.exp((b-r)*T)*S*sp.stats.norm.pdf(d1)*sigma / (2*T**0.5) - r*X*np.exp(-r*T)*sp.stats.norm.cdf(d2) - (b-r)*S*np.exp((b-r)*T)*sp.stats.norm.cdf(d1)
            if b !=0: # rho比较特别，b是否为0会影响求导结果的形式
                rho = X * T * np.exp(-r*T) * sp.stats.norm.cdf(d2)
            else:
                rho = -np.exp(-r*T) * (S*sp.stats.norm.cdf(d1)-X*sp.stats.norm.cdf(d2))

        else:
            option_value = X*np.exp(-r*T)*sp.stats.norm.cdf(-d2) - S*np.exp((b-r)*T)*sp.stats.norm.cdf(-d1)
            delta = -np.exp((b-r)*T) * sp.stats.norm.cdf(-d1)
            gamma = np.exp((b-r)*T)*sp.stats.norm.pdf(d1) / (S*sigma*T**0.5) # 跟看涨其实一样，不过还是先写在这里
            vega = S * np.exp((b-r)*T) * sp.stats.norm.pdf(d1) * T**0.5 # #跟看涨其实一样，不过还是先写在这里
            theta = -np.exp((b-r)*T)*S*sp.stats.norm.pdf(d1)*sigma / (2*T**0.5) + r*X*np.exp(-r*T)*sp.stats.norm.cdf(-d2) + (b-r)*S*np.exp((b-r)*T)*sp.stats.norm.cdf(-d1)
            if b !=0: # rho比较特别，b是否为0会影响求导结果的形式
                rho = -X * T * np.exp(-r*T) * sp.stats.norm.cdf(-d2)
            else:
                rho = -np.exp(-r*T) * (X*sp.stats.norm.cdf(-d2) - S*sp.stats.norm.cdf(-d1)) 
        # 写成函数时要有个返回，这里直接把整个写成字典一次性输出。
        greeks = {"option_value":option_value, "delta":delta, "gamma":gamma, "vega":vega, "theta":theta, "rho":rho}
        
        return greeks

    # S = np.linspace(0.1, 200, 100) # 生产0.01到200的100个价格序列
    # result = greeks(CP, S, X, sigma, T, r, b)
    # fig,ax = plt.subplots(nrows=3, ncols=2, figsize=(8,12)) # 使用多子图的方式输入结果，所以写的复杂一点
    # greek_list = [['option_value','delta'], ['gamma','vega'], ['theta','rho']] # 和子图的二维数组对应一下
    # for m in range(3):
    #     for n in range(2):
    #         plot_item = greek_list[m][n]
    #         ax[m,n].plot(S,result[plot_item])
    #         ax[m,n].legend([plot_item])
    # plt.show()


    # 3.2、差分方式的实现
    def greeks_diff(CP, S, X, sigma, T, r, b, pct_change): # 计算greeks的函数,差分方式,pct_change表示价格变化的幅度
        option_value = Pri_option.BSM(CP, S, X, sigma, T, r, b)
        delta = (Pri_option.BSM(CP, S+S*pct_change, X, sigma, T, r, b) - Pri_option.BSM(CP, S-S*pct_change, X, sigma, T, r, b)) / (2*S*pct_change)
        gamma = (Pri_option.BSM(CP, S+S*pct_change, X, sigma, T, r, b) + Pri_option.BSM(CP, S-S*pct_change, X, sigma, T, r, b) - 2*Pri_option.BSM(CP, S, X, sigma, T, r, b)) / ((S*pct_change)**2)
        vega = (Pri_option.BSM(CP, S, X, sigma+sigma*pct_change, T, r, b) - Pri_option.BSM(CP, S, X, sigma-sigma*pct_change, T, r, b)) / (2*sigma*pct_change)
        #theta因为表示的是时间流逝，所+—号是反过来的
        theta = (Pri_option.BSM(CP, S, X, sigma, T-T*pct_change, r, b) - Pri_option.BSM(CP, S, X, sigma, T+T*pct_change, r, b)) / (2*T*pct_change) 
        if b != 0:
            rho = (Pri_option.BSM(CP, S, X, sigma, T, r+r*pct_change, b+r*pct_change) - Pri_option.BSM(CP, S, X, sigma, T, r-r*pct_change, b-r*pct_change)) / (2*r*pct_change)
        else:
            rho = (Pri_option.BSM(CP, S, X, sigma, T, r+r*pct_change, b) - Pri_option.BSM(CP, S, X, sigma, T, r-r*pct_change, b)) / (2*r*pct_change)
        greeks = {"option_value":option_value, "delta":delta, "gamma":gamma, "vega":vega, "theta":theta, "rho":rho}
        
        return greeks

    # S = np.linspace(0.1, 200, 100) # 生产0.01到200的100个价格序列
    # result_diff = greeks_diff(CP, S, X, sigma, T, r, b, pct_change)
    # fig,ax = plt.subplots(nrows=3, ncols=2, figsize=(8,12)) # 使用多子图的方式输入结果，所以写的复杂一点
    # greek_list = [['option_value','delta'], ['gamma','vega'], ['theta','rho']] # 和子图的二维数组对应一下
    # for m in range(3):
    #     for n in range(2):
    #         plot_item = greek_list[m][n]
    #         ax[m,n].plot(S, result_diff[plot_item])
    #         ax[m,n].legend([plot_item])
    # plt.show()



    # 4、美式期权定价
    # 4.1、二叉树定价
    def simulate_tree_am(CP, m, S0, T, sigma, K, r, b): # 二叉树模型美式期权
        """
        CP : 看涨或看跌.
        m : 模拟的期数.
        S0 : 期初价格.
        T : 期限.
        sigma : 波动率.
        K : 行权价格.
        r : 无风险利率.
        b : 持有成本,当b=r时，为标准的无股利模型，b=0时，为black76，b为r-q时，为支付股利模型，b为r-rf时为外汇期权.
        """
        dt = T / m
        u = np.exp(sigma * np.sqrt(dt))
        d = 1 / u
        S = np.zeros((m+1, m+1))
        S[0, 0] = S0
        p = (np.exp(b*dt) - d) / (u-d)
        for i in range(1, m+1):  # 模拟每个节点的价格
            for a in range(i):
                S[a, i] = S[a, i-1] * u
                S[a+1, i] = S[a, i-1] * d
        Sv = np.zeros_like(S) # 创建期权价值的矩阵，用到从最后一期倒推期权价值
        if CP == "C":
            S_intrinsic = np.maximum(S-K, 0)
        else:
            S_intrinsic = np.maximum(K-S, 0)
        Sv[:, -1] = S_intrinsic[:, -1]
        for i in range(m-1, -1, -1): # 反向倒推每个节点的价值
            for a in range(i+1):
                Sv[a, i] = max((Sv[a, i+1]*p + Sv[a+1, i+1]*(1-p)) / np.exp(r*dt), S_intrinsic[a, i])
        
        return Sv[0, 0]

    # value = simulate_tree_am(CP="C", m=1000, S0=100, K=95, sigma=0.25, T=1, r=0.03, b=0.03)


    # 4.2、BAW公式定价
    # 方法一、论文的迭代方式
    def find_Sx(CP, X, sigma, T, r, b): # 手动写的标准的牛顿迭代法
        ITERATION_MAX_ERROR = 0.00001 # 牛顿法迭代的精度
        M = 2*r / sigma**2
        N = 2*b / sigma**2
        K = 1 - np.exp(-r*T)
        q1 = (-(N-1) - np.sqrt((N-1)**2 + 4*M/K)) / 2
        q2 = (-(N-1) + np.sqrt((N-1)**2 + 4*M/K)) / 2
        if CP == "C":
            S_infinite = X / (1 - 2*(-(N-1) + np.sqrt((N-1)**2 + 4*M))**-1)  # 到期时间为无穷时的价格
            h2 = -(b*T + 2*sigma*np.sqrt(T)) * X / (S_infinite-X)
            Si = X + (S_infinite-X) * (1-np.exp(h2))  # 计算种子值
            # print(f"Si的种子值为{Si}")
            LHS = Si - X
            d1 = (np.log(Si/X) + (b + sigma**2/2)*T) / (sigma*np.sqrt(T))
            RHS = Pri_option.BSM("C", Si, X, sigma, T, r, b) + (1 - np.exp((b-r)*T)*sp.stats.norm.cdf(d1)) * Si / q2
            bi = np.exp((b-r)*T) * sp.stats.norm.cdf(d1) * (1 - 1/q2) + (1 - (np.exp((b-r)*T)*sp.stats.norm.pdf(d1)) / sigma / np.sqrt(T)) / q2  # bi为迭代使用的初始斜率
            while np.abs((LHS-RHS) / X) > ITERATION_MAX_ERROR:
                Si = (X + RHS - bi*Si) / (1-bi)
                # print(f"Si的值迭代为{Si}")
                LHS = Si - X
                d1 = (np.log(Si/X) + (b + sigma**2/2)*T) / (sigma*np.sqrt(T))
                RHS = Pri_option.BSM("C", Si, X, sigma, T, r, b) + (1 - np.exp((b-r)*T)*sp.stats.norm.cdf(d1)) * Si / q2
                bi = np.exp((b-r)*T) * sp.stats.norm.cdf(d1) * (1 - 1/q2) + (1 - (np.exp((b-r)*T)*sp.stats.norm.pdf(d1)) / sigma / np.sqrt(T)) / q2
            return Si
        else:
            S_infinite= X / (1 - 2*(-(N-1) - np.sqrt((N-1)**2+4*M))**-1)
            h1 = -(b*T - 2*sigma*np.sqrt(T)) * X / (X-S_infinite)
            Si = S_infinite + (X-S_infinite) * np.exp(h1)  # 计算种子值
            # print(f"Si的种子值为{Si}")
            LHS = X - Si
            d1 = (np.log(Si/X) + (b+sigma**2/2)*T) / (sigma*np.sqrt(T))
            RHS = Pri_option.BSM("P", Si, X, sigma, T, r, b) - (1 - np.exp((b-r)*T)*sp.stats.norm.cdf(-d1)) * Si / q1
            bi = -np.exp((b-r)*T) * sp.stats.norm.cdf(-d1) * (1 - 1/q1) - (1 + (np.exp((b-r)*T)*sp.stats.norm.pdf(-d1)) / sigma / np.sqrt(T)) / q1
            while np.abs((LHS-RHS) / X) > ITERATION_MAX_ERROR:
                Si = (X - RHS  + bi*Si) / (1+bi)
                # print(f"Si的值迭代为{Si}")
                LHS = X - Si
                d1 = (np.log(Si/X) + (b+sigma**2/2)*T) / (sigma*np.sqrt(T))
                RHS = Pri_option.BSM("P", Si, X, sigma, T, r, b) - (1 - np.exp((b-r)*T)*sp.stats.norm.cdf(-d1)) * Si / q1
                bi = -np.exp((b-r)*T) * sp.stats.norm.cdf(-d1) * (1 - 1/q1) - (1 + (np.exp((b-r)*T)*sp.stats.norm.pdf(-d1)) / sigma / np.sqrt(T)) / q1
            return Si

    # 方法二、使用scipy优化的方式
    def find_Sx_func(CP, S, X, sigma, T, r, b): # opt版本的迭代
        M = 2*r / sigma**2
        N = 2*b /sigma**2
        K = 1 - np.exp(-r*T)
        q1 = (-(N-1) - np.sqrt((N-1)**2 + 4*M/K)) / 2
        q2 = (-(N-1) + np.sqrt((N-1)**2 + 4*M/K)) / 2
        if CP == "C":
            LHS = S - X
            RHS = Pri_option.BSM("C", S, X, sigma, T, r, b) + (1 - np.exp((b-r)*T)*sp.stats.norm.cdf((np.log(S/X)+(b + sigma**2/2)*T) / (sigma*np.sqrt(T)))) * S / q2
            y = (RHS-LHS)**2    
        else:
            LHS = X - S
            RHS = Pri_option.BSM("P", S, X, sigma, T, r, b) - (1 - np.exp((b-r)*T)*sp.stats.norm.cdf(-((np.log(S/X)+(b + sigma**2/2)*T) / (sigma*np.sqrt(T))))) * S / q1
            y = (RHS-LHS)**2
        return y
    def find_Sx_opt(CP, S, X, sigma, T, r, b):
        start = S # 随便给一个S的初始值，或者其他值都行
        func = lambda S : Pri_option.find_Sx_func(CP, S, X, sigma, T, r, b)
        Si = sp.optimize.fmin(func, start) # 直接做掉包侠
        return Si

    # BAW定价
    def BAW(CP, S, X, sigma, T, r, b, opt_method="newton"):
        if b > r : # b>r时，美式期权价值和欧式期权相同
            value = Pri_option.BSM(CP, S, X, sigma, T, r, b)

        else:
            M = 2 * r / sigma**2
            N = 2 * b / sigma**2
            K = 1 - np.exp(-r*T)
            if opt_method == "newton": # 若为牛顿法就用第一种迭代法
                Si = Pri_option.find_Sx(CP, X, sigma, T, r, b)
            else: # 若不为牛顿法，其他方法这里就是scipy的优化方法
                Si = Pri_option.find_Sx_opt(CP, S, X, sigma, T, r, b)
            d1 = (np.log(Si/X) + (b + sigma**2/2)*T) / (sigma*np.sqrt(T))
            if CP == "C":   
                q2 = (-(N-1) + np.sqrt((N-1)**2 + 4*M/K)) / 2
                A2 = Si / q2 * (1 - np.exp((b-r)*T)*sp.stats.norm.cdf(d1))
                if S < Si:
                    value = Pri_option.BSM(CP, S, X, sigma, T, r, b) + A2*(S/Si)**q2
                else:
                    value = S - X 

            else:
                q1 = (-(N-1) - np.sqrt((N-1)**2 + 4*M/K)) / 2
                A1 = -Si / q1 * (1 - np.exp((b-r)*T)*sp.stats.norm.cdf(-d1))
                if S > Si:
                    value = Pri_option.BSM(CP, S, X, sigma, T, r, b) + A1*(S/Si)**q1
                else:
                    value = X - S             
        
        return value

    # result1 = BAW(CP="P", S=100, X=99, sigma=0.2, T=1, r=0.03, b=0, opt_method="newton")
    # result2 = BAW(CP="P", S=100, X=99, sigma=0.2, T=1, r=0.03, b=0, opt_method="scipy")

    # 二分法求解美式期权隐含波动率
    def American_binary(V0, CP, S, X, T, r, b, sigma=0.2):
        """
        Parameters
        ----------
        V0 : 期权价值.
        CP : 看涨或看跌"C"or"P".
        S : 标的价格.
        X : 行权价格.
        T : 年化到期时间.
        r : 收益率.
        b : 持有成本，当b=r时，为标准的无股利模型，b=0时，为期货期权，b为r-q时，为支付股利模型，b为r-rf时为外汇期权.
        sigma=0.2 : 预计的初始波动率.
        Returns
        ----------
        返回看涨期权的隐含波动率。
        """
        start = 0  # 初始波动率下限
        end = 2 # 初始波动率上限
        e = 1 # 先给定一个值，让循环运转起来
        while abs(e) >= 0.0001: # 迭代差异的精度，根据需要调整
            try:
                val = Pri_option.BAW(CP, S, X, sigma, T, r, b, opt_method="newton")  
            except ZeroDivisionError:
                print("期权的内在价值大于期权的价格，无法收敛出波动率，会触发除0错误！")
                break
            if val - V0 > 0: # 若计算的期权价值大于实际价值，说明使用的波动率偏大
                end = sigma
                sigma = (start + end) / 2    
                e = end - sigma
            else: # 若计算的期权价值小于实际价值，说明使用的波动率偏小
                start = sigma
                sigma = (start + end) / 2
                e = start - sigma
        
        return round(sigma, 4)


    # 4.3、最小二乘蒙特卡洛模拟定价
    def LSM(steps, paths, CP, S0, X, sigma, T, r, b):
        #代码也可以多写几行计算出所有的提前行权节点，这里为了逻辑清晰就没有列出
        S_path = Pri_option.geo_brownian(steps, paths, T, S0, b, sigma) # 价格生成路径
        dt = T / steps
        cash_flow = np.zeros_like(S_path) # 实现创建好现金流量的矩阵，后续使用
        df = np.exp(-r*dt) # 每一期的折现因子
        if CP == "C":
            cash_flow[-1] = np.maximum(S_path[-1]-X, 0)   #先确定最后一期的价值，就是实值额
            exercise_value = np.maximum(S_path-X, 0)
        else:
            cash_flow[-1] = np.maximum(X-S_path[-1], 0) # 先确定最后一期的价值，就是实值额
            exercise_value = np.maximum(X-S_path, 0)

        for t in range(steps-1, 0, -1): # M-1为倒数第二个时点，从该时点循环至1时点
            df_cash_flow = cash_flow[t+1] * df
            S_price = S_path[t] # 标的股价，回归用的X
            itm_index = (exercise_value[t] > 0) # 确定实值的index，后面回归要用，通过index的方式可以不破坏价格和现金流矩阵的大小
            reg = np.polyfit(S_price[itm_index], df_cash_flow[itm_index], 2) # 实值路径下的标的股价X和下一期的折现现金流Y回归
            holding_value = exercise_value[t].copy() # 创建一个同长度的向量，为了保持index一致，当然也可以用np.zeros_like等方式，本质一样
            holding_value[itm_index] = np.polyval(reg, S_price[itm_index]) # 回归出 holding_value，其他的值虽然等于exercise_value，但是后续判断会去除
            ex_index = itm_index & (exercise_value[t] > holding_value) # 在实值路径上，进一步寻找出提前行权的index

            df_cash_flow[ex_index] = exercise_value[t][ex_index] # 将cash_flow中提前行权的替换为行权价值，其他保持下一期折现不变
            cash_flow[t] = df_cash_flow

        value = cash_flow[1].mean() * df

        return value

    # LSM(steps=1000, paths=50000, CP="P", S0=40, X=40, sigma=0.2, T=1, r=0.06, b=0.06)


    # 4.4、有限差分法定价
    # 定义一个生成系数矩阵的函数
    def gen_diag(M, a, b, c, Sd_idx=0):  # 生成M-1维度的d对角矩阵，a为对角线左下方的值，b为对角的值，c为对角线右上方的值
        """
        Sd_idx代表S划分的下边界Smin/ds的值，一般下边界为0，可设置为0，对于具有下障碍等类型期权需要单独设置Sd_idx
        a,b,c需要函数作为参数，建议使用lambda生成函数
        """
        a_m_1 = [a(i) for i in range(Sd_idx+2, M)]  # a的系数是从2开始的
        b_m_1 = [b(i) for i in range(Sd_idx+1, M)]  # b的系数是从1开始的
        c_m_1 = [c(i) for i in range(Sd_idx+1, M-1)] # c的系数是从1开始,但是M-2结束
        diag_matrix = np.diag(a_m_1, -1) + np.diag(b_m_1, 0) + np.diag(c_m_1, 1)
        
        return diag_matrix

    # 4.4.1、显式有限差分法定价
    def explicit_FD_M(CP, S, K, T, sigma, r, b, M, N):

        ds = S/M # 确定价格步长，分子用S的意义在于可以让S必定落在网格点上，后续不需要使用插值法
        M = int(K/ds) * 4 # 确定覆盖的价格范围，这里设置为4倍的行权价，也可根据需要设置为其他，这里根据价格范围重新计算价格点位数量M
        S_idx= int(S/ds) # S所在的index，用于方便确定初始S对应的期权价值
        dt = T/N # 时间步长
        df = 1 / (1 + r*dt) # 折现因子
        print(f"生产的网格：价格分为M = {M}个点位，时间分为N = {N}个点位")
        V_grid = np.zeros((M+1, N+1)) # 预先生成包括0在内的期权价值矩阵

        S_array = np.linspace(0, M*ds, M+1)  # 价格序列
        T_array = np.linspace(0, N*dt, N+1)  # 时间序列
        T2M_array = T_array[-1] - T_array    # 生成到期时间的数组，方便后面计算边界条件

        if CP == "C":
            V_grid[:, N] = np.maximum(S_array-K, 0) # 确定终值条件，到期时期权价值很好计算
            V_grid[M] = np.exp(-r*T2M_array) * (S_array[-1] * np.exp(b*T2M_array) - K) # 上边界价格够高，期权表现像远期，这里是远期定价，而不是简单得S-X
        else:
            V_grid[:, N] = np.maximum(K-S_array, 0)  # 确定终值条件，到期时期权价值很好计算
            V_grid[0] = np.exp(-r*T2M_array) * K

        aj = lambda i : 0.5 * (sigma**2 * i**2 - b*i) * dt 
        bj = lambda i : 1 - sigma**2 * i**2 * dt
        cj = lambda i : 0.5 * (sigma**2 * i**2 + b*i) * dt
        coef_matrix = Pri_option.gen_diag(M, aj, bj, cj)
        for j in range(N-1, -1, -1): #时间倒推循环
            Z = np.zeros_like(V_grid[1:M, j+1])  #用来存储边界条件
            Z[0] = aj(1) * V_grid[0, j+1]
            Z[-1] = cj(M-1) * V_grid[-1, j+1]
            # 矩阵求解
            V_grid[1:M, j] = df * (coef_matrix @ V_grid[1:M, j+1] + Z)
            # 美式期权提前行权判断
            if CP == "C":
                V_grid[1:M, j] = np.maximum(S_array[1:M]-K, V_grid[1:M, j])  #美式期权提前行权判断
            else:
                V_grid[1:M, j] = np.maximum(K - S_array[1:M], V_grid[1:M, j])
        
        return V_grid[S_idx, 0] # 返回初0时点的初始价格的价值

    # explicit_FD_M(CP="P", S=36, K=40, T=0.5, sigma=0.4, r=0.06, b=0.06, M=125, N=50000)

    # 4.4.2、隐式有限差分法定价
    def implicit_FD(CP, S, K, T, sigma, r, b, M, N):
        """
        隐式有限差分法，比显示更容易收敛，因此N直接指定也容易收敛,但需要通过求逆矩阵解方程组
        f[i+1, j]
        f[i, j]  ➡  f[i, j+1]
        f[i-1, j]
        """
        ds = S/M # 确定价格步长，用S的意义在于可以让S必定落在网格点上，后续不需要使用插值法
        M = int(K/ds) * 2 # 确定覆盖的价格范围，这里设置为2倍的行权价，也可根据需要设置为其他，这里根据价格范围重新计算价格点位数量M
        S_idx= int(S/ds) # S所在的index，用于方便确定初始S对应的期权价值
        dt = T/N # 确定步长dt，隐式方法收敛性相对较好，不像显示那么依赖于dt必须得够小，所以这里直接指定N
        T_array = np.linspace(0, N*dt, N+1) # 时间序列
        T2M_array = T_array[-1] - T_array
        print(f"生产的网格：价格分为M = {M}个点位，时间分为N = {N}个点位")

        V_grid = np.zeros((M+1, N+1)) # 预先生成包括0在内的期权价值矩阵
        S_array = np.linspace(0, M*ds, M+1) # 生产价格序列
        if CP == "C":
            V_grid[:, N] = np.maximum(S_array-K, 0) # 确定边界条件，到期时期权价值很好计算
            V_grid[M] = np.exp(-r*T2M_array) * (S_array[-1] * np.exp(b*T2M_array) - K)
        else:
            V_grid[:, N] = np.maximum(K-S_array, 0) # 确定边界条件，到期时期权价值很好计算
            V_grid[0] = np.exp(-r*T2M_array) * K       


        # 定义方程的系数的算法，方便后面计算，而且也比较直观
        aj = lambda i : 0.5 * i * (b - sigma**2 * i) * dt
        bj = lambda i : 1 + (r + sigma**2 * i**2) * dt
        cj = lambda i : 0.5 * i * (-b - sigma**2 * i) * dt

        # 用自定义的函数gen_diag有效减少代码
        coefficient_matrix = Pri_option.gen_diag(M, aj, bj, cj)
        M_inverse = np.linalg.inv(coefficient_matrix)     

        for j in range(N-1, -1, -1): # 隐式也是时间倒推循环，区别在于隐式是要解方程组
            # 准备好解方程组 fj = M**-1 * fj+1,M就是coefficient_matrix的逆矩阵，fj+1的第一项和最后一项需要减去pd*V_grid(0, j)和pu*V_grid(M, j)
            Z = np.zeros_like(V_grid[1:M, j]) # 用来存储边界条件
            Z[0] = aj(1) * V_grid[0, j] # 隐式这里用的边界条件是j而不是j+1
            Z[-1] = cj(M-1) * V_grid[-1, j]
            V_grid[1:M, j] = M_inverse @ (V_grid[1:M, j+1] - Z)

            # 美式期权提前行权判断
            if CP == "C":
                V_grid[1:M, j] = np.maximum(S_array[1:M]-K, V_grid[1:M, j])
            else:
                V_grid[1:M, j] = np.maximum(K-S_array[1:M], V_grid[1:M, j])
    
        return V_grid[S_idx, 0]  # 返回初0时点的初始价格的价值

    # implicit_FD(CP="P", S=36, K=40, T=0.5, sigma=0.4, r=0.06, b=0.06, M=500, N=2000)

    # 4.4.3、半隐式有限差分法定价
    def CN_FD(CP, S, K, T, sigma, r, b, M, N):
        """
        半隐式有限差分法，Crank_Nicolson，最稳定的方法，推荐
        f[i+1, j]    f[i+1, j+1]
        f[i, j]  ⬅  f[i, j+1]
        f[i-1, j]    f[i-1, j+1]
        """
        ds = S/M # 确定价格步长，用S的意义在于可以让S必定落在网格点上，后续不需要使用插值法
        M = int(K/ds) * 2 # 确定覆盖的价格范围，这里设置为2倍的行权价，也可根据需要设置为其他，这里根据价格范围重新计算价格点位数量M
        S_idx= int(S/ds) # S所在的index，用于方便确定初始S对应的期权价值
        dt = T/N # 重新确定步长dt，半隐式方法收敛性相对较好，不像显示那么依赖于dt必须得够小，所以这里直接指定N
        T_array = np.linspace(0, N*dt, N+1) # 时间序列
        T2M_array = T_array[-1] - T_array
        print(f"生产的网格：价格分为M = {M}个点位，时间分为N = {N}个点位")

        V_grid = np.zeros((M+1, N+1)) # 预先生成包括0在内的期权价值矩阵
        S_array = np.linspace(0, M*ds, M+1) # 生产价格序列
        if CP == "C":
            V_grid[:, N] = np.maximum(S_array-K, 0) # 确定边界条件，到期时期权价值很好计算
            V_grid[M] = np.exp(-r*T2M_array) * (S_array[-1] * np.exp(b*T2M_array) - K) # 表现为远期定价
        else:
            V_grid[:,N] = np.maximum(K-S_array, 0) # 确定边界条件，到期时期权价值很好计算
            V_grid[0] = np.exp(-r*T2M_array) * K


        # 定义方程的系数的算法，方便后面计算，而且也比较直观
        aj = lambda i : 0.25 * (sigma**2 * i**2 - b * i) * dt
        bj = lambda i : -0.5 * (r + sigma**2 * i**2) * dt
        cj = lambda i : 0.25 * (sigma**2 * i**2 + b * i) * dt
        matrix_ones = np.diag([1 for i in range(M-1)])
        matrix_1 = - Pri_option.gen_diag(M, aj, bj, cj) + matrix_ones
        matrix_2 = Pri_option.gen_diag(M, aj, bj, cj) + matrix_ones
        M1_inverse = np.linalg.inv(matrix_1)

        for j in range(N-1, -1, -1): #隐式也是时间倒推循环，区别在于隐式是要解方程组
            # 准备好解方程组 M_1 * fj = M_2 * fj+1 + b_1
            # Z是对边界条件的处理
            Z = np.zeros_like(V_grid[1:M, j+1])
            Z[0] = aj(1) * (V_grid[0, j] + V_grid[0, j+1])
            Z[-1] = cj(M-1) * (V_grid[-1, j] + V_grid[-1, j+1])

            V_grid[1:M, j] = M1_inverse @ ( matrix_2 @ V_grid[1:M, j+1] + Z)
            # print(f_j_1,V_grid[1:M,j])
            # 美式期权提前行权判断
            if CP == "C":
                V_grid[1:M, j] = np.maximum(S_array[1:M]-K, V_grid[1:M, j])
            else:
                V_grid[1:M, j] = np.maximum(K-S_array[1:M], V_grid[1:M, j])
        
        return V_grid[S_idx, 0]  # 返回初0时点的初始价格的价值

    # CN_FD(CP="P", S=36, K=40, T=0.5, sigma=0.4, r=0.06, b=0.06, M=500, N=2000)


