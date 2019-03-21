import pandas as pd
from module import module as md
import datetime
import csv
import shutil
import os
from util import util 

'''
黑天鹅事件导致个股快速下跌，
统计黑天鹅事件bs_amount,保存在blackswan_csv里,在blackswan_csv1里增加未来3个月股价走势

统计黑天鹅后1个月，2个月，3个月股价是否回升及回升的幅度
统计该策略的成功率及成功幅度
'''
class blackswan(object):
    def __init__(self):
        self.blackday = 30
        self.droprate = 0.3
        self.blackswan_list = []
        self.blackswan_csv = 'E:\stock\stock\data\mblackswan.csv'
        self.blackswan_csv1 = 'E:\stock\stock\data\mblackswan1.csv'
        self.moniter_csv = 'E:\stock\stock\data\moniter_blackswan.csv'
        self.step = 1
        self.df = pd.DataFrame()
    def moniter(self):
        msql = md.datamodule()
        #add culmn name to self.df
        emptydf = pd.DataFrame()
        t_today = datetime.datetime.now()
        t_delta = datetime.timedelta(self.blackday+20)
        t_startday = t_today - t_delta
        today = t_today.strftime('%Y%m%d')
        startday = t_startday.strftime('%Y%m%d')
        ts_code_df = msql.getts_code()
        ts_day_list = msql.gettradedays(start_date1= startday,end_date1 = today)
        
        for i,code in ts_code_df.iterrows():
            df1 = emptydf
            for day in ts_day_list:
                dftemp = msql.pull_mysql(db = 'daily_basic_ts_code',date = day,ts_code = code['ts_code'])
                if not dftemp.empty:
                    df1= pd.concat([df1,dftemp],ignore_index = True)
            if not df1.empty:
                self.sub_findblackswan(df = df1)

        try:
            str = util.dftostring(df)
            util.sendmail(str)
            self.df.to_csv(self.moniter_csv,index=False)

        except:
            print("self.df.to_csv failed!!!")
        return 
        
    def train(self):
        self.sub_findall_blackswan()
        self.sub_getlaterprice()

    def sub_getlaterprice(self):
        msql = md.datamodule()
        tradeday_list = msql.gettradedays()
        df = pd.read_csv(self.blackswan_csv1)

        df['onemonth'] = 0
        df['twomonth'] = 0
        df['threemonth'] = 0

        total = len(df)
  
        for i,row in df.iterrows():
            trade_day = str(row['trade_date'])
            lestdays = len(tradeday_list)
            try:
                d = tradeday_list.index(trade_day)
            except:
                print("err：trade day not in list"+trade_day)
                continue
            if d + 90 <= lestdays:
                date_treemonth = tradeday_list[d+90]
            else:
                date_treemonth = tradeday_list[lestdays-1]
            if d + 60 <= lestdays:
                date_twoonth = tradeday_list[d+60]  
            else:
                date_twoonth = tradeday_list[lestdays-1]
            if d + 30 < lestdays:
                date_onemonth = tradeday_list[d+30]
            else:
                date_onemonth = tradeday_list[lestdays-1]
            
            df_onemonth = msql.pull_mysql(db = 'daily_basic_ts_code',date = date_onemonth,ts_code = row['ts_code'])
            df_twomonth = msql.pull_mysql(db = 'daily_basic_ts_code',date = date_twoonth,ts_code = row['ts_code'])
            df_threemonth = msql.pull_mysql(db = 'daily_basic_ts_code',date = date_treemonth,ts_code = row['ts_code'])
            
            if not df_onemonth.empty:
                df.at[i,'onemonth'] = df_onemonth.loc[0]['total_mv']
            if not df_twomonth.empty:
                df.at[i,'twomonth'] = df_twomonth.loc[0]['total_mv']
            if not df_threemonth.empty:
                df.at[i,'threemonth'] = df_threemonth.loc[0]['total_mv']

        df.to_csv(self.blackswan_csv1,index=False)
        return
 
    def sub_findall_blackswan(self):
        ts_code_df = msql.getts_code()

        for i,code in ts_code_df.iterrows():
            df1 = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = code['ts_code'])
            self.sub_findblackswan(df = df1)
        df = d.removedupdate(d.self.df)
        df.to_csv(self.blackswan_csv,index=False)
        shutil.copy(self.blackswan_csv,self.blackswan_csv1)

        return 

    def sub_findblackswan(self,df):
        lenth = len(df)
        lastday = self.blackday
        for i, row in df.iterrows():
            if (i+lastday >= lenth):
                break
            #控制步长

            if (i % self.step) != 0:
                continue
   
            if (df.loc[i]['total_mv'] == 0):
                continue

            rate = (df.loc[i]['total_mv'] - df.loc[i+lastday-1]['total_mv'])/df.loc[i]['total_mv']
            if rate > self.droprate:
                self.df = pd.concat([self.df,df.loc[i+lastday-1:i+lastday-1,]],ignore_index = True)

    def removedupdate(self,df):
        df1 = pd.DataFrame()
        df['trade_date'] = df['trade_date'].astype('int')
        lenth = len(df)
        for i, row in df.iterrows():
            if i+1 < lenth:
                if df.loc[i+1]['trade_date'] - df.loc[i]['trade_date'] > 10 :
                    df1 = pd.concat([df1,df.loc[i:i,]],ignore_index=True)
        df1 = pd.concat([df1,df.loc[lenth-1:lenth-1,]],ignore_index=True)
        return df1

    def test_findoneblackswan(self,code = '000001.SZ'):  
        msql = md.datamodule()      
        df1 = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = code)
        self.sub_findblackswan(df = df1)
        print(self.df)

if __name__ == '__main__':
    d = blackswan()
    "d.sub_findblackswan('20150101','20190312')"
    "d.sub_getlaterprice()"
    #d.moniter()
    #d.train()
    #d.test_findoneblackswan('600703.SH')
    #d.test_findoneblackswan('600519.SH')
    #d.test_findoneblackswan('600887.SH')
    #df = pd.read_csv(d.blackswan_csv)
    #df = d.removedupdate(df)
    #df.to_csv(d.blackswan_csv1,index=False)
    d.sub_getlaterprice()

    #util.sendmail(mailcontent = 'monitor finished')


    
  