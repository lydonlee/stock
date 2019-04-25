import pandas as pd
import module as md
import csv
import shutil
import os
from util import util 
import config

'''
黑天鹅事件导致个股快速下跌，
统计黑天鹅事件bs_amount,保存在blackswan_csv里,在blackswan_csv1里增加未来3个月股价走势

统计黑天鹅后1个月，2个月，3个月股价是否回升及回升的幅度
统计该策略的成功率及成功幅度
板块指数，行业指数，历史统计成功率
'''
class blackswan(object):
    def __init__(self):
        cfg = config.configs.blackswan
        self.blackday = 30
        self.droprate = 0.15
        self.blackswan_list = []
        self.blackswan_csv = cfg.blackswan_csv
        self.blackswan_csv1 = cfg.blackswan_csv1
        self.moniter_csv = cfg.moniter_csv
        self.step = 1
        self.indicator = 'total_mv'
        self.df = pd.DataFrame()

    def moniter(self):
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        df = pd.DataFrame()

        for i,code in ts_code_df.iterrows():
            #取到的数据默认按照日期排序，最近的排在前面
            df1 = msql.pull_mysql(db = 'daily_basic_ts_code',limit = self.blackday,ts_code = code['ts_code'])
            if not df1.empty:
                rate = self._isblackswan(df = df1)
                if rate > self.droprate:
                    df1.at[0,'droprate'] = rate
                    df = pd.concat([df,df1.loc[0:0,]],ignore_index = True)
  
        if not df.empty:
            str = util.dftostring(df)
            df = msql.joinnames(df=df)
            df.to_csv(self.moniter_csv,index=True)
        else:
            str = '没发现黑天鹅事件！'
            print(str)
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
        df['onedate'] = 0
        df['twodate'] = 0
        df['threedate'] = 0

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
                df.at[i,'onedate'] = df_onemonth.loc[0]['trade_date']                
            if not df_twomonth.empty:
                df.at[i,'twomonth'] = df_twomonth.loc[0]['total_mv']
                df.at[i,'twodate'] = df_twomonth.loc[0]['trade_date'] 
            if not df_threemonth.empty:
                df.at[i,'threemonth'] = df_threemonth.loc[0]['total_mv']
                df.at[i,'threedate'] = df_threemonth.loc[0]['trade_date']

        df.to_csv(self.blackswan_csv1,index=False)
        return
 
    def sub_findall_blackswan(self):
        msql = md.datamodule()
        ts_code_df = msql.getts_code()

        for i,code in ts_code_df.iterrows():
            df1 = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = code['ts_code'])
            self.sub_findblackswan(df = df1)
        df = self.removedupdate(self.df)
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
            print(rate)
            if rate > self.droprate:
                self.df = pd.concat([self.df,df.loc[i+lastday-1:i+lastday-1,]],ignore_index = True)

    def _isblackswan(self,df):
            nowin = df.iloc[0][self.indicator]
            d = df.agg({ self.indicator: ['min', 'max']})
            max = d.loc['max'][self.indicator]
            rate = (max - nowin)/max
            return rate
  
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
        df1 = msql.pull_mysql(db = 'daily_basic_ts_code',limit = self.blackday,ts_code = code)
        self._isblackswan(df = df1)
        print(df1)

if __name__ == '__main__':
    #d = blackswan()
    m = md.datamodule()
    m.updatalldb()

    "d.sub_findblackswan('20150101','20190312')"
    "d.sub_getlaterprice()"
    #d.moniter()
    #d.train()
    #d.test_findoneblackswan('000023.SZ')
    #d.test_findoneblackswan('600519.SH')
    #d.test_findoneblackswan('600887.SH')
    #df = pd.read_csv(d.blackswan_csv)
    #df = d.removedupdate(df)
    #df.to_csv(d.blackswan_csv1,index=False)
    #d.sub_getlaterprice()
    
    #util.sendmail(mailcontent = 'monitor finished')


    
  