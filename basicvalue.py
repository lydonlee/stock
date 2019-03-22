import pandas as pd
from module import module as md
import datetime
from config import config

class basicvalue(object):
    def __init__(self):
        cfg = config.configs.blackswan
        self.periodbyday = 1000
        self.df = pd.DataFrame()
        self.col_list = ['close','pe_ttm','pb','turnover_rate_f','ps_ttm','total_mv']
        self.basic_csv = cfg.basic_csv
        self.monitor_basic = cfg.monitor_basic

    def moniter(self):
        msql = md.datamodule()
        latestday = msql.getlatestday('daily_basic')
        df_now = msql.pull_mysql(db = 'daily_basic',date = latestday)
        df_now.set_index(["ts_code"], inplace=True)

        df_basic = pd.read_csv(self.basic_csv)
        df_basic.set_index(["ts_code"], inplace=True) 

        for col in self.col_list:
            collow = col +'low'
            colhigh = col+'high'
            colrate = col+'rate'
            for i,row in df_basic.iterrows(): 
                try:
                    now = df_now.loc[i][col]
                    low = df_basic.loc[i][collow]
                    high = df_basic.loc[i][colhigh]

                    df_basic.at[i,col] = df_now.loc[i][col] 
                    df_basic.at[i,colrate] = (now-low)/(high - low)*100
                except:
                    print('err:not find this',i)
        
        df_basic.to_csv(self.monitor_basic)
        
    def builddf(self):
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        for i,code in ts_code_df.iterrows():   
            df1 = msql.pull_mysql(db = 'daily_basic_ts_code',limit = self.periodbyday,ts_code = code['ts_code'])
            if not df1.empty:
                for c in self.col_list:
                    self.appendcol(df = df1,by= c,r =code['ts_code'])

        self.df.to_csv(self.basic_csv)    
            
    def appendcol(self,df,by = 'close',r = 0):
        collow = by +'low'
        colhigh = by+'high'
        
        d = df.agg({ by: ['min', 'max']})

        self.df.at[r,collow] = d.loc['min'][by]
        self.df.at[r,colhigh] = d.loc['max'][by]
        
    def testlatestday(self):
        msql = md.datamodule()
        latestday = msql.getlatestday('daily_basic')
        print(latestday)
if __name__ == '__main__':
    b = basicvalue()
    b.moniter()
        