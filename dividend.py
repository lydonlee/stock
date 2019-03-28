import pandas as pd
from module import module as md
import datetime
import config
#使用dividend数据库
class dividend(object):
    def __init__(self):
        cfg = config.configs.dividend
        self.dividend_csv = cfg.dividend_csv
        
    def build(self):
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        df = pd.DataFrame()
        latestday = msql.getlatestday('daily_basic')
        df_now = msql.pull_mysql(db = 'daily_basic',date = latestday)
        df_now.set_index(["ts_code"], inplace=True) 
 
        for i,code in ts_code_df.iterrows():
            #取到的数据默认按照日期排序，最近的排在前面
            
            df1 = msql.pull_mysql(db = 'dividend',ts_code = code['ts_code'])
            if not df1.empty:
                closenow = df_now['close'][code][0]
                df1['dividenrate'] = df1['cash_div_tax']*100/closenow      
                df = pd.concat([df,df1],ignore_index = True)
        if not df.empty:
            df = df.sort_values('dividenrate',ascending = False)
            df.to_csv(self.dividend_csv,index=False)
        else:
            str = 'error'
            print(str)
        return 
if __name__ == '__main__':
    d = dividend()
    d.build()
