import tushare as ts
import pandas as pd  
from sqlalchemy import create_engine

class datamodule(object):
    def __init__(self):
        ts.set_token('1360474e70eee70c9c1b9740d684a8800e89903641c6ffb82ac549da')
        self.pro = ts.pro_api()
        self.db_func_list = ['daily_basic','margin_detail']

    def pullcoherentdata(self,code = '000001.sz',start = '20180101',end='20180201'):
        yconnect = create_engine('mysql+pymysql://root:152921@localhost:3306/margin_detail?charset=utf8')  
        
        tradedays_list = self.gettradedays(start,end)
        for day in tradedays_list:
            table = 't' + day
            sql_cmd = "SELECT * FROM table"
            try:
                df = pd.read_sql(sql=sql_cmd, con=yconnect)
            except:
                print("err："+day)
                continue 
        
        return

    def fush_mysql(self,db = 'daily_basic',start='20180802',end='20180809'):
        yconnect = create_engine('mysql+pymysql://root:152921@localhost:3306/'+db+'?charset=utf8') 
        tradedays_list = self.gettradedays(start,end)
        for day in tradedays_list:
            try:
                df = self.pro.query(db,trade_date = date)
            except :
                print("err："+db+day)
                continue 
            table = 't'+day
            pd.io.sql.to_sql(df,table,con=yconnect, schema=db,if_exists='replace') 

        yconnect.dispose()

    def gettradedays(self,start_date1='20180101', end_date1='20181231'):

        df = self.pro.trade_cal(exchange='', start_date=start_date1 , end_date=end_date1)
        tradedays_list = []
        for i in range(0, len(df)):
            if df.iloc[i]['is_open']==1:
                tradedays_list.append(df.iloc[i]['cal_date'])
        return tradedays_list

    def getts_code(self):
        df = self.pro.query('stock_basic', exchange='', list_status='L')
        return df['ts_code']

'''
    def push_margin_detail(self,start='20180802',end='20180809'):
        yconnect = create_engine('mysql+pymysql://root:152921@localhost:3306/margin_detail?charset=utf8')  
        
        tradedays_list = self.gettradedays(start,end)
        for day in tradedays_list:
            try:
                df = self.pro.margin_detail(trade_date=day)
            except :
                print("err："+day)
                continue 
            table = 't'+day
            pd.io.sql.to_sql(df,table,con=yconnect, schema='margin_detail',if_exists='append') 

        yconnect.dispose()

    def push_daily_basic(self,start='20180802',end='20180809'):
        yconnect = create_engine('mysql+pymysql://root:152921@localhost:3306/daily_basic?charset=utf8') 
        tradedays_list = self.gettradedays(start,end)
        for day in tradedays_list:
            try:
                df = self.pro.daily_basic(trade_date=date)
            except :
                print("err："+day)
                continue 
            table = 't'+day
            pd.io.sql.to_sql(df,table,con=yconnect, schema='daily_basic',if_exists='append') 

        yconnect.dispose()
'''
if __name__ == '__main__':
    d = datamodule()
    d.push_daily_basic()
    "d.push_margin_detail(start= '20140101',end = '20151231')"