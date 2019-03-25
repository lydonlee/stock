import tushare as ts
import pandas as pd  
from sqlalchemy import create_engine
import datetime
import csv
import config
class datamodule(object):
    def __init__(self):
        cfg = config.configs.module
        ts.set_token('1360474e70eee70c9c1b9740d684a8800e89903641c6ffb82ac549da')
        self.pro = ts.pro_api()
        self.db_func_list = cfg.db_func_list
        self.mysqlrecord = cfg.mysqlrecord
        self.mysqlcmd = cfg.mysqlcmd
    
    def pull_mysql(self,db = 'daily_basic',date = None,limit = None,ts_code = '300552.SZ'):
        concmd = self.mysqlcmd.format(db)
        yconnect = create_engine(concmd) 
        df = pd.DataFrame()
        if db == 'daily_basic_ts_code': 
            '以数字开头的数据库名字要用1前边的那个符号引用才能识别'
            if limit != None:
                sql_cmd = 'select * from `'+ts_code + '`' +'order by trade_date DESC limit '+ str(limit) 
            elif date == None:
                sql_cmd = 'select * from `'+ts_code + '`' +'order by trade_date DESC'
            else:
                sql_cmd = 'select * from `'+ts_code + '`' +'where trade_date = '+date
        else:
            sql_cmd = 'select * from '+'t'+ date

        try:
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
        except:
            print("err：pull_mysql:"+sql_cmd)
        yconnect.dispose()

        return df

    def push_mysql(self,database = 'daily_basic',start='20180802',end='20180809',firsttime = 0):
        concmd = self.mysqlcmd.format(database)
        yconnect = create_engine(concmd)
        tradedays_list = self.gettradedays(start,end,firsttime = firsttime)
        for day in tradedays_list:
            try:
                df = self.pro.query(database,trade_date = day)
                table = 't'+day
                pd.io.sql.to_sql(df,table,con=yconnect, schema=database,if_exists='replace') 
            except :
                print("err："+database+day)
                continue 

        yconnect.dispose()

    def push_daily_basic(self,start='20180802',end='20180809',firsttime = 0):
        sqlcmd=self.mysqlcmd.format('daily_basic_ts_code')
        yconnect = create_engine(sqlcmd) 
       
        df1 = self.getts_code()

        for index, code in df1.iterrows():
            try:
                if firsttime == 1:
                    df = self.pro.query('daily_basic',ts_code = code['ts_code'])
                else:
                    df = self.pro.query('daily_basic',ts_code = code['ts_code'],start_date = start,end_date=end)
                
                pd.io.sql.to_sql(df,code['ts_code'],con=yconnect, schema='daily_basic_ts_code',if_exists='append') 
            except :
                print("err："+code['ts_code'])
                continue 
            print(code['ts_code'])
            
        
        yconnect.dispose()

    def getstock_basic(self):
        df = self.pro.query('stock_basic',exchange='', list_status='L')
        return df
    def gettradedays(self,start_date1='', end_date1='',firsttime = 0):

        if firsttime == 1 :
            df = self.pro.trade_cal(exchange='')
        else:    
            df = self.pro.trade_cal(exchange='', start_date=start_date1 , end_date=end_date1)
        tradedays_list = []
        for i in range(0, len(df)):
            if df.iloc[i]['is_open']==1:
                tradedays_list.append(df.iloc[i]['cal_date'])
        return tradedays_list

    def getlatestday(self,db = 'margin_detail'):
        concmd = self.mysqlcmd.format(db)
        yconnect = create_engine(concmd)
        if db == 'daily_basic_ts_code':
            sql_cmd = "select trade_date from `000002.SZ` order by trade_date DESC limit 1"
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
            lastday = df.iloc[0][0]
        
        else:
            sql_cmd = "SELECT  TABLE_NAME  FROM information_schema.TABLES WHERE  TABLE_SCHEMA="+"'"+db+"'"+" order by TABLE_NAME DESC LIMIT 1"
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
            ttable = df.iloc[0][0]
            lastday = ttable[1:]
        return lastday           

    def getts_code(self):
        df = self.pro.query('stock_basic',exchange='', list_status='L', fields='ts_code')
        return df

    def updatalldb(self,firsttime = 0):
        now = datetime.datetime.now().strftime('%Y%m%d')
        for db1 in self.db_func_list:
            try:
                s = self.getlatestday(db1)
            except:
                print('not find database,the first time update!')
                firsttime = 1
                s = '19950101'

            if s == now:
                print('already the latest db!')
                continue
            elif db1 == 'daily_basic_ts_code':
                self.push_daily_basic(start=s,end=now,firsttime=firsttime)
            else:
                self.push_mysql(database = db1,start=s,end=now,firsttime=firsttime)
    #查看没有下载的数据库，重新下载            
    def fix_daily_basic_ts_code(self):
        sqlcmd=self.mysqlcmd.format('daily_basic_ts_code')
        yconnect = create_engine(sqlcmd) 
       
        df1 = self.getts_code()

        for index, code in df1.iterrows():
            sql_cmd = "SELECT  *  FROM information_schema.TABLES WHERE  TABLE_SCHEMA='daily_basic_ts_code' and TABLE_NAME="+"'" +code['ts_code']+"'"
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
            if df.empty:
                try:
                    df = self.pro.query('daily_basic',ts_code = code['ts_code'])
                    pd.io.sql.to_sql(df,code['ts_code'],con=yconnect, schema='daily_basic_ts_code',if_exists='replace') 
                except :
                    print("err："+code['ts_code'])
                continue 

if __name__ == '__main__':
    d = datamodule()
    "d.push_mysql(database = 'daily_basic',start='20100101',end='20151231')"
    d.updatalldb()
    "print(d.datanotinmysql(db = 'daily_basic_ts_code',wstart = '20190101',wend='20190313'))"
    "df = d.pull_mysql(db = 'daily_basic_ts_code',date = '201706030',ts_code = '000004.SZ')"

  