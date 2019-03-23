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
        #select *, count(distinct name) from table group by name
        #df = df.drop_duplicates(['trade_date'])
        #df = df.sort_values(by = 'trade_date',axis = 0,ascending = True)
        #df = df.reset_index()
        return df

    def push_mysql(self,database = 'daily_basic',start='20180802',end='20180809'):
        yconnect = create_engine('mysql+pymysql://root:152921@localhost:3306/'+database+'?charset=utf8') 
        tradedays_list = self.gettradedays(start,end)
        for day in tradedays_list:
            try:
                df = self.pro.query(database,trade_date = day)
                table = 't'+day
                pd.io.sql.to_sql(df,table,con=yconnect, schema=database,if_exists='replace') 
            except :
                print("err："+database+day)
                continue 

        yconnect.dispose()
        self.savemysqlrecorde(db =database,wstart = start,wend=end)

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
        self.savemysqlrecorde(db ='daily_basic_ts_code',wstart = start,wend=end)


    def gettradedays(self,start_date1='', end_date1=''):

        if start_date1 == '' and end_date1 == '' :
            df = self.pro.trade_cal(exchange='')
        else:    
            df = self.pro.trade_cal(exchange='', start_date=start_date1 , end_date=end_date1)
        tradedays_list = []
        for i in range(0, len(df)):
            if df.iloc[i]['is_open']==1:
                tradedays_list.append(df.iloc[i]['cal_date'])
        return tradedays_list
    
    def datanotinmysql(self,db = 'margin_detail',wstart = '20180205',wend='20180601'):
        start = int(wstart)
        end = int(wend)

        df = pd.read_csv(self.mysqlrecord)
        for index, row in df.iterrows():
            if (row['db'] == db) :
                if start > int(row['start']) and start < int(row['end']) and end < int(row['end']):
                    start = '0'
                    end = '0'
                    return start,end
                elif start >= int(row['start']) and start < int(row['end']) and end >= int(row['end']):
                    start = row['end']
                elif start < int(row['start']) and end > int(row['start']) and end < int(row['end']):
                    end = row['start']

        return str(start) , str(end)

    def savemysqlrecorde(self,db = 'margin_detail',wstart = '20180401',wend='20180501'):
        df = pd.DataFrame([[db,wstart,wend]])
        df.to_csv(self.mysqlrecord,mode='a',header = False)

    def getlatestday(self,db = 'margin_detail'):
        df = pd.read_csv(self.mysqlrecord)
        latestday = 0
        for index, row in df.iterrows():
            if (row['db'] == db) :
                if latestday < int(row['end']):
                    latestday = int(row['end'])
        return str(latestday)            

    def getts_code(self):
        df = self.pro.query('stock_basic',exchange='', list_status='L', fields='ts_code')
        return df

    def updatalldb(self):
        now = datetime.datetime.now().strftime('%Y%m%d')
        for db1 in self.db_func_list:
            s,e = self.datanotinmysql(db = db1,wstart = '20190101',wend = now )
            if s == e:
                continue
            elif db1 == 'daily_basic_ts_code':
                self.push_daily_basic(start=s,end=e)
            else:
                self.push_mysql(database = db1,start=s,end=e)

if __name__ == '__main__':
    d = datamodule()
    "d.push_mysql(database = 'daily_basic',start='20100101',end='20151231')"
    d.updatalldb()
    "print(d.datanotinmysql(db = 'daily_basic_ts_code',wstart = '20190101',wend='20190313'))"
    "df = d.pull_mysql(db = 'daily_basic_ts_code',date = '201706030',ts_code = '000004.SZ')"

  