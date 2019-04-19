import tushare as ts
import pandas as pd  
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import datetime
import csv
import config
import time

def _lower(x):
    return x.lower()
class datamodule(object):
    def __init__(self):
        cfg = config.configs.module
        #self.db_func_list = cfg.db_func_list
        self.mysqlrecord = cfg.mysqlrecord
        self.mysqlcmd = cfg.mysqlcmd
        
    def _init_tushare(self):
        ts.set_token('1360474e70eee70c9c1b9740d684a8800e89903641c6ffb82ac549da')
        self.pro = ts.pro_api()

    def pull_data(self,db = 'daily_basic',date = None,limit = None,ts_code = '300552.SZ'):
        df = self.pro.query(db,ts_code = ts_code)
        return df

    def pull_mysql(self,db = 'daily_basic',date = None,limit = None,ts_code = '300552.SZ'):
        concmd = self.mysqlcmd.format(db)
        yconnect = create_engine(concmd) 
        df = pd.DataFrame()
        if db == 'daily_basic_ts_code' : 
            '以数字开头的数据库名字要用1前边的那个符号引用才能识别'
            if limit != None:
                sql_cmd = 'select * from `'+ts_code + '`' +'order by trade_date DESC limit '+ str(limit) 
            elif date == None:
                sql_cmd = 'select * from `'+ts_code + '`' +'order by trade_date DESC'
            else:
                sql_cmd = 'select * from `'+ts_code + '`' +'where trade_date = '+date
        elif db == 'dividend' or db == 'income' or db == 'cashflow' or db == 'balancesheet' or db == 'fina_indicator':
            sql_cmd = 'select * from `'+ts_code + '`'
        elif db == 'future_income':
            sql_cmd = 'select * from future_income where ts_code ='+ts_code[:-3]
        else:
            sql_cmd = 'select * from '+'t'+ date

        try:
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
        except:
            print("err：pull_mysql:"+db+sql_cmd)
        yconnect.dispose()

        return df

    def _push_mysql(self,database = 'daily_basic',start='20180802',end='20180809',firsttime = 0):
        concmd = self.mysqlcmd.format(database)
        yconnect = create_engine(concmd)
        tradedays_list = self.gettradedays(start,end,firsttime = firsttime)
        for day in tradedays_list:
            try:
                df = self.pro.query(database,trade_date = day)
                table = 't'+day
                if not df.empty:
                    pd.io.sql.to_sql(df,table,con=yconnect, schema=database,if_exists='replace') 
            except :
                print("err："+database+day)
                continue 
        yconnect.dispose()
    def _push_daily_basic(self,start='20180802',end='20180809',firsttime = 0):
        sqlcmd=self.mysqlcmd.format('daily_basic_ts_code')
        yconnect = create_engine(sqlcmd) 
       
        df1 = self.getts_code()

        for index, code in df1.iterrows():
            ts_code = code['ts_code'].lower()
            time.sleep(1)
            try:
                if firsttime == 1:
                    df = self.pro.query('daily_basic',ts_code = ts_code)
                else:
                    df = self.pro.query('daily_basic',ts_code = ts_code,start_date = start,end_date=end)
                
                if not df.empty:
                    pd.io.sql.to_sql(df,ts_code,con=yconnect, schema='daily_basic_ts_code',if_exists='append') 
            except Exception as e:
                print(e)
                continue 
        yconnect.dispose()

    def _push_by_code(self,db):
        sqlcmd=self.mysqlcmd.format(db)
        yconnect = create_engine(sqlcmd) 
       
        df1 = self.getts_code()

        for index, code in df1.iterrows():
            ts_code = code['ts_code'].lower()
            try:
                df = self.pro.query(db,ts_code = ts_code)
                if not df.empty:
                    pd.io.sql.to_sql(df,ts_code,con=yconnect, schema=db,if_exists='replace') 
            except Exception as e:
                print(e)
                continue 
     
        yconnect.dispose()

    #为df增加股票名字，行业等基本信息    
    def joinnames(self,df):
        df1 = self.gettable(pdb='stock_basic',ptable = 'stock_basic')
        df1.set_index(["ts_code"], inplace=True,drop = True)
        if df.index.name != 'ts_code':
            df.set_index(["ts_code"], inplace=True,drop = True)
        df = pd.concat([df,df1],axis=1,join_axes=[df.index])
        return df

    def _push_db(self,db):
        sqlcmd=self.mysqlcmd.format(db)
        yconnect = create_engine(sqlcmd) 
        if db == 'stock_basic' or db =='trade_cal':
            df = self.pro.query(db)
        elif db == 'index_daily':
            df = self.pro.query(db,ts_code = '000001.SH')
        
        pd.io.sql.to_sql(df,db,con=yconnect, schema=db,if_exists='replace') 
        yconnect.dispose()

    def gettable(self,pdb = 'stock_basic',ptable = 'stock_basic'):
        concmd = self.mysqlcmd.format(pdb)
        yconnect = create_engine(concmd) 
        df = pd.DataFrame()
       
        sql_cmd = 'select * from '+ ptable
       
        try:
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
        except:
            print("err：pull_mysql:"+sql_cmd)
        yconnect.dispose()
        return df


    def _getstock_basic_net(self):
        self._init_tushare()
        df = self.pro.query('stock_basic',exchange='', list_status='L')
        return df

    def gettradedays(self,start_date1=None, end_date1=None,firsttime = 0):
        concmd = self.mysqlcmd.format('trade_cal')
        yconnect = create_engine(concmd) 
        df = pd.DataFrame()
        if firsttime == 1 or (start_date1==None and end_date1==None):
            sql_cmd = 'select  cal_date from trade_cal where is_open = 1'
        elif start_date1 != None and end_date1 != None:
            sql_cmd = "select  cal_date from trade_cal where is_open = 1 and cal_date >= '"+ start_date1 +"' and cal_date <= '"+end_date1+"'"
        elif start_date1 != None and end_date1 == None:
            sql_cmd = "select  cal_date from trade_cal where is_open = 1 and cal_date >= '"+ start_date1+"'"
        elif start_date1 == None and end_date1 != None:
            sql_cmd = "select  cal_date from trade_cal where is_open = 1 and cal_date <= '"+ end_date1+"'"
        try:
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
        except:
            print("err：pull_mysql:"+sql_cmd)
        yconnect.dispose()

        return df['cal_date'].tolist()

    def gettradedays_net(self,start_date1='', end_date1='',firsttime = 0):
        self._init_tushare()
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
        concmd = self.mysqlcmd.format('stock_basic')
        yconnect = create_engine(concmd) 
        df = pd.DataFrame()
        sql_cmd = 'select ts_code from stock_basic'

        try:
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
        except:
            print("err：pull_mysql:"+sql_cmd)
        yconnect.dispose()
        #df['ts_code'] = df['ts_code'].apply(lambda x: _lower(x))
        return df

    def getts_code_net(self):
        self._init_tushare()
        df = self.pro.query('stock_basic',exchange='', list_status='L', fields='ts_code')
        df['ts_code'] = df['ts_code'].apply(lambda x: _lower(x))
        return df

    def updatedbone(self,db1 = 'stock_basic',firsttime=0):
        self._init_tushare()
        now = datetime.datetime.now().strftime('%Y%m%d')
        try:
            s = self.getlatestday(db1)
        except:
            print('not find database,the first time update!')
            firsttime = 1
            s = '19950101'
        if firsttime == 1:
            try :
                self._createdb(db1)
            except Exception as e:
                print(e)
        if s == now:
            print('already the latest db!')
            return
        elif db1 == 'daily_basic_ts_code':
            self._push_daily_basic(start=s,end=now,firsttime=firsttime)
        elif db1 == 'dividend' or db1 == 'income' or db1 == 'cashflow' or db1 == 'balancesheet' or db1 =='fina_indicator':
            self._push_by_code(db1)
        elif db1 == 'stock_basic' or db1 == 'trade_cal' or db1=='index_daily' :
            self._push_db(db1)
        elif db1 == 'future_income':
            self._pushfutureincome()
        else:
            self._push_mysql(database = db1,start=s,end=now,firsttime=firsttime)

    def updatedball(self,firsttime = 0,daily = True,quter=False):
        
        cfg = config.configs.module
        if daily and quter:
            db_list = cfg.db_update_day + cfg.db_update_quter
        elif daily:
            db_list = cfg.db_update_day
        elif quter:
            db_list = cfg.db_update_quter
                      
        for db1 in db_list:
            self.updatedbone(db1 = db1,firsttime=firsttime)
            self.fix_db(db = db1)
    
    #查看没有下载的数据库，重新下载,判断依据是没有创建表，不判断表里的内容是否为最新  
    def fix_db(self,db = 'daily_basic_ts_code'):
        self._init_tushare()
        if (db == 'daily_basic_ts_code' or db == 'dividend' or db == 'income' or db == 'cashflow' or db == 'balancesheet' or db == 'fina_indicator'):
            self._fix_by_ts_code(sqldb=db) 
        elif db == 'stock_basic' or db == 'trade_cal' or db =='future_income' or db=='index_daily' :
            return
        else:
            self._fix_by_time(db)

    def _fix_by_ts_code(self,sqldb):
        if sqldb == 'daily_basic_ts_code':
            tsdb = 'daily_basic'
        else:
            tsdb = sqldb
        sqlcmd=self.mysqlcmd.format(sqldb)
        yconnect = create_engine(sqlcmd) 

        df1 = self.getts_code()
     
        for index, code in df1.iterrows():
            sql_cmd = "SELECT  TABLE_NAME  FROM information_schema.TABLES WHERE  TABLE_SCHEMA='" + sqldb + "' and TABLE_NAME="+"'" + code['ts_code'] + "'"
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
            if df.empty:
                try:
                    df = self.pro.query(tsdb,ts_code = code['ts_code'])
                    pd.io.sql.to_sql(df,code['ts_code'],con=yconnect, schema=sqldb,if_exists='replace') 
                except :
                    print("err："+code['ts_code'])
                continue 

    def _fix_by_time(self,db = 'daily_basic'):
        sqlcmd=self.mysqlcmd.format(db)
        yconnect = create_engine(sqlcmd) 
        
        lsdays = self.gettradedays(firsttime = 1)
        for day in lsdays:
            table = 't'+day
            sql_cmd = "SELECT  TABLE_NAME  FROM information_schema.TABLES WHERE  TABLE_SCHEMA='"+db+"' AND TABLE_NAME='"+ table+"'"
            df = pd.read_sql(sql=sql_cmd, con=yconnect)
            if df.empty:
                try:
                    df = self.pro.query(db,trade_date = day)
                    pd.io.sql.to_sql(df,table,con=yconnect, schema=database,if_exists='replace') 
                except :
                    print("err："+ table)
                continue 
    def _createdb(self,db):
        #选一个已经存在的数据库先连上
        concmd = self.mysqlcmd.format('performance_schema')
        yconnect = create_engine(concmd) 
        conn = yconnect.connect()
        conn.execute("commit")

        sqlcmd = "create database " + db
        conn.execute(sqlcmd)
        conn.close()
    def geturl(self,url = ''):
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.managed_default_content_settings.images": 2,
                 'profile.default_content_setting_values' :{'notifications' : 2}
        }
        chrome_options.add_experimental_option("prefs", prefs)

        #browser = webdriver.Chrome('/Users/liligong/anaconda/lib/python3.6/site-packages/chromedriver',
        #                           chrome_options=chrome_options)
        #browser = webdriver.Remote("http://localhost:4444/wd/hub", webdriver.DesiredCapabilities.HTMLUNITWITHJS)
        browser = webdriver.Chrome(chrome_options=chrome_options)
        browser.implicitly_wait(10)
        try:
            browser.get(url)
        except Exception as e:
            print(e)
        return browser

    def _pushfutureincome(self):
        db = 'future_income'
        concmd = self.mysqlcmd.format(db)
        yconnect = create_engine(concmd) 
        df = pd.DataFrame()
        url_formate = 'http://stockpage.10jqka.com.cn/{}/worth/#forecast'
        dft = self.getts_code()
        driver = self.geturl()
        for index, code in dft.iterrows():
            ts_code = code['ts_code'][:-3]
            url = url_formate.format(ts_code)

            driver.get(url)
            driver.implicitly_wait(10)
            try:
                y2019 = driver.find_element_by_xpath("//*[@id='forecast']/div[2]/div[2]/div[2]/table/tbody/tr[1]/th").text
                price2019 = driver.find_element_by_xpath("//*[@id='forecast']/div[2]/div[2]/div[2]/table/tbody/tr[1]/td[3]").text

                y2020 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[2]/th').text
                price2020 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[2]/td[3]').text

                y2021 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[3]/th').text
                price2021 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[3]/td[3]').text
            except Exception as e:
                print(e)
                continue
            d = {'ts_code': [ts_code,ts_code,ts_code], 'year': [y2019,y2020,y2021],'n_income':[price2019,price2020,price2021]}
            df1 = pd.DataFrame.from_dict(d)
            pd.io.sql.to_sql(df1,db,con=yconnect, schema=db,if_exists='append')#,index = False)
            #df = pd.concat([df,df1],ignore_index = False,sort=False)

        #pd.io.sql.to_sql(df,db,con=yconnect, schema=db,if_exists='replace')

if __name__ == '__main__':
    d = datamodule()
    "d.push_mysql(database = 'daily_basic',start='20100101',end='20151231')"
    #d.updatalldb()
    d._createdb('income')
    "print(d.datanotinmysql(db = 'daily_basic_ts_code',wstart = '20190101',wend='20190313'))"
    "df = d.pull_mysql(db = 'daily_basic_ts_code',date = '201706030',ts_code = '000004.SZ')"

  