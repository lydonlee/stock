import pandas as pd
from module import module as md
import config
#使用daily_basic，daily_basic_ts_code数据库，
class basicvalue(object):
    def __init__(self):
        cfg = config.configs.basicvalue
        self.periodbyday = 300
        self.df = pd.DataFrame()
        self.col_list = ['close','pe_ttm','pb','turnover_rate_f','ps_ttm','total_mv']
        self.basic_csv = cfg.basic_csv
        self.monitor_basic = cfg.monitor_basic
        self.rateth = {'closerate':5 ,'pe_ttmrate':5,'pbrate':5,'turnover_rate_frate':5,'ps_ttmrate':5,'total_mvrate':5}
        self.recommand_basic = cfg.recommand_basic
    
    def moniter(self):
        msql = md.datamodule()
        latestday = msql.getlatestday('daily_basic')
        df_now = msql.pull_mysql(db = 'daily_basic',date = latestday)
        #df_now.set_index(["ts_code"], inplace=True)

        #读取过去几年最高价和最低价记录
        df_basic = pd.read_csv(self.basic_csv)
        #df_basic.set_index(["ts_code"], inplace=True) 

        for col in self.col_list:
            collow = col +'low'
            colhigh = col+'high'
            colrate = col+'rate'

            df_basic[col] = df_now[col]
            df_basic[colrate] = (df_now[col] - df_basic[collow])/(df_basic[colhigh]-df_basic[collow])*100
        
        df_basic.to_csv(self.monitor_basic)

    def recommand(self):
        dfr = pd.DataFrame()
        df = pd.read_csv(self.monitor_basic)
        for (key,value) in self.rateth.items():
            df1 = df[df[key] < value]
            df1[key+'r'] = 1
            dfr = pd.concat([dfr,df1],ignore_index = False,sort=False)
        dfr.to_csv(self.recommand_basic,index = False)

    def builddf(self):
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        for i,code in ts_code_df.iterrows():   
            df1 = msql.pull_mysql(db = 'daily_basic_ts_code',limit = self.periodbyday,ts_code = code['ts_code'])
            if not df1.empty:
                for c in self.col_list:
                    self._appendcol(df = df1,by= c,r =code['ts_code'])

        self.df.to_csv(self.basic_csv,index_value='ts_code')    
            
    def _appendcol(self,df,by = 'close',r = 0):
        collow = by +'low'
        colhigh = by+'high'
        
        d = df.agg({ by: ['min', 'max']})

        self.df.at[r,collow] = d.loc['min'][by]
        self.df.at[r,colhigh] = d.loc['max'][by]
        
    def testlatestday(self):
        msql = md.datamodule()
        latestday = msql.getlatestday('daily_basic_ts_code')
        print(latestday)

if __name__ == '__main__':
    b = basicvalue()
    msql = md.datamodule()
    #msql.push_daily_basic(start='19950101',end='20190324',firsttime = 1)
    #df = msql.pull_mysql(db = 'daily_basic_ts_code',limit = b.periodbyday,ts_code = '300750.SZ')
    #b._appendcol(df,by = 'close',r = '300750.SZ')
    #b.testlatestday()
    msql.fix_db(db = 'dividend')
    #df.to_csv(b.recommand_basic)
    #b.builddf()
    #b.moniter()
    #b.recommand()
    #b.recommand_sum()


        