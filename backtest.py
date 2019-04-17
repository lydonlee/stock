import pandas as pd
from module import module as md
import config
import FCFF as fc

journalaccount = {'ts_code':[],'date':[],'price':[],'Shares':[],'buysell':[],'buypower':[],}
class Backtest(object):
    def __init__(self,pstartdate = None,penddate = None,pwhichAccount = 0):
        self.startdate   = pstartdate
        self.enddate     = penddate
        self.account     = Account(pwhichAccount = pwhichAccount)
        
    def run(self):
        msql = md.datamodule()
        days = msql.gettradedays(start_date1=self.startdate, end_date1=self.enddate)
        for date in days:
           self.onbar(date)

    def onbar(self,pdate):
        fcff = fc.FCFF()
        dffc = fcff.monitor(pdate=pdate)
        if dffc.empty:
          return
        dfbuysell = self.logic(dffc)
        self.doit(pdf=dfbuysell,pdate=pdate)

    def logic(self,pdf):
        df =pdf.copy(deep=True)
        dfbs = pd.DataFrame()
        #dfbs = self._assetalloc(df)
        #self._stocktiming(df)
        dfbs = self._stockselection(df)
        return dfbs

    def doit(self,pdf,pdate):
        for i,row in pdf.iterrows():
            self.account.buysell(row,pdate)

    def _stockselection(self,df):
        jn = pd.DataFrame(journalaccount)
        df = df[df['grade'] > 0]
        df = df[df['grade'] < 3]
     
        jn['ts_code'] = df['ts_code']
        jn['price'] = df['close']
        jn['Shares'] = 100
        jn['buysell'] = 1

        return jn
    def _assetalloc(self,df):
        pass
    def _stocktiming(self,df):
        return df
    

class Account(object):
    def __init__(self,pwhichAccount):
        cfg = config.configs.Backtest
        self.account_csv = cfg.account_csv
        if pwhichAccount == 1:#如果是账户1，从csv读取
            self.journalaccount  = pd.read_csv(self.account_csv,index_col = 0)
            self.buypower = self.account.iloc[-1]['buypower']
        else:
            self.journalaccount = pd.DataFrame(journalaccount)
            self.buypower = 1e6

    def buysell(self,pbs,pdate):
        self.buypower = self.buypower - ( pbs['price'] * pbs['Shares'] * pbs['buysell'] )
        self.journalaccount = self.journalaccount.append({'ts_code':pbs['ts_code'],'date':pdate,'price':pbs['price'],'Shares':pbs['Shares'],'buysell':pbs['buysell'],'buypower':self.buypower}, ignore_index=True)

    def holding(self):
        df = self.journalaccount
        df['sh'] = df['Shares'] * df['buysell']
        grouped = df['sh'].groupby(df['ts_code'])
        grouped = grouped.sum()
        print(grouped)

        return grouped.sum()

    def netvalue(self,pdate):
        msql = md.datamodule()
        df_now = msql.pull_mysql(db = 'daily_basic',date = pdate)
        if df_now.empty:
            print('df empty!')
            return
        df_now.set_index(["ts_code"], inplace=True,drop = True) 
        jn = self.journalaccount.set_index(["ts_code"], inplace=False,drop = True)

        jn['close'] = df_now['close']
        return jn['close'] * jn['Shares'] * jn['buysell']



if __name__ == '__main__':
    backtest = Backtest(pstartdate='20190409',penddate='20190412')
    backtest.run()
    backtest.account.holding()