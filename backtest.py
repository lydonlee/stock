import pandas as pd
from module import module as md
import config
import FCFF as fc
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
        self._assetalloc(df)
        self._stocktiming(df)
        self._stockselection(df)
        return 

    def doit(self,pdf,pdate):
        for i,row in pdf.iterrows():
            self.account.buysell(row,pdate)

    def _stockselection(self,df):

class Account(object):
    def __init__(self,pwhichAccount):
        cfg = config.configs.Backtest
        self.account_csv = cfg.account_csv
        if pwhichAccount == 1:#如果是账户1，从csv读取
            self.journalaccount  = pd.read_csv(self.account_csv,index_col = 0)
            self.buypower = self.account.iloc[-1]['buypower']
        else:
            acct = {'ts_code':[],'date':[],'price':[],'Shares':[],'buysell':[],'buypower':[],}
            self.journalaccount = pd.DataFrame(acct)
            self.buypower = 1e6

    def buysell(self,pbs,pdate):
        self.buypower = self.buypower - ( pbs['price'] * pbs['Shares'] * pbs['pbuysell'] )
        self.journalaccount = self.journalaccount.append({'ts_code':pbs['ts_code'],'date':pdate,'price':pbs['price'],'Shares':pbs['Shares'],'buysell':pbs['buysell'],'buypower':self.buypower}, ignore_index=True)

if __name__ == '__main__':
    backtest = Backtest(pstartdate='20190410',penddate='20190412')
    backtest.run()