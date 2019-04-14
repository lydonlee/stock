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
        return pdf

    def doit(self,pdf,pdate):
        for i,row in pdf.iterrows():
            if row['buysell'] == 1: # 1:buy ,-1 sell
                self.account.buy(row['ts_code'],pdate,row['price'],row['quantity'])
            else:
                self.account.sell(row['ts_code'],pdate,row['price'],row['quantity'])

class Account(object):
    def __init__(self,pwhichAccount):
        cfg = config.configs.Backtest
        self.account_csv = cfg.account_csv
        if pwhichAccount == 1:#如果是账户1，从csv读取
            self.account = pd.read_csv(self.account_csv,index_col = 0)
            self.buypower = self.account.iloc[-1]['buypower']
        else:
            acct = {'ts_code':[],'date':[],'price':[],'quantity':[],'buysell':[],'buypower':[],}
            self.account = pd.DataFrame(acct)
            self.buypower = 1e6

    def buy(pcode,pdate,pprice,pquantity):
        self.buypower
        self.account = self.account.append({'ts_code':pcode,'date':pdate,'price':pprice,'quantity':pquantity}, ignore_index=True)

    def sell(pcode,pdate,pprice,pquantity):

if __name__ == '__main__':
    backtest = Backtest(pstartdate='20190410',penddate='20190412')
    backtest.run()