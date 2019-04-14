import pandas as pd
from module import module as md
import config
import FCFF as fc
class Backtest(object):
    def __init__(self,pstartdate,penddate):
        cfg = config.configs.Backtest
        self.account_csv = cfg.account_csv
        self.startdate   = pstartdate
        self.enddate     = penddate
        
    def run(self):
        msql = md.datamodule()
        days = msql.gettradedays(start_date1=self.startdate, end_date1=self.enddate)
        for date in days:
           self.onbar(date)

    def onbar(self,pdate):
        fcff = fc.FCFF()
        dffc = fcff.monitor(pdate=pdate)
        if dffc.empty:
          print('empty!')
          return
        dfbuysell = self.logic(dffc)
        self.doit(dfbuysell)

    def logic(self,pdf):
        print('logic')
        return pdf

    def doit(self,pdf):
        print(pdf)

if __name__ == '__main__':
    backtest = Backtest(pstartdate='20190410',penddate='20190412')
    backtest.run()