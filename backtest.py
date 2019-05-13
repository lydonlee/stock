import pandas as pd
from multiprocessing import Pool
import module as md
import config
import FCFF as fc
import datetime
import matplotlib.pyplot as plt

journalaccount = {'ts_code':[],'date':[],'price':[],'Shares':[],'buysell':[],'buypower':[],}
class runtimes(object):
    def __init__(self, max=1):
        self.max = max
        self.count = 0
        
    def __call__(self, func): # 接受函数
        def wrapper(*args, **kwargs):
            if self.count < self.max:
                self.count = self.count + 1
                return func(*args, **kwargs)
               
            else:
                return pd.DataFrame()
        return wrapper  #返回函数
def findbest():
     pool = Pool(processes=5) 
     pool.map(_findbest_worker, range(1,10))
 
def _findbest_worker(j = 1):
    df = pd.DataFrame()
    maxn = 0
    maxi = 0
    i   = 0
    startdate = '20150{}05'
    enddate = '20181201'
    
    startdate = startdate.format(j)
    while( i<= 5):
        i = i + 0.01
        backtest = Backtest(pmanager = i)
        backtest.run(pstartdate=startdate,penddate=enddate,pstep=300)
        t = pd.DataFrame()
        if backtest.hisnetvalue.empty:
            return
        t['netvalue'] = backtest.hisnetvalue['netvalue']
        mid = t.apply('max',axis=0)
        dic = {'i':i,'max':mid}
        df = df.append(dic,ignore_index=True)
        if maxn < mid['netvalue']:
            maxn = mid['netvalue']
            maxi = i
       
        print(backtest.hisnetvalue)
        print(backtest.account.journalaccount)

    filepath = config.detaildir('FCFF_PNG',str(j)+'_'+str(maxi)+'.png')
    
    plt.plot(df['i'],df['max'],'r')
    plt.savefig(filepath)
    #plt.show()

class Backtest(object):
    def __init__(self,pwhichAccount = 0,pmanager = 0):
        self.account     = Account(pwhichAccount = pwhichAccount)
        self.manager     = Manager(par = pmanager,paccount = self.account)
        self.analyst     = Analyst()
        self.trader      = Trader(self.account)
        self.hisnetvalue = pd.DataFrame()

    def run(self,pstartdate,penddate,pstep = 1):
        msql = md.datamodule()
        days = msql.gettradedays(start_date1=pstartdate, end_date1=penddate)
        lenth = len(days)
        for i in range(0,lenth,pstep):
            self._onbar(days[i])
            if i+ pstep >= lenth:
                self._onbar(days[lenth-1])#确保最后一天能执行
                
        #print(self.account.journalaccount)
        #self.plot()
  
    def _onbar(self,pdate):
        fcff = self.analyst.work(pdate = pdate)
        dfbuysell = self.manager.work(fcff)
        self.trader.work(pdf=dfbuysell,pdate=pdate,pfcff=fcff)
        net = self.account.netvalue(pdate = pdate)
        dic = {'netvalue':net,'date':pdate}
        self.hisnetvalue = self.hisnetvalue.append(dic,ignore_index=True)
        
    def plot(self):
        msql = md.datamodule()
        sh = msql.gettable(pdb = 'index_daily',ptable = 'index_daily')
        df = self.hisnetvalue.copy(deep=True)

        sh.set_index(["trade_date"], inplace=True,drop = False)
        df.set_index(["date"], inplace=True,drop = False)

        df['000001.SH'] = sh['close']
        s = df.iloc[0]['000001.SH']
        df['000001.SH'] = (df['000001.SH']-s)/s *100
        df['netvalue']  = (df['netvalue']-1e6)/1e6 *100
    
        df['date'] = df['date'].apply(lambda x:datetime.datetime.strptime(str(x), "%Y%m%d"))
        plt.plot_date(df['date'],df['netvalue'],linestyle="-",label="my")
        plt.plot_date(df['date'],df['000001.SH'],linestyle="-",label="000001.SH")
        print(self.account.commi)
        plt.show()

class Analyst(object):
    def __init__(self):
        self.count = 0
    def work(self,pdate):
        return self.buyandhold(pdate=pdate)
    def buyandhold(self,pdate):
        if self.count == 0:
            fcff = fc.FCFF()
            print("buyandhold",pdate)
            dffc = fcff.monitor(pdate=pdate)
            if dffc.empty:
                print('fcff.monitor empty:',pdate)
            self.count = 1
            return dffc
        return pd.DataFrame()

#1,买入了错误的股票，买入后股价一直下跌
#2，买入后持有时间不够长，错误时间卖出了
#3，频繁买卖，佣金损失大
class Manager(object):
    def __init__(self,par = 0,paccount = None):
        self.par = par
        self.account = paccount
    def work(self,pdf):
        dfbs = pd.DataFrame()
        if pdf.empty:
            return dfbs
        df =pdf.copy(deep=True)
        #dfbs = self._assetalloc(df)
        #self._stocktiming(df)
 
        dfbs = self._stockselection(df)
        return dfbs

    def _stockselection(self,df):
        jn = pd.DataFrame()
        if self.account == None:
            bp = 1e5
        else:
            bp = self.account.buypower
            
        #df = df[df['grade'] > 0]
        df = df[df['grade'] < self.par]
        df = df[:10]
        jn['ts_code'] = df['ts_code']
        jn['price'] = df['close']

        jn['Shares'] = bp/100//jn['price']*100
        jn['buysell'] = 1
        return jn

    def _assetalloc(self,df):
        pass
    def _stocktiming(self,df):
        return df

class Trader(object):
    def __init__(self,paccount):
        self.account = paccount
    def work(self,pdf,pdate,pfcff):
        df = pdf.copy(deep=True)
        hd = self.account.holding()
        if not hd.empty:
            for i,row in hd.iterrows():#处理卖出
                ts = row['ts_code']
                if df.empty:
                    hd1 = pd.DataFrame()
                else:
                    hd1 = df[df['ts_code']==ts]
                if hd1.empty:#如果目标清单里没有该股票则卖出
                    row['buysell'] = -1
                    row['price'] = pfcff.loc[ts]['close']
                    self.account.buysell(row,pdate)

        hd = self.account.holding()
        for i,row in df.iterrows():
            if not hd.empty:
                ts = row['ts_code']
                hd1 = hd[hd['ts_code']==ts]
                if not hd1.empty: 
                    #if hd1.loc[ts]['Shares'] >= row['Shares']:
                    continue
            self.account.buysell(row,pdate)

class Account(object):
    def __init__(self,pwhichAccount):
        cfg = config.configs.Backtest
        self.account_csv = cfg.account_csv
        self.commission_rate = 0.005
        if pwhichAccount == 1:#如果是账户1，从csv读取
            self.journalaccount  = ut.read_csv(self.account_csv,index_col = 0)
            self.buypower = self.account.iloc[-1]['buypower']
        else:
            self.journalaccount = pd.DataFrame(journalaccount)
            self.buypower = 1e5
            self.commi = 0

    def buysell(self,pbs,pdate):
        bp = self.buypower - ( pbs['price'] * pbs['Shares'] * pbs['buysell'] )
        if bp < 0:
            print('没钱继续买入啦！')
            return
        comm = self.commission(pbs=pbs)
        buypower = bp - comm
        if buypower < 0:
            print('不够支付佣金！')
            return
        self.commi = self.commi + comm
        self.buypower = buypower
        dic = {'ts_code':pbs['ts_code'],'date':pdate,'price':pbs['price'],'Shares':pbs['Shares'],'buysell':pbs['buysell'],'buypower':self.buypower}
        self.journalaccount = self.journalaccount.append(dic, ignore_index=True)
        
    def commission(self,pbs):
        rslt = 0
        amt = pbs['price'] * pbs['Shares']
        rslt =rslt + amt*3/1000#买入佣金 千3
        rslt = rslt + pbs['Shares']/1000 #上海市场过户费，1000股1元

        if pbs['buysell'] == -1:
            rslt = rslt + amt/1000 #卖出印花税千一
        return rslt

    def holding(self):
        df = self.journalaccount.copy(deep = True)
        df['hold'] = df['Shares'] * df['buysell']
        grouped = df['hold'].groupby(df['ts_code'])
        jn = grouped.sum()
        dict_jn = {'ts_code':jn.index,'Shares':jn.values}
        dfjn = pd.DataFrame(dict_jn)
        dfjn.set_index(["ts_code"], inplace=True,drop = False)
        dfjn = dfjn[dfjn['Shares']>0]
        return dfjn

    def netvalue(self,pdate=None):
        if self.journalaccount.empty:
            return self.buypower
            
        if pdate < self.journalaccount.iloc[-1]['date']:
            print('错误日期')
            return None
        msql = md.datamodule()
        df_now = msql.pull_mysql(db = 'daily_basic',date = pdate)
        if df_now.empty:
            print('df empty!')
            return None
        df_now.set_index(["ts_code"], inplace=True,drop = True) 
        dfjn = self.holding()
        dfjn.set_index(["ts_code"], inplace=True,drop = True)
        
        dfjn['close'] = df_now['close']
        dfjn['netvalue'] = dfjn['close'] * dfjn['Shares'] 
        return dfjn['netvalue'].sum() + self.journalaccount.iloc[-1]['buypower']

if __name__ == '__main__':
    #backtest = Backtest(pstartdate='20170109',penddate='20181201')
    #backtest.run()
    findbest()
