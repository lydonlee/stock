import tushare as ts
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
class storedata(object):
    def __init__(self):
        ts.set_token('1360474e70eee70c9c1b9740d684a8800e89903641c6ffb82ac549da')
        self.pro = ts.pro_api()
        self.mysqlrecord = '/Users/liligong/stock/mysqlrecord.csv'

    def tradedays(self,start_date1='20180101', end_date1='20181231'):

        df = self.pro.trade_cal(exchange='', start_date=start_date1 , end_date=end_date1)
        tradedays_list = []
        for i in range(0, len(df)):
            if df.iloc[i]['is_open']==1:
                tradedays_list.append(df.iloc[i]['cal_date'])
        return tradedays_list

    def rqye(self,start = '20180802',end = '20180902',code='000036.SZ'):
        tradedays_list = self.tradedays(start_date1 = start,end_date1 = end)
        rqye_list = []
        close_list = []
        for day in tradedays_list:
            df = self.pro.margin_detail(trade_date=day,ts_code = code)
            rqye_list.append(df.iloc[0]['rqye'])

            df = self.pro.daily_basic(trade_date=day,ts_code = code)
            close_list.append(df.iloc[0]['close'])

        return rqye_list,close_list,tradedays_list

    def datanotinmysql(self,db = 'margin_detail',wstart = '20180101',wend='20180201'):
        start = wstart
        end = wend

        df = pd.read_csv(self.mysqlrecord)
        for row in df.iterrows():
            if (row['db'] == db) :
                if start > row['start'] and start < row['end'] and end < row['end']:
                    start = '0'
                    end = '0'
                    return start,end
                elif start > row['start'] and start < row['end'] and end > row['end']:
                    start = row['end']
                    continue
                elif start < row['start'] and end > row['start'] and end < row['end']:
                    end = row['start']

        return start , end

    def savemysqlrecorde(self,db = 'margin_detail',wstart = '20180101',wend='20180201'):
        df = pd.DataFrame([[db,wstart,wend]])
        df.to_csv(self.mysqlrecord,mode='a',header = False)

    def test(self):
        f = df.iloc[0:, [1, 2]]
        d =df.values
        b = d[0:, 1]
        x = list(map(int, strlist))

if __name__ == '__main__':
    sd = storedata()
    s = '20180101'
    e = '20180301'

    rqye_list,close_list,td_list = sd.rqye(start = s,end = e)
    'tradedays_list = list(map(int,td_list))'

    day= np.arange(0,len(td_list))
    x = np.array(rqye_list,dtype=np.float)
    print(x/100000*5)
    print(close_list)
    plt.subplot(211)
    'plt.axis([0,70,5,40])'
    plt.plot(day, x/100000*5, day, close_list)
    plt.xlabel('start:'+s+'end:'+e)
    plt.ylabel('rqye and close')
    plt.grid(axis="y")

    print(np.corrcoef(x/100000*5, close_list))