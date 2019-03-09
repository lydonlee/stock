import tushare as ts
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np

class storedata(object):
    def __init__(self):
        ts.set_token('1360474e70eee70c9c1b9740d684a8800e89903641c6ffb82ac549da')
        self.pro = ts.pro_api()

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


    def test(self):
        f = df.iloc[0:, [1, 2]]
        d =df.values
        b = d[0:, 1]
        x = list(map(int, strlist))

if __name__ == '__main__':
    sd = storedata()
    rqye_list,close_list,td_list = sd.rqye(start = '20180101',end = '20180301')
    tradedays_list = list(map(int,td_list))
    x = np.array(rqye_list,dtype=np.float)
    print(x/100000*5)
    print(close_list)
    plt.plot(tradedays_list, x/100000*5, tradedays_list, close_list)

    plt.xlabel('time')
    plt.ylabel('rqye and close')
    plt.grid(True)

    plt.show()