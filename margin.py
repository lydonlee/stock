import tushare as ts
from sqlalchemy import create_engine
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

if __name__ == '__main__':
    sd = storedata()
    tradedays_list = sd.tradedays(start_date1 = '20180101',end_date1 = '20180201')
    print (tradedays_list)