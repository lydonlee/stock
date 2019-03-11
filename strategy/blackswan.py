import pandas as pd
import ./module as md


class blackswan(object):
    def __init__(self):
        blackday = 30
        droprate = 0.4
        blackswan_list = []
        
    def findblackswan(self):
        

    def plotblackswan(self):
        return    

    def total_mv(self,start_day ='20180101',end_day = '20181231'):
        msql = md.datamodule()
        ts_code_df = msql.getts_code()

        df = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = '300552.SZ')
        for i, row in df:
            df30 = df[i:(i+self.blackday)]
            df30.orderBy('total_mv')
            if (df30[self.blackday-1]['total_mv']-df30[0]['total_mv']）/df30[self.blackday-1]['total_mv']）> droprate:
                 blackswan_list.append(df30[self.blackday-1])
        return 
