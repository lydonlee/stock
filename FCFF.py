import pandas as pd
from module import module as md
import config
import datetime
import tushare as ts
#使用dividend数据库
class FCFF(object):
    def __init__(self):
        cfg = config.configs.FCFF
        self.template = cfg.FCFF_template
        self.FCFF_csv = cfg.FCFF_csv
        self.thisyear = int(datetime.datetime.now().strftime('%Y'))
        ts.set_token('1360474e70eee70c9c1b9740d684a8800e89903641c6ffb82ac549da')
        self.pro = ts.pro_api()
    def build(self):
        df = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        df = self._getdata(ts_code = '600519.SH',template = df)
        df = self._processdata(df=df)
        df.to_csv(self.FCFF_csv)
    #获取利润等数据，存入以下行：8:12，22，32，33，34
    def _getdata(self,ts_code,template):
        
        #msql = md.datamodule()
        #df = msql.pull_mysql(db = 'income',ts_code = ts_code)
        df = self.pro.income(ts_code=ts_code)
        df_cash = self.pro.cashflow(ts_code=ts_code)
        df_blc = self.pro.balancesheet(ts_code=ts_code)

        for i in range(self.thisyear - 7,self.thisyear):
            end_date = str(i)+'1231'
            end_date1 = str(i+1)+'1231'
            y = 'y'+str(i - self.thisyear)

            #8 :净利润：income：n_income
            df1 = df[df['end_date'] == end_date]
            if not df1.empty:
                template.at[8,y] = df1.iloc[0]['n_income']
            
            #9 +折旧和摊销cashflow，depr_fa_coga_dpba + amort_intang_assets + lt_amort_deferred_exp
            df1 = df_cash[df_cash['end_date'] == end_date]
            if not df1.empty:
                template.at[9,y] = df1.iloc[0]['depr_fa_coga_dpba'] + df1.iloc[0]['amort_intang_assets'] + df1.iloc[0]['lt_amort_deferred_exp']
            #10 +支付的利息 cashflow:c_pay_dist_dpcp_int_exp
                template.at[10,y] = df1.iloc[0]['c_pay_dist_dpcp_int_exp']
            #11 -资本性支出：cashflow：c_pay_acq_const_fiolta
                template.at[11,y] = df1.iloc[0]['c_pay_acq_const_fiolta']*(-1)

            #12 营运资金变动：balancesheet：y0（total_cur_assets - total_cur_liab）- y1(total_cur_assets - total_cur_liab)
                df1 = df_blc[df_blc['end_date'] == end_date]
                df2 = df_blc[df_blc['end_date'] == end_date1]
                if (not df1.empty) and (not df2.empty) :
                    template.at[12,y] = df1.iloc[0]['total_cur_assets'] - df1.iloc[0]['total_cur_liab'] -(df2.iloc[0]['total_cur_assets'] - df2.iloc[0]['total_cur_liab'])
        #22增长率待定
        #32，33，34带息负债，非经营负债，少数股东权益设为0
        #template.at[32,'y-6'] = 0
        #template.at[33,'y-6'] = 0
        #template.at[34,'y-6'] = 0

        return template


    def _processdata(self,df):
        #处理第13行
        df1 = df.loc[8:12,'y-7':'y0'] 
        df.at[13,'y-7':'y0'] = df1.apply('sum',axis=0)
        for i in range(5):
            y0 = 'y'+str(i)
            y1 = 'y'+str(i+1)
            df.at[13,y1] = df.loc[13][y0]*(1+df.loc[22][y1])

        #处理第14行

        df.at[14,'y-3'] = df.loc[8]['y-3']/df.loc[8]['y-4']-1
        df.at[14,'y-2'] = df.loc[8]['y-2']/df.loc[8]['y-3']-1
        df.at[14,'y-1'] = df.loc[8]['y-1']/df.loc[8]['y-2']-1
    
        #处理第19行
        for i in range(-7,6):
            y = 'y'+str(i)
            df.at[19,y] = df.loc[13][y]*df.loc[18][y]
        #处理第24行
        t = df.loc[19:19,'y-4':'y0'].apply('sum',axis=1)
        df.at[24,'y-6'] = t.iloc[0]

        #处理第25行
        t = df.loc[19:19,'y1':'y5'].apply('sum',axis=1)
        df.at[25,'y-6'] = t.iloc[0]

        #处理第26行
        df.at[26,'y-6'] = df['y5'][13] * (1+df['y-6'][22]) / ( df['y-6'][16 ]- df['y-6'][22] ) * df['y5'][18]
   
        #处理第27行
        df.at[27,'y-6'] =  df['y-6'][24] + df['y-6'][25] + df['y-6'][26] 

        #处理第35行
        df.at[35,'y-6'] =  df['y-6'][27] + df['y-6'][32] + df['y-6'][33] #+ df['y-6'][34] 

        #处理第36行
        df.at[36,'y-6'] = 0

        #处理第37行
        df.at[37,'y-6'] = df.at[35,'y-6'] + df.at[36,'y-6'] 

        return df


if __name__ == '__main__':
    f = FCFF()
    f.build()
    

