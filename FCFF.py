import pandas as pd
from module import module as md
import config
import datetime
import tushare as ts
import numpy as np
import matplotlib.pyplot as plt
import os
import math
import logging

SUSTAINABLE_GROWTH_RATE = 0.06

def _fun(x):
    if x >= 3:
        return 3
    elif x<=-3:
        return -3
    return x

class FCFF(object):
    def __init__(self):
        cfg = config.configs.FCFF
        self.template = cfg.FCFF_template
        self.FCFF_csv = cfg.FCFF_csv
        self.monitor_csv = cfg.monitor_FCFF_csv
        self.thisyear = int(datetime.datetime.now().strftime('%Y'))
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s  %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename= cfg.logfile_path,
                    filemode='w')

    def get_grade(self,pcode = None,pdate = None):
        try:
            dfm = pd.read_csv(self.monitor_csv,index_col = 0)
            if not dfm.empty:
                if pcode ==None:
                    return dfm
                else:
                    return dfm.loc[pcode]
        except:
            print('没找到:'+self.monitor_csv,'请重新调用monitor')
            return pd.DataFrame()

    #新建训练数据，X,Y，X为截止当前某股票的grade,Y为未来几天Y_day的股价相对财报日Y_fc_day的涨幅
    def monitor_for_train(self,pcode = None,Y_day,Y_fc_day):
        if Y_day <= Y_fc_day:
            return
        msql = md.datamodule()
        #latestday = msql.getlatestday('daily_basic')
        monitor_path = self._trainpath('monitor_'+pcode)
        evl_path = self._trainpath(pcode)

        Y_col = str(Y_day)+'_'+Y_fc_day
        #dfm = pd.DataFrame()
        
        dfm = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = '600519.SH')

        dfm.set_index(["close"], inplace=True,drop = True) 

        df_evl = pd.read_csv(evl_path,index_col = 0)
        #df_basic.set_index(["ts_code"], inplace=True,drop = True)
        #df_basic['lastupdate'] = latestday
        end_date1 = '19950101'
        end_date2 = '19950101'
        for i,row in df_evl:
            end_date1 = end_date2
            end_date2 = row['end_date']
            df = dfm.loc[(dfm['close']>end_date1) & (df['close']<end_date2)]
            df['evaluation'] = row['evaluation']
            dfm['evaluation'] = df['evaluation']

        dfm['市场低估比率'] = (dfm['evaluation'] - dfm['total_mv']*10000)/(dfm['total_mv']*10000)
        df = dfm['close']
        dfm[Y_col] = dfm[Y_day]
        dfm = msql.joinnames(df_basic)
        dfm = dfm.sort_values('市场低估比率',ascending = False) 

        dfm['grade'] = dfm['市场低估比率'].apply(lambda x: _fun(x))

        dfm.to_csv(monitor_path,encoding='utf_8_sig',index = True)

    #用每天的数据和季度估值做比较，每天调用    
    def monitor(self,pcode = None):
        msql = md.datamodule()
        latestday = msql.getlatestday('daily_basic')
        dfm = pd.DataFrame()
        if pcode != None :
            try:
                dfm = pd.read_csv(self.monitor_csv,index_col = 0)
                if not dfm.empty:
                    #已经是最新的数据了，直接返回需要的值
                    if dfm.iloc[0]['lastupdate'] == int(latestday):
                        return dfm.loc[pcode]
            except:
                print('没找到:'+self.monitor_csv,'重新建立')

        df_now = msql.pull_mysql(db = 'daily_basic',date = latestday)
        df_now.set_index(["ts_code"], inplace=True,drop = True) 

        df_basic = pd.read_csv(self.FCFF_csv,index_col = 0)
        df_basic.set_index(["ts_code"], inplace=True,drop = True)
        df_basic['lastupdate'] = latestday
        df_basic['total_mv'] = df_now['total_mv']*10000
        df_basic['市场低估比率'] = (df_basic['evaluation'] - df_now['total_mv']*10000)/(df_now['total_mv']*10000)
        
        df_basic = msql.joinnames(df_basic)
        df_basic = df_basic.sort_values('市场低估比率',ascending = False) 

 
        df_basic['grade'] = df_basic['市场低估比率'].apply(lambda x: _fun(x))

        df_basic.to_csv(self.monitor_csv,encoding='utf_8_sig',index = True)
        if pcode != None:
            return df_basic.loc[pcode]
    
    #根据季度报估值，每季度调用一次  
    def build_for_monitor(self):
        df_rslt = pd.DataFrame()
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        end_date = msql.getlatestday()
        if end_date[4:] <='0630':
            end_date = end_date[:4]+'0630'
        else:
            end_date = end_date[:4]+'1231' 
        for i,code in ts_code_df.iterrows():
            df1 = self._buildone(df = df_template,code = code['ts_code'],pdate = end_date)
            if df1.empty:
                continue
            dic = {}

            dic['ts_code'] = [code['ts_code']]
            dic['evaluation'] = [df1['y-6'][37]]
            dic['sus_grow_rate'] = [df1['y-6'][22]]
            dic['y1_grow_rate'] = [df1['y1'][22]]
            dic['y2_grow_rate'] = [df1['y2'][22]]
            dic['y3_grow_rate'] = [df1['y3'][22]]
            dic['y4_grow_rate'] = [df1['y4'][22]]
            dic['y5_grow_rate'] = [df1['y5'][22]]

            dic['y0_grow_rate'] = [df1['y0'][14]]
            dic['y-1_grow_rate'] = [df1['y-1'][14]]
            dic['y-2_grow_rate'] = [df1['y-2'][14]]
    
            df1 = pd.DataFrame.from_dict(dic)
            df_rslt = pd.concat([df_rslt,df1.loc[0:0,]],ignore_index = True)

        df_rslt.to_csv(self.FCFF_csv,encoding='utf_8_sig',index = False) 

    def build_for_train(self):
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        for i,code in ts_code_df.iterrows():
            df_rslt = pd.DataFrame()
            
            for year in range(1995,self.thisyear,1):
                for month in (0,1):
                    if month ==0:
                        date = str(year)+'0630'
                    elif month ==1:
                        date = str(year)+'1231'
                    df1 = pd.DataFrame()
                    df1 = self._buildone(df = df_template,code = code['ts_code'],pdate = date)
        
                    if df1.empty:
                        continue
                    dic = {}
    
                    dic['ts_code'] = [code['ts_code']]
                    dic['evaluation'] = [df1['y-6'][37]]
                    dic['end_date'] = [date]
                    dic['sus_grow_rate'] = [df1['y-6'][22]]
                    dic['y1_grow_rate'] = [df1['y1'][22]]
                    dic['y2_grow_rate'] = [df1['y2'][22]]
                    dic['y3_grow_rate'] = [df1['y3'][22]]
                    dic['y4_grow_rate'] = [df1['y4'][22]]
                    dic['y5_grow_rate'] = [df1['y5'][22]]

                    dic['y0_grow_rate'] = [df1['y0'][14]]
                    dic['y-1_grow_rate'] = [df1['y-1'][14]]
                    dic['y-2_grow_rate'] = [df1['y-2'][14]]
            
                    df1 = pd.DataFrame.from_dict(dic)
                    df_rslt = pd.concat([df_rslt,df1.loc[0:0,]],ignore_index = True)

            filepath=self._trainpath(code['ts_code'])
            df_rslt.to_csv(filepath,encoding='utf_8_sig',index = False)

    def _buildone(self,df,code,pdate):
        df = self._getdata(ts_code = code,ptemplate = df,pdate = pdate)
        if df.empty:
            return df
        df = self._processdata(pdf=df)
        return df
        #df.to_csv(self.FCFF_csv,encoding='utf_8_sig')
    #获取利润等数据，存入以下行：8:12，22，32，33，34
    def _getdata(self,ts_code,ptemplate,pdate):
        template = ptemplate
        msql = md.datamodule()
        df = msql.pull_mysql(db = 'income',ts_code = ts_code)
        df_cash = msql.pull_mysql(db = 'cashflow',ts_code = ts_code)
        df_blc = msql.pull_mysql(db = 'balancesheet',ts_code = ts_code)

        if df.empty or df_cash.empty or df_blc.empty :
            return pd.DataFrame()
        #如果输入的pdate是整年1231，则获取过去7年的年报，如果pdate是‘ 0930，0630，0331’等季度，则滚动前四季度记为一年
        endyear = int(pdate[0:4])
        for i in range(endyear,endyear- 7,-1):
            #滚动四季度等于今年前3季度+去年第四季度q3+(lq12-lq3)，对其他季度同样适用这个公式
            q3 = str(i)+pdate[4:] #'20181231' or 20180930，20180630，20180331 ,
            lq12 = str(i-1)+'1231' #20171231
            lq3 = str(i-1)+pdate[4:]#20171231 or 20170931 2017630 20170331
            #end_date1 = str(i+1)+'1231'
            y = 'y'+str(i - endyear)
            #8 :净利润：income：n_income
            dfq3   = df[df['end_date'] == q3]
            dflq12 = df[df['end_date'] == lq12]
            dflq3  = df[df['end_date'] == lq3]
   
            if not dfq3.empty and not dflq12.empty and not dflq3.empty:
                try:
                    template.loc[8,y] = dfq3.iloc[0]['n_income'] + dflq12.iloc[0]['n_income'] - dflq3.iloc[0]['n_income']
                except:
                    logging.debug('净利润数据缺失')
                    continue
            else:
                logging.debug('净利润数据为空')
                continue
               
            #9 +折旧和摊销cashflow，depr_fa_coga_dpba + amort_intang_assets + lt_amort_deferred_exp
            #df1 = df_cash[df_cash['end_date'] == end_date]
            dfq3   = df_cash[df_cash['end_date'] == q3]
            dflq12 = df_cash[df_cash['end_date'] == lq12]
            dflq3  = df_cash[df_cash['end_date'] == lq3]
            
            if not dfq3.empty and not dflq12.empty and not dflq3.empty:
                #可能会因为数据缺失导致异常
                try:
               
                    depr_fa_coga_dpba     =   dfq3.iloc[0]['depr_fa_coga_dpba']+dflq12.iloc[0]['depr_fa_coga_dpba']-dflq3.iloc[0]['depr_fa_coga_dpba']
                    amort_intang_assets   =   dfq3.iloc[0]['amort_intang_assets']+dflq12.iloc[0]['amort_intang_assets']-dflq3.iloc[0]['amort_intang_assets']
                    lt_amort_deferred_exp =   dfq3.iloc[0]['lt_amort_deferred_exp']+dflq12.iloc[0]['lt_amort_deferred_exp']-dflq3.iloc[0]['lt_amort_deferred_exp']
                    template.loc[9,y] = depr_fa_coga_dpba + amort_intang_assets + lt_amort_deferred_exp
        
                    #10 +支付的利息 cashflow:c_pay_dist_dpcp_int_exp
                    c_pay_dist_dpcp_int_exp = dfq3.iloc[0]['c_pay_dist_dpcp_int_exp']+dflq12.iloc[0]['c_pay_dist_dpcp_int_exp']-dflq3.iloc[0]['c_pay_dist_dpcp_int_exp']
                    template.loc[10,y] = c_pay_dist_dpcp_int_exp
 
                    #11 -资本性支出：cashflow：c_pay_acq_const_fiolta
                    c_pay_acq_const_fiolta = dfq3.iloc[0]['c_pay_acq_const_fiolta']+dflq12.iloc[0]['c_pay_acq_const_fiolta']-dflq3.iloc[0]['c_pay_acq_const_fiolta']
                    template.loc[11,y] = c_pay_acq_const_fiolta*(-1)
      
                    #12 营运资金变动：balancesheet：y0（total_cur_assets - total_cur_liab）- y1(total_cur_assets - total_cur_liab)
                    q31 = str(i+1)+pdate[4:] #'20181231' or 20180930，20180630，20180331 ,
                    lq121 = str(i-1+1)+'1231' #2011231
                    lq31 = str(i-1+1)+pdate[4:]#20171231 or 20170931 2017630 20170331

                    dfq3   = df_blc[df_blc['end_date'] == q3]
                    dflq12 = df_blc[df_blc['end_date'] == lq12]
                    dflq3  = df_blc[df_blc['end_date'] == lq3]

                    dfq31   = df_blc[df_blc['end_date'] == q31]
                    dflq121 = df_blc[df_blc['end_date'] == lq121]
                    dflq31  = df_blc[df_blc['end_date'] == lq31]
         
                    if not dfq3.empty and not dflq12.empty and not dflq3.empty:
                        if not dfq31.empty and not dflq121.empty and not dflq31.empty:
                           
                            total_cur_assets1 = dfq3.iloc[0]['total_cur_assets']+dflq12.iloc[0]['total_cur_assets']-dflq3.iloc[0]['total_cur_assets']
                            total_cur_assets2 = dfq31.iloc[0]['total_cur_assets']+dflq121.iloc[0]['total_cur_assets']-dflq31.iloc[0]['total_cur_assets']
                            total_cur_liab1 = dfq3.iloc[0]['total_cur_liab']+dflq12.iloc[0]['total_cur_liab']-dflq3.iloc[0]['total_cur_liab']
                            total_cur_liab2 = dfq31.iloc[0]['total_cur_liab']+dflq121.iloc[0]['total_cur_liab']-dflq31.iloc[0]['total_cur_liab']
                            template.loc[12,y] = total_cur_assets1 - total_cur_liab1 -(total_cur_assets2 - total_cur_liab2)
                except:
                    logging.debug('dflq3数据为空')
                    continue

        #22永续增长率设为gdp
        template.loc[22,'y-6'] = SUSTAINABLE_GROWTH_RATE
        #32，33，34带息负债，非经营负债，少数股东权益设为0
        template.loc[32,'y-6'] = 0
        template.loc[33,'y-6'] = 0
        template.loc[34,'y-6'] = 0
        
        return template

    def _processdata(self,pdf):
        df = pdf
        #处理第13行前半部分y-7:y0
        df1 = df.loc[8:12,'y-7':'y0'] 
        df.loc[13,'y-7':'y0'] = df1.apply('sum',axis=0)

        #处理第14行
        for i in range(-6,1):
            y0 = 'y'+str(i-1)
            y1 = 'y'+str(i)
            df.loc[14,y1] = (df.loc[8][y1]-df.loc[8][y0])/math.fabs(df.loc[8][y0])
        
        
        #处理第22行，用过去三年平均增长率预测未来增长率,如果增长率为负数，则取0，默认未来不会恶化
        for i in range(1,6):
            y = 'y'+str(i)
            s1 = df.loc[14:14,'y-2':'y0'].apply('mean',axis=1)
            if s1.iloc[0] < 0 :
                df.loc[22,y] = 0 
            else:
                df.loc[22,y] = s1.iloc[0]
 
        #处理第13行后半部分：y1-y5
 
        for i in range(0,5):
            y_1 = 'y' +str(i-1)
            y0 = 'y'+str(i)
            y1 = 'y'+str(i+1)
            s = df.loc[13][y0]
            #数据质量比较差，如果没有上一年数据，则向前找一年数据
            if s == 0 :
                s = df.loc[13][y_1]
            df.loc[13,y1] = s*(1+df.loc[22][y1])

        #处理第19行
        for i in range(-7,6):
            y = 'y'+str(i)
            df.loc[19,y] = df.loc[13][y]*df.loc[18][y]

        #处理第24行
        t = df.loc[19:19,'y-6':'y0'].apply('sum',axis=1)
        df.loc[24,'y-6'] = t.iloc[0]

        #处理第25行
        t = df.loc[19:19,'y1':'y5'].apply('sum',axis=1)
        df.loc[25,'y-6'] = t.iloc[0]

        #处理第26行
        df.loc[26,'y-6'] = df['y5'][13] * (1+df['y-6'][22]) / ( df['y-6'][16 ]- df['y-6'][22] ) * df['y5'][18]
   
        #处理第27行
        df.loc[27,'y-6'] =  df['y-6'][24] + df['y-6'][25] + df['y-6'][26] 

        #处理第35行
        df.loc[35,'y-6'] =  df['y-6'][27] + df['y-6'][32] + df['y-6'][33] #+ df['y-6'][34] 

        #处理第36行
        df.loc[36,'y-6'] = 0

        #处理第37行
        df.loc[37,'y-6'] = df.loc[35]['y-6'] + df.loc[36]['y-6'] 
        return df

    def plot(self):
        dfm = pd.read_csv(self.monitor_csv,index_col = None)
        df = dfm.loc[300:3000,'市场高估比率']
        print(df)
        #df = dfm.loc['300287.SZ':'603881.SH','市场高估比率']
        df.plot.kde()
        #plt.savefig('D:\test.png')
        #df.plot(x='x',y='市场高估比率',kind = 'density')#'scatter')
     
        plt.show()

    def _trainpath(self,file):
        fpath = file+'.csv'
        return os.path.join(config.MODULE_PATH['train_FCFF'], fpath)

    def _testbuild(self):
        '000010.SZ'
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        df = self._buildone(df =df_template, code = '600519.SH',pdate = '20181231')
        file = self._trainpath('test.csv')
        df.to_csv(file,encoding='utf_8_sig',index = True)
if __name__ == '__main__':
    f = FCFF()
    #f._testbuild()
    f.build()
    #msql._createdb('test1')
    #f.plot()
    #d = f.monitor('000002.SZ')
    #print(d)
    
    

