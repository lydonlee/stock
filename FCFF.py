from multiprocessing import Pool
from multiprocessing import Lock
import threading
import pandas as pd
from pandas.plotting import register_matplotlib_converters
import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
import math
import logging
from sklearn import linear_model
from scipy import stats
import tushare as ts
import config
import module  as md
import util as ut
import pytorch

register_matplotlib_converters()
# monitor ：用每天的数据和季度估值做比较，输出所有股票的当前估值 
#train_FCFF_monitor：输出某只股票的所有历史估值
#detail_template
SUSTAINABLE_GROWTH_RATE = 0.06
    
def _fun(x):
    if x >= 5:
        return 5
    elif x<=-5:
        return -5
    return x

def func1(x):
    try:
        return float(x)
    except:
        return None
def all_build_for_monitor():
    pool = Pool(processes=5) 
    pool.map(_build_for_monitor_woker, range(1995,2019,1))

def _build_for_monitor_woker(day):
    day1 = str(day)+'0630'
    _build_for_monitor(pdate = day1)

    day2 = str(day)+'1231'
    _build_for_monitor(pdate = day2)

def _build_for_monitor(pdate = None):
    fcff = FCFF()
    ut.thread_loop(by = 'ts_code',pFunc = fcff._build_for_monitor_one,p1=pdate)

    filepath=fcff._get_path_build_for_monitor(pdate=pdate)
    fcff.df_rslt.to_csv(filepath,encoding='utf_8_sig',index = False) 
def plotsave():
    ut.process_loop(by ='ts_code',pclass=FCFF,pFunc = 'plotsave_one')


#新建训练数据，X,Y，X为截止当前某股票的grade,Y为股价相对最近一次财报日的涨幅
#后续要考虑财报公布日期的因素
def monitor_xueqiu():
    fcff = FCFF()
    ut.thread_loop(by = 'ts_code',pFunc = fcff.monitor_one_xueqiu)
    filepath = fcff._trainpath('xueqiu')
    fcff.df_rslt.to_csv(filepath,encoding='utf_8_sig',index = True)
def monitor_xueqiu1():
    reids_key = 'FCFF'+'monitor_xueqiu1'
    mutex=Lock()
    if md.redisConn.exists(reids_key):
         md.redisConn.delete(reids_key)
    ut.process_loop(by = 'ts_code',pclass=FCFF,pFunc = 'monitor_one_xueqiu',p1=mutex,p2=reids_key,p3='20181231')
    print('monitor_xueqiu1 finish!')
    fcff = FCFF()
    filepath = fcff._trainpath('xueqiu')
    if md.redisConn.exists(reids_key):
        df = pd.read_msgpack(md.redisConn.get(reids_key))
        df.to_csv(filepath,encoding='utf_8_sig',index = False)

def train_FCFF1():
    reids_key = 'FCFF'+'train_FCFF1'
    mutex=Lock()
    if md.redisConn.exists(reids_key):
         md.redisConn.delete(reids_key)
    ut.process_loop(by = 'ts_code',pclass=FCFF,pFunc = '_build_one_train',p1=mutex,p2=reids_key)
    print('train_FCFF1 finish!')

def train_FCFF_monitor():
    fcff = FCFF()
    ut.thread_loop(by = 'ts_code',pFunc = fcff._monitor_one_train)
def train_FCFF():
    fcff = FCFF()
    ut.thread_loop(by = 'ts_code',pFunc = fcff._build_one_train)

class FCFF(object):
    def __init__(self):
        cfg = config.configs.FCFF
        self.template = cfg.FCFF_template
        #self.FCFF_csv = cfg.FCFF_csv
        self.monitor_csv = cfg.monitor_FCFF_csv
        self.thisyear = int(datetime.datetime.now().strftime('%Y'))
        self.df_rslt  = pd.DataFrame()
        self.sem = threading.Semaphore(1)
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

    #新建训练数据，X,Y，X为截止当前某股票的grade,Y为未来几天Y_day的股价相对上一次财报日的涨幅
    #后续要考虑财报公布日期的因素
    def _monitor_one_train(self,pcode = None):
        msql = md.datamodule()
        #latestday = msql.getlatestday('daily_basic')
        monitor_path = self._trainpath('monitor_'+pcode)
        evl_path = self._trainpath(pcode)
        X_col = 'X'
        Y_col = 'Y'
        #dfm = pd.DataFrame()
        try:
            df_evl = pd.read_csv(evl_path)
        except:
            print('没有build该文件:'+pcode)
            return
        dfm = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = pcode)
        dfm[X_col] = 0
        dfm[Y_col] = 0
        dfm['evaluation'] = 0
        dfm['PEG'] = 0
        dfm['evaluation_grow_rate'] = 0
        dfm['total_mv_grow_rate']=0

        dfm['trade_date'] = dfm['trade_date'].apply(lambda x:int(x))
        dfm = dfm.drop_duplicates('trade_date')
        dfm.set_index(["trade_date"], inplace=True,drop = False) 
        
        end_date1 = '19950101'
        end_date2 = '19950101'
        #遍历每个财报日
     
        for i,row in df_evl.iterrows():
            end_date1 = end_date2
            end_date2 = row['end_date']
            df = dfm[(dfm['trade_date']>int(end_date1)) & (dfm['trade_date']<int(end_date2))]
            if df.empty:
                continue
            l = len(df)
            s = df.iloc[0]['trade_date']
            e = df.iloc[l-1]['trade_date']   
            dfm.loc[s:e,'evaluation'] = row['evaluation']  
            dfm.loc[s:e,'evaluation_grow_rate'] = row['evaluation_grow_rate']  
            dfm.loc[s:e,'total_mv_grow_rate'] = (dfm.loc[e]['total_mv'] - dfm.loc[s]['total_mv'])/dfm.loc[s]['total_mv']
           
            #df[Y_col] = (df['total_mv'] - df.iloc[0]['total_mv']) / df.iloc[0]['total_mv']
            dfm.loc[s:e,Y_col] = (df['total_mv'] - df.iloc[0]['total_mv']) / df.iloc[0]['total_mv']

            dfm.loc[s:e,'PEG'] = df['pe_ttm'] / (row['y0_grow_rate']*100)
        dfm['市场低估比率'] = (dfm['evaluation'] - dfm['total_mv']*10000)/(dfm['total_mv']*10000)
        dfm['grade'] = dfm['市场低估比率'].apply(lambda x: _fun(x))
        #dfm = msql.joinnames(df_basic)
        #dfm = dfm.sort_values('市场低估比率',ascending = False) 
        dfm[X_col] = dfm['市场低估比率'].apply(lambda x: _fun(x))

        dfm.to_csv(monitor_path,encoding='utf_8_sig',index = True)


    #用每天的数据和季度估值做比较，每天调用,    
    def monitor(self,pcode = None,pdate = None):
        msql = md.datamodule()
        if pdate != None:
            latestday = pdate
        else:
            latestday = msql.getlatestday('daily_basic')
        dfm = pd.DataFrame()
        FCFF_csv = self._get_path_build_for_monitor(pdate=latestday)
        if pcode != None :
            try:
                dfm = pd.read_csv(FCFF_csv,index_col = 0)
                if not dfm.empty:
                    #已经是最新的数据了，直接返回需要的值
                    if dfm.iloc[0]['lastupdate'] == int(latestday):
                        return dfm.loc[pcode]
            except:
                print('没找到:'+FCFF_csv,'重新建立')

        df_now = msql.pull_mysql(db = 'daily_basic',date = latestday)
        if df_now.empty:
            print('FCFF monitor df_now.empty:',latestday)
            return df_now
        df_now.set_index(["ts_code"], inplace=True,drop = True) 

        df_basic = pd.read_csv(FCFF_csv,index_col = False )
        df_basic.set_index(["ts_code"], inplace=True,drop = False)
        df_basic['lastupdate'] = latestday
        df_basic['total_mv'] = df_now['total_mv']*10000
        df_basic['close'] = df_now['close']
  
        df_basic['市场低估比率'] = (df_basic['evaluation'] - df_now['total_mv']*10000)/(df_now['total_mv']*10000)
        
        df_basic = msql.joinnames(df_basic)
        df_basic = df_basic.sort_values('市场低估比率',ascending = False) 

 
        df_basic['grade'] = df_basic['市场低估比率'].apply(lambda x: _fun(x))
        df_basic['evaluation_price'] = df_basic['evaluation']/(df_now['total_share']*10000)

        
        if pdate !=None:
            #df_basic = df_basic.reset_index()
            return df_basic
        if pcode != None:
            return df_basic.loc[pcode]

        df_basic.to_csv(self.monitor_csv,encoding='utf_8_sig',index = True)

    def _get_path_build_for_monitor(self,pdate=None):
        if pdate == None:
            msql = md.datamodule()
            end_date = msql.getlatestday()
        else:#20190418，为了backtest，生成所有日期的信息
            end_date = pdate
    
        if end_date[4:] <='0630':
            end_date = end_date[:4]+'0630'
        else:
            end_date = end_date[:4]+'1231'

        return self._trainpath(end_date)

    def _build_for_monitor(self,pdate = None):
        df_rslt = pd.DataFrame()
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        msql = md.datamodule()

        ts_code_df = msql.getts_code()
        if pdate == None:
            end_date = msql.getlatestday()
        else:#20190418，为了backtest，生成所有日期的信息
            end_date = pdate

        if end_date[4:] <='0630':
            end_date = end_date[:4]+'0630'
        else:
            end_date = end_date[:4]+'1231' 
        for i,code in ts_code_df.iterrows():
            print(code['ts_code'])
            df_income = msql.pull_mysql(db = 'income',ts_code = code['ts_code'])
            df_cash = msql.pull_mysql(db = 'cashflow',ts_code = code['ts_code'])
            df_blc = msql.pull_mysql(db = 'balancesheet',ts_code = code['ts_code'])
            df_future = msql.pull_mysql(db = 'future_income',ts_code = code['ts_code'])
            df1 = self._buildtemplate(df = df_template,code = code['ts_code'],pdate = end_date,df_income=df_income,df_cash=df_cash,df_blc=df_blc,df_future=df_future)
            
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

        filepath=self._get_path_build_for_monitor(pdate=pdate)
        df_rslt.to_csv(filepath,encoding='utf_8_sig',index = False) 

    def _build_for_monitor_one(self,pcode=None,p1 = None):
        pdate = p1
        print(pdate)
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        msql = md.datamodule()

        ts_code_df = msql.getts_code()
        if pdate == None:
            end_date = msql.getlatestday()
        else:#20190418，为了backtest，生成所有日期的信息
            end_date = pdate

        if end_date[4:] <='0630':
            end_date = end_date[:4]+'0630'
        else:
            end_date = end_date[:4]+'1231' 
        code = {}
        code['ts_code'] = pcode
      
        print(code['ts_code'])
        df_income = msql.pull_mysql(db = 'income',ts_code = code['ts_code'])
        df_cash = msql.pull_mysql(db = 'cashflow',ts_code = code['ts_code'])
        df_blc = msql.pull_mysql(db = 'balancesheet',ts_code = code['ts_code'])
        df_future = msql.pull_mysql(db = 'future_income',ts_code = code['ts_code'])
        df1 = self._buildtemplate(df = df_template,code = code['ts_code'],pdate = end_date,df_income=df_income,df_cash=df_cash,df_blc=df_blc,df_future=df_future)
        
        if df1.empty:
            return
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
        self.sem.acquire()
        self.df_rslt = pd.concat([self.df_rslt,df1.loc[0:0,]],ignore_index = True)
        self.sem.release()

    #pcode必须，储存一个code的evaluation数据到 /data/train_FCFF/002008.sz
    def detail_template(self,pcode = None,ptrade_date = None):
        if pcode == None or ptrade_date == None:
            return
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        msql = md.datamodule()
        df_income = msql.pull_mysql(db = 'income',ts_code = pcode)
        df_cash = msql.pull_mysql(db = 'cashflow',ts_code = pcode)
        df_blc = msql.pull_mysql(db = 'balancesheet',ts_code = pcode)
        df_future = msql.pull_mysql(db = 'future_income',ts_code = pcode)

        df_rslt = pd.DataFrame()
             
        df_rslt = self._buildtemplate(df = df_template,code = pcode,pdate = ptrade_date,df_income=df_income,df_cash=df_cash,df_blc=df_blc,df_future = df_future)
        
        filepath = config.detaildir(pcode,pcode+ptrade_date+'template.csv')
        df_rslt.to_csv(filepath,encoding='utf_8_sig',index = False)        

    def _build_one_train(self,pcode = None,ptrade_date = None):
        if pcode == None:
            return
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        msql = md.datamodule()
        df_income = msql.pull_mysql(db = 'income',ts_code = pcode)
        df_cash = msql.pull_mysql(db = 'cashflow',ts_code = pcode)
        df_blc = msql.pull_mysql(db = 'balancesheet',ts_code = pcode)
        df_future = msql.pull_mysql(db = 'future_income',ts_code = pcode)
        

        df_rslt = pd.DataFrame()
        last_evalueation = 0
        for year in range(1995,self.thisyear,1):
            for month in (0,1):
                if month ==0:
                    date = str(year)+'0630'
                elif month ==1:
                    date = str(year)+'1231'
                
                if ptrade_date != None:
                    date = ptrade_date

                df1 = pd.DataFrame()
                df2 = pd.DataFrame()
                df1 = self._buildtemplate(df = df_template,code = pcode,pdate = date,df_income=df_income,df_cash=df_cash,df_blc=df_blc,df_future = df_future)
    
                if df1.empty:
                    continue
        
                dic = {}

                dic['ts_code'] = pcode
                dic['evaluation'] = df1['y-6'][37]
                dic['end_date'] = [date]
                dic['debt']    = df1['y-6'][32]

                dic['y0_flow'] = df1['y0'][13]
                dic['y-1_flow'] = df1['y-1'][13]
                dic['y-2_flow'] = df1['y-2'][13]

                dic['y0_income'] = df1['y0'][8]
                dic['y-1_income'] = df1['y-1'][8]
                dic['y-2_income'] = df1['y-2'][8]

                dic['y0_grow_rate'] = df1['y0'][14]
                dic['y-1_grow_rate'] = df1['y-1'][14]
                dic['y-2_grow_rate'] = df1['y-2'][14]

                dic['sus_grow_rate'] = df1['y-6'][22]
                dic['y1_grow_rate'] = df1['y1'][22]
                dic['y2_grow_rate'] = df1['y2'][22]
                if last_evalueation != 0:
                    dic['evaluation_grow_rate'] = (dic['evaluation'] - last_evalueation)/last_evalueation
                last_evalueation = dic['evaluation']

        
                df2 = pd.DataFrame.from_dict(dic)
                df_rslt = pd.concat([df_rslt,df2],ignore_index = True)

                if ptrade_date != None:
                    filepath=self._trainpath(pcode)
                    df_rslt.to_csv(filepath,encoding='utf_8_sig',index = False)
                    return
    
        filepath=self._trainpath(pcode)
        df_rslt.to_csv(filepath,encoding='utf_8_sig',index = False)
        #self._monitor_one_train(pcode = pcode)
    
    def _buildtemplate(self,df,code,pdate,df_income,df_cash,df_blc,df_future):
        df = self._getdata(ts_code = code,ptemplate = df,pdate = pdate,df_income=df_income,df_cash=df_cash,df_blc=df_blc,df_future=df_future)
        if df.empty:
            return df
        df = self._processdata(pdf=df)
        return df
        #df.to_csv(self.FCFF_csv,encoding='utf_8_sig')
    #获取利润等数据，存入以下行：8:12，22，32，33，34
    def _getdata_one(self,ptemplate,pdate,df_income,df_cash,df_blc,i):
        template = ptemplate.copy(deep=True)
        endyear = int(pdate[0:4])
        #滚动四季度等于今年前3季度+去年第四季度q3+(lq12-lq3)，对其他季度同样适用这个公式
        q3 = str(i)+pdate[4:] #'20181231' or 20180930，20180630，20180331 ,
        lq12 = str(i-1)+'1231' #20171231
        lq3 = str(i-1)+pdate[4:]#20171231 or 20170931 2017630 20170331
        #end_date1 = str(i+1)+'1231'
        y = 'y'+str(i - endyear)
        #8 :净利润：income：n_income
        dfq3   = df_income[df_income['end_date'] == q3]
        dflq12 = df_income[df_income['end_date'] == lq12]
        dflq3  = df_income[df_income['end_date'] == lq3]

        if not dfq3.empty and not dflq12.empty and not dflq3.empty:
            try:
                template.loc[8,y] = dfq3.iloc[0]['n_income'] + dflq12.iloc[0]['n_income'] - dflq3.iloc[0]['n_income']
            except:
                logging.debug(q3+'净利润数据缺失')
                return template
        else:
            logging.debug(q3+'净利润数据为空')
            return template
            
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

                #15 从tushare直接获取自由现金流，用于对比
      
                template.loc[15,y] = dfq3.iloc[0]['free_cashflow']
                template.loc[15,'name'] = '从ts获取的自由现金流'
    
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
        return template
                

    def _getdata(self,ts_code,ptemplate,pdate,df_income,df_cash,df_blc,df_future):
        template = ptemplate.copy(deep=True)

        if df_income.empty or df_cash.empty or df_blc.empty :
            return pd.DataFrame()
        #如果输入的pdate是整年1231，则获取过去7年的年报，如果pdate是‘ 0930，0630，0331’等季度，则滚动前四季度记为一年
        endyear = int(pdate[0:4])
        df_future['year'] = df_future['year'].apply(lambda x:int(x))
        df_future['n_income'] = df_future['n_income'].apply(lambda x:func1(x))

        #处理y0 --y-7 的现金流
        for i in range(endyear,endyear- 7,-1):
            template = self._getdata_one(ptemplate = template,pdate = pdate ,df_income = df_income,df_cash=df_cash,df_blc=df_blc,i=i)
           
        #处理y1--y6现金流
        for i in range(endyear+1,endyear+6,1):
            future = pd.DataFrame()
            if not df_future.empty:
                future = df_future[df_future['year'] == i]
            if i < self.thisyear:
                template = self._getdata_one(ptemplate = template,pdate = pdate ,df_income = df_income,df_cash=df_cash,df_blc=df_blc,i=i)
            elif not future.empty:
                #处理future现金流
                y = 'y'+str(i - endyear)
                #print(future.iloc[0]['n_income'])
                template.loc[8,y] = future.iloc[0]['n_income']*1e8
               
            else:
                #保持空
                pass
                #template.loc[8,y] = 0
          
        #22永续增长率设为gdp
        template.loc[22,'y-6'] = SUSTAINABLE_GROWTH_RATE
        #32，33，34带息负债，非经营负债，少数股东权益设为0
        template.loc[32,'y-6'] = 0
        template.loc[33,'y-6'] = 0
        template.loc[34,'y-6'] = 0
        
        #处理第7行，对年份的显示
        for i in range(endyear-6,endyear+6,1):
            y = 'y'+str(i - endyear)
            template.loc[7,y] = i
        #处理34行，少数股东权益
        dftemp = df_blc[df_blc['end_date'] == pdate]
        if not dftemp.empty:
            t = pd.isnull(dftemp)
            if not t.iloc[0]['minority_int']:
                template.loc[34,'y-6'] = 0-dftemp.iloc[0]['minority_int']
            if not t.iloc[0]['bond_payable']:
                template.loc[32,'y-6'] = 0-dftemp.iloc[0]['bond_payable']

        return template

    def _processdata(self,pdf):
        df = pdf.copy(deep=True)
        #处理第13行前半部分y-7:y0
        df1 = df.loc[8:12,'y-7':'y0'] 
        df.loc[13,'y-7':'y0'] = df1.apply('sum',axis=0)

        #处理第14行
        for i in range(-6,1):
            y0 = 'y'+str(i-1)
            y1 = 'y'+str(i)
            df.loc[14,y1] = (df.loc[8][y1]-df.loc[8][y0])/math.fabs(df.loc[8][y0])
        
        
        #处理第22行，如果增长率为负数，则取0，默认未来不会恶化
        #如果过去两年现金流大于0，则计算过去两年增速，否则过去两年增速设为0
        if df.loc[13,'y-1'] > 0 and df.loc[13,'y-2']>0:
            df.loc[22,'y0'] = df.loc[13,'y0']/df.loc[13,'y-1'] -1
            df.loc[22,'y-1'] = df.loc[13,'y-1']/df.loc[13,'y-2'] -1
        else :
            df.loc[22,'y0'] = 0
            df.loc[22,'y-1'] = 0
        for i in range(1,6):
            y0 = 'y'+str(i-1)
            y = 'y'+str(i)
            df1 = df.loc[8:12,y:y] 
    
            s = pd.isnull(df1)
            #s=np.isnan(df1[y])
            #如果利润为空，则用上一年的增速
            if s.loc[8][y]:
                df.loc[22,y] = df.loc[22,y0]
            #如果有利润但是其他值为空,则用利润增速作为现金流增速
            elif s.loc[9][y] and s.loc[10][y] and s.loc[11][y] and s.loc[12][y]:
                if df.loc[8,y]>0 and df.loc[8,y0]>0:
                    df.loc[22,y] = df.loc[8,y]/df.loc[8,y0] -1
                else:
                    df.loc[22,y] = 0
            #即有利润又有其他值，则按照标准公式计算出现金流及增速
            else:
                su = df1.apply('sum',axis=0)
                df.loc[13,y] = su.iloc[0]
                df.loc[23,y] = 'T' #标记已经处理了现金流，不需要处理了
                #df.loc[22,y] = df.loc[13,y]/df.loc[13,y0] -1
        
        #处理第13行后半部分：y1-y5
 
        for i in range(0,5):
            y_1 = 'y' +str(i-1)
            y0 = 'y'+str(i)
            y1 = 'y'+str(i+1)
            s = df.loc[13][y0]
            #数据质量比较差，如果没有上一年数据，则向前找一年数据,并且将缺失的数据用前一年数据补齐
            if s == 0 :
                s = df.loc[13][y_1]
                df.loc[13,y0] = s
            #如果没有用标准公式得到现金流，则用增速得到现金流
            if not df.loc[23,y] == 'T':
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
        #df.loc[27,'y-6'] =  df['y-6'][24] + df['y-6'][25] + df['y-6'][26] 
        df.loc[27,'y-6'] =  df['y0'][19] + df['y-6'][25] + df['y-6'][26]
        
        #处理第35行
        df.loc[35,'y-6'] =  df['y-6'][27] + df['y-6'][32] + df['y-6'][33] + df['y-6'][34] 

        #处理第36行
        df.loc[36,'y-6'] = 0

        #处理第37行
        df.loc[37,'y-6'] = df.loc[35]['y-6'] + df.loc[36]['y-6'] 
        return df

    def plot(self,ts_code):
        filepath = self._trainpath('monitor_'+ts_code)
        dfm = pd.read_csv(filepath,index_col = None)
        dfm['X'] = dfm['evaluation_grow_rate']
        dfm['Y'] = dfm['total_mv_grow_rate']
        dfm['trade_date'] = dfm['trade_date'].apply(lambda x:datetime.datetime.strptime(str(x), "%Y%m%d"))
        #df = dfm.loc[300:3000,'市场高估比率']
        #print(df)
        #df = dfm.loc['300287.SZ':'603881.SH','市场高估比率']
        #df.plot.kde()
        #plt.savefig('D:\test.png')
        #dfm.plot(x='X',y='Y',kind = 'scatter')

        Pearson = stats.pearsonr(dfm['X'],dfm['Y'])
        print(Pearson)
        plt.plot_date(dfm['trade_date'],dfm['X'])
        plt.plot_date(dfm['trade_date'],dfm['Y'])
        plt.show()

    def plotsave_one(self,ts_code):
        plt.rcParams['font.family'] = ['sans-serif']
        plt.rcParams['font.sans-serif'] = ['SimHei']

        msql = md.datamodule()
        ts_name = msql.getname(ts_code)

        filepath = self._trainpath(ts_code)

        dfm = pd.read_csv(filepath,index_col = None)
        dfm['evaluation_price'] = 0
        dfm['close']    = 0
        dfm['total_share']=0
        dfm['close_date'] = '1111'
        dfm['name'] = ts_name
        dfm['evaluation_price_avg']=0 
        dfm['evaluation_price_avg1'] = 0
       
        
        dfy = pd.DataFrame()  
        dfz = pd.DataFrame()  
        h_share = 0 
        dfy = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = ts_code)
        
        dfz = msql.gettable(pdb = 'h_stock',ptable = 'h_stock')
        dfz=dfz[dfz['ts_code']== ts_code]
        if not dfz.empty:
            h_share = dfz.iloc[0]['h_share']
        dfm.set_index(["end_date"], inplace=True,drop = False)
        dfy.set_index(["trade_date"], inplace=True,drop = False)
       
        j = 0
        for i,row in dfm.iterrows():
            date = row['end_date']

            date = ut.findtradeday(pdf=dfy,pdate=date,pdatestr = 'trade_date')
            dfm.loc[i,'close_date']  = dfy.loc[date]['trade_date']
            total_share = dfy.loc[date]['total_share']*10000+h_share
            dfm.loc[i,'total_share'] = total_share
             
            dfm.loc[i,'evaluation_price'] = row['evaluation']/total_share
            dfm.loc[i,'close'] = dfy.loc[date]['close']
            if j >= 2:
                dfm.loc[i,'evaluation_price_avg'] = (dfm.iloc[j]['evaluation_price']+dfm.iloc[j-1]['evaluation_price']+dfm.iloc[j-2]['evaluation_price'])/3
            else:
                dfm.loc[i,'evaluation_price_avg'] = dfm.loc[i,'evaluation_price']
            j = j+1

        self.sem.acquire()
        self.df_rslt = pd.concat([self.df_rslt,dfm],ignore_index = True)
        self.sem.release()

        #dfm['evaluation_price_avg1'] = dfm['evaluation_price'].apply('mean',axis=0)
        #dfy['avg'] = dfy['close'].apply('mean',axis=0)
        
        dfm['Y']=dfm['evaluation_price_avg']
        dfy['Y']=dfy['close']
        dfy['ROE'] = dfy['pb']/dfy['pe']*100
        dfm = self.line(dfm)
        dfy = self.line(dfy)

        dfy['trade_date'] = dfy['trade_date'].apply(lambda x:datetime.datetime.strptime(str(x), "%Y%m%d"))
        dfm['end_date'] = dfm['end_date'].apply(lambda x:datetime.datetime.strptime(str(x), "%Y%m%d"))
   
        plt.title(ts_name+'_'+ts_code,fontsize='large',fontweight='bold') 
        #plt.plot_date(dfm.loc['20021231':]['end_date'],dfm.loc['20021231':]['evaluation_price'],ls='-',label='评估值')
        plt.plot_date(dfm.loc['20021231':]['end_date'],dfm.loc['20021231':]['evaluation_price_avg'],ls='-',Markersize=0.1,color='darkorange',label='内涵值')
        plt.plot_date(dfm.loc['20021231':]['end_date'],dfm.loc['20021231':]['y_line'],ls='--',Markersize=0,color='darkorange',label='内涵值均线')
        plt.plot_date(dfy['trade_date'],dfy['close'],ls='-',Markersize=0.1,color = 'darkgreen',label='实际股价')
        plt.plot_date(dfy['trade_date'],dfy['y_line'],ls='--',Markersize=0,color = 'lightgreen',label='实际均线')
        plt.plot_date(dfy['trade_date'],dfy['ROE'],ls='-',Markersize=0,color = 'lightgreen',label='ROE')

        plt.legend(loc=0,ncol=2)
        plt.savefig(filepath+'.png')
        plt.clf()
        plt.close()
        #filepath = self._trainpath('xueqiu'+ts_code)
        #self.df_rslt.to_csv(filepath,encoding='utf_8_sig',index = True)
    def line(self,pdf = None):
        df = pd.DataFrame()
        df['Y'] = pdf['Y']
        df = df.reset_index()
        df['line_date'] = df.index


        rawmat = np.mat(df)
        mat = rawmat[:,2]
        y = rawmat[:,1]
        clf = linear_model.LinearRegression(fit_intercept = True)
        clf.fit(mat,y)
        weights_OLS = float(clf.coef_[0][0])
        bias = clf.intercept_
        df.index = pdf.index
        pdf['y_line'] = df['line_date']*weights_OLS + bias
   
        '''
        lenth = len(df)
        if lenth > 1000:
            lr = 0.00001
            epochs = 200
        else:
            lr = 0.001
            epochs = 200

        net = pytorch.Pytorch(pnet='linearRegression',d_in=1,plr=lr,pepochs = epochs)
        net.fit(df_x=df.loc[:,'line_date':'line_date'],df_y=df.loc[:,'Y':'Y'])

        weight = net.model.state_dict()['lin.weight'].numpy()[0][0]
        bias   = net.model.state_dict()['lin.bias'].numpy()[0]
        df.index = pdf.index
        pdf['y_line'] = df['line_date']*weight + bias
        '''
        return pdf
    def monitor_one_xueqiu(self,ts_code = None,p3=None):
        trade_date = p3
        plt.rcParams['font.family'] = ['sans-serif']
        plt.rcParams['font.sans-serif'] = ['SimHei']

        msql = md.datamodule()
        ts_name = msql.getname(ts_code)

        filepath = self._trainpath(ts_code)

        dfm = pd.read_csv(filepath,index_col = None)
        dfm['evaluation_price'] = 0
        dfm['close']    = 0
        dfm['total_share']=0
        dfm['close_date'] = '1111'
        dfm['name'] = ts_name
        
        dfy = pd.DataFrame()  
        dfz = pd.DataFrame()  
        h_share = 0 
        dfy = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = ts_code)
        
        dfz = msql.gettable(pdb = 'h_stock',ptable = 'h_stock')
        dfz=dfz[dfz['ts_code']== ts_code]
        if not dfz.empty:
            h_share = dfz.iloc[0]['h_share']
            print(dfz)
        dfm.set_index(["end_date"], inplace=True,drop = False)
        dfy.set_index(["trade_date"], inplace=True,drop = False)

        for i,row in dfm.iterrows():
            date = row['end_date']

            date = ut.findtradeday(pdf=dfy,pdate=date,pdatestr = 'trade_date')
            dfm.loc[i,'close_date']  = dfy.loc[date]['trade_date']
            total_share = dfy.loc[date]['total_share']*10000+h_share
            dfm.loc[i,'total_share'] = total_share
             
            dfm.loc[i,'evaluation_price'] = row['evaluation']/total_share
            dfm.loc[i,'close'] = dfy.loc[date]['close']

        if trade_date != None:
           dfm = dfm[dfm['end_date']==int(trade_date)]

        self.sem.acquire()
        self.df_rslt = pd.concat([self.df_rslt,dfm],ignore_index = True)
        self.sem.release()
    
    def _trainpath(self,file):
        fpath = file+'.csv'
        return os.path.join(config.MODULE_PATH['train_FCFF'], fpath)

    def buildone_template(self,ts_code,pdate):
        df_template = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
        msql = md.datamodule()
        df_income = msql.pull_mysql(db = 'income',ts_code = ts_code)
        df_cash = msql.pull_mysql(db = 'cashflow',ts_code = ts_code)
        df_blc = msql.pull_mysql(db = 'balancesheet',ts_code = ts_code)
        df_future = msql.pull_mysql(db = 'future_income',ts_code = ts_code)
  
        date = pdate
        df = self._buildtemplate(df =df_template, code = ts_code,pdate = date,df_income=df_income,df_cash=df_cash,df_blc=df_blc,df_future=df_future)
        file = self._trainpath(ts_code+'template')
        df.to_csv(file,encoding='utf_8_sig',index = True)


if __name__ == '__main__':
    #all_build_for_monitor()
    fcff = FCFF()
    fcff._build_one_for_monitor(pcode='000002.SZ',p1 = '20181231')
    fcff._build_one_for_monitor(pcode='600036.SH',p1 = '20181231')
    print(fcff.df_rslt)
    #fcff.df_rslt.to_csv()
    #f.all_build_for_monitor()
    #f.monitor()
    #f.detail_template(pcode = '002672.SZ',ptrade_date = '20181231')
    #f.train_FCFF()
    #f.monitor_train()
    #msql = md.datamodule()
    #msql.updatedbone(db1 = 'index_daily',firsttime=1)
    #msql._push_daily_basic(start='19940101',end='20011118',firsttime = 0)
    #msql.updatedball(daily = False,quter=True)
    #print(msql.gettradedays())
    #msql._pushfutureincome()
    #f._build_one_train(pcode = '300418.SZ',ptrade_date = None)#(pcode = '002008.SZ')#,ptrade_date = '20181231')
    #f._monitor_for_train(pcode = '002008.SZ')
 
    #f.plot('000651.SZ')
    #d = f.monitor('000002.SZ')
    #print(d)
    #piecewise_linear(t, deltas, k, m, changepoint_ts)
    
    

