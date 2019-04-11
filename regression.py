import pandas as pd
import numpy as np
import threading
from sklearn import linear_model
from scipy import stats
import matplotlib.pyplot as plt
from module import module as md
import util as ut
import config

# 归一化
def normalization(series):
    return (series - min(series))/(max(series) - min(series)) 
CONFIG = {
    'trainStart' :   0,
    'trainEnd'   :  200,
    'testStart'  :  201,
    'testEnd'    :  300,
    'features'   :  ["X1", "X2"],
}
class Regression(object):
    def __init__(self):
        cfg = config.configs.Regression
        self.r_csv = cfg.Regression_csv
        self.df_rslt = pd.DataFrame()
        self.sem = threading.Semaphore(1)
        self.regconfig_cfg = cfg.Regression_cfg
        #需要转变为每股收益的columne
        self.col_list = ['tangible_asset','extra_item','profit_dedt','ebitda','gross_margin','op_income','ebit','fcff','fcfe','current_exint','noncurrent_exint','interestdebt','netdebt','working_capital','networking_capital','invest_capital','retained_earnings','fixed_assets']
        #self.loadconfig()

    def loadconfig(self):
        try:
            self.CONFIG = np.load(self.regconfig_cfg).item()
        except Exception as e:
            print(e)         
        print(self.CONFIG) 
    
    def saveconfig(self):
        self.CONFIG['trainStart'] = self.trainStart
        self.CONFIG['trainEnd'] = self.trainEnd
        self.CONFIG['testStart'] = self.testStart
        self.CONFIG['testEnd'] = self.testEnd
        self.CONFIG['featuresStart'] = self.featuresStart
        self.CONFIG['DATA_FILENAME'] = self.r_csv
        self.CONFIG['weights'] = self.weights_lasso
        self.CONFIG['weights_OLS'] = self.weights_OLS
        self.CONFIG['weights_weights_lasso'] = self.weights_lasso
        np.save(self.regconfig_cfg, self.CONFIG) 



    def linear_model(self):
        clf = linear_model.LinearRegression()
        clf.fit(self.trainMat,self.trainY)
        self.weights_OLS = clf.coef_[0]
        # 查看回归系数
    def lasso_model(self):
        best_lam = self._lasso_best_err()
        clf2 = linear_model.Lasso(alpha=best_lam)
        clf2.fit(self.trainMat,self.trainY)
        self.weights_lasso = clf2.coef_
        print(self.weights_lasso)

    def predict(self):
        #dfm = pd.DataFrame()
        x = np.dot(self.testMat,self.weights_lasso)
        x1 = x.getA()
        return x1[0]
        #dfm['X'] = x1[0]

    def _lasso_best_err(self):
        # 参数寻优
        best_err = 1000000000
        best_lam = 0
        for i in range(10000):
            lam = 0.0001*(i+1)
            clf2 = linear_model.Lasso(alpha=lam)
            clf2.fit(self.trainMat,self.trainY)
            y_pre = clf2.predict(self.testMat)
            err = ((np.array(y_pre)-np.array(self.testY))**2).sum()
            if err < best_err:
                best_lam = lam
                best_err = err
        return best_lam
    def build_Regression(self):
        ut.thread_loop(by = 'ts_code',pFunc = self._build_one_train)
        self.df_rslt.to_csv(self.r_csv,encoding='utf_8_sig',index = False)

    def _build_one_train(self,pcode = None):
        if pcode == None:
            return
        msql = md.datamodule()
        df_fina = msql.pull_mysql(db = 'fina_indicator',ts_code = pcode).dropna()
        df_basic = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = pcode).dropna()
        df_fina['Y_price'] = 0
        df_fina['Y_date'] = '19950101'

        df = pd.DataFrame()
        #df_fina_null = pd.isnull(df_fina)
        for i,row in df_fina.iterrows():
            ann_date = row['ann_date']
            ann_date = self._findtradeday(df_basic,ann_date)
            df = df_basic[df_basic['trade_date'] == ann_date] 

            df_fina.loc[i,'Y_price'] = df.iloc[0]['close']
            df_fina.loc[i,'Y_date'] =  ann_date
            #转变为每股
            total_share = df.iloc[0]['total_share']*1e4
            for col in self.col_list:
                #if not df_fina_null.loc[i,col]:
                df_fina.loc[i,col] = df_fina.loc[i,col]/total_share
        self.sem.acquire()
        self.df_rslt = pd.concat([self.df_rslt,df_fina],ignore_index = True)
        #self.df_rslt.to_csv(self.r_csv,encoding='utf_8_sig',index = False)
        self.sem.release()

    def _findtradeday(self,pdf,pdate):
        df = pdf.copy(deep=True)
        df['sub'] = abs(df['trade_date'].apply(lambda x:int(x)) - int(pdate))
        i = df['sub'].idxmin()
        return pdf.loc[i]['trade_date']
        
    def getdata(self):
        df = pd.read_csv(self.r_csv,index_col = None)
        #del df['Unnamed: 0']
        df = df.dropna()

        ndf = len(df)
        nd = round(ndf/3)
        self.trainStart=0
        self.trainEnd = nd
        self.testStart = nd
        self.testEnd = nd*2
        self.featuresStart = 4
        self.featuresEnd = 109

        # 矩阵化
        rawmat = np.mat(df)
        mat = rawmat[:,4:109]
        y = rawmat[:,109]
      
        # 数据集划分
        self.trainMat = mat[0:nd] 
        self.testMat = mat[nd:nd*2]
        self.trainY = y[0:nd]
        self.testY = y[nd:nd*2]
    
    def plot(self):
        dfm = pd.DataFrame()
        dft = pd.DataFrame()
        x = np.dot(self.trainMat,self.weights_lasso)
        x1 = x.getA()

        dfm['X'] = x1[0]
        dfm['Y'] = self.trainY
        Pearson = stats.pearsonr(dfm['X'],dfm['Y'])
        print(Pearson)
        lenth = len(dfm)
        plt.plot(range(lenth),dfm['X'],'r')
        plt.plot(range(lenth),dfm['Y'],'r')

        dft['X1'] = self.predict()
        dft['Y1'] = self.testY

        Pearson = stats.pearsonr(dft['X1'],dft['Y1'])
        print(Pearson)
        lenth2 = len(dft)
        plt.plot(range(lenth-1,lenth+lenth2-1,1),dft['X1'])
        plt.plot(range(lenth-1,lenth+lenth2-1,1),dft['Y1'])

        plt.show()

if __name__ == '__main__':
    #msql = md.datamodule()
    #msql.updatedbone(db1 = 'fina_indicator',firsttime=1)
    reg = Regression()
    reg.build_Regression()
    #reg._build_one_train('000002.SZ')

    #reg.getdata()
    #reg.lasso_model()
    #reg.plot()



