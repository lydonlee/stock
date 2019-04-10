import pandas as pd
import numpy as np
from sklearn import linear_model
from scipy import stats
import matplotlib.pyplot as plt
import config

# 归一化
def normalization(series):
    return (series - min(series))/(max(series) - min(series)) 

class Regression(object):
    def __init__(self):
        cfg = config.configs.Regression
        self.r_csv = cfg.Regression_csv
        #factors = fina_indicator.columns.values.tolist()#['n_income','evaluation','n_asset','grow_rate','dividend']
    def linear_model(self):
        clf = linear_model.LinearRegression()
        clf.fit(self.trainMat,self.trainY)
        self.weights_OLS = clf.coef_[0]
        # 查看回归系数
        print(self.weights_OLS)

    def lasso_model(self):
        best_lam = self._lasso_best_err()
        clf2 = linear_model.Lasso(alpha=best_lam)
        clf2.fit(self.trainMat,self.trainY)
        self.weights_lasso = clf2.coef_
        print(self.weights_lasso)

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

    def _build_one_train(self,pcode = None):
        if pcode == None:
            return

        msql = md.datamodule()
        df_fina = msql.pull_mysql(db = 'fina_indicator',ts_code = pcode)
        df_basic = msql.pull_mysql(db = 'daily_basic_ts_code',ts_code = pcode)
        df_fina['price'] = df_basic[df_basic['end_date'] == ]

        df_rslt = pd.DataFrame()
    def getdata(self):
        df = pd.read_csv("data_3.csv")
        del df['Unnamed: 0']
        df = df.dropna()

        factors = fina_indicator.columns.values.tolist()
        ncol = len(factors)
        ndf = len(df)
        nd = round(ndf/3)

        # 矩阵化
        rawmat = np.mat(df)
        mat = rawmat[:,2:ncol]
        y = rawmat[:,ncol]

        # 数据集划分
        self.trainMat = mat[0:nd] 
        self.testMat = mat[nd:nd*2]
        self.trainY = y[0:nd]
        self.testY = y[nd:nd*2]
    
    def plot(self):
        dfm['X'] = np.dot(self.trainMate,self.weights_lasso)
        dfm['Y'] = self.trainY
        Pearson = stats.pearsonr(dfm['X'],dfm['Y'])
        print(Pearson)
        plt.plot_date(dfm['X'],dfm['Y'])
        plt.show()

    if __name__ == '__main__':
        msql = md.datamodule()
        msql.updatedbone(self,db1 = 'fina_indicator',firsttime=1)

        reg = Regression()
        reg.getdata()
        reg.lasso_model()
        reg.plot()

