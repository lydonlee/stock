import pandas as pd
import numpy as np
from sklearn import linear_model
from scipy import stats
import matplotlib.pyplot as plt

# 归一化
def normalization(series):
    return (series - min(series))/(max(series) - min(series)) 

class regression(object):
    def __init__(self):
        factors = ['n_income','evaluation','n_asset','grow_rate','dividend']
        pass
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

    def getdata(self):
        df = pd.read_csv("data_3.csv")
        del df['Unnamed: 0']
        df = df.dropna()

        df['Gain'] = 100*normalization(df['Gain'])  # 这里乘以了100，主要是为了方便观察回归系数，否则系数太小。

        df['PE'] = normalization(df['PE'])
        df['LFLO'] = normalization(df['LFLO'])
        df['NetProfitGrowRate'] = normalization(df['NetProfitGrowRate'])
        df['MoneyFlow20'] = normalization(df['MoneyFlow20'])
        df['PB'] = normalization(df['PB'])
        df['AccountsPayablesTRate'] = normalization(df['AccountsPayablesTRate'])
        df['ROE'] = normalization(df['ROE'])
        df['FiftyTwoWeekHigh'] = normalization(df['FiftyTwoWeekHigh'])

        # 矩阵化
        rawmat = np.mat(df)
        mat = rawmat[:,2:10]
        y = rawmat[:,10]

        # 数据集划分
        self.trainMat = mat[0:4000] 
        self.testMat = mat[4000:]
        self.trainY = y[0:4000]
        self.testY = y[4000:]
    
    def plot(self,ts_code):
        filepath = self._trainpath('monitor_'+ts_code)
        dfm = pd.read_csv(filepath,index_col = None)
        dfm['X'] = (dfm['evaluation']/10000)/dfm['total_share']/2
        dfm['Y'] = dfm['total_mv']/dfm['total_share']
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