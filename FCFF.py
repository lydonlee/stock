import pandas as pd
from module import module as md
import config
#使用dividend数据库
class FCFF(object):
    def __init__(self):
        cfg = config.configs.FCFF
        self.template = cfg.FCFF_template
        #self.dic_incrate = {'ever':0.03,'y1':0.1,'y2':0.09,'y3':0.08,'y4':0.07,'y5':0.06}
    def build(self):
        #需要后续处理下面这些行：8:12，14，22，32，33，34

        df = pd.read_excel(self.template,sheet_name='估值结论（FCFF）',index_col = 'id')
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

        print(df.loc[37])


if __name__ == '__main__':
    f = FCFF()
    f.build()
    

