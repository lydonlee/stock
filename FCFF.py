import pandas as pd
from module import module as md
import config
#使用dividend数据库
class FCFF(object):
    def __init__(self):
        self.dic_incrate = {'ever':0.03,'y1':0.1,'y2':0.09,'y3':0.08,'y4':0.07,'y5':0.06}
    def build(self):
        df = pd.read_excel("E:\\stock\\template.xls",sheet_name='估值结论（FCFF）',index_col = 'id')
        #df.to_csv("E:\\stock\\FCFF.csv",index=False,encoding = 'utf_8_sig')
        
        #df.set_index(["id"], inplace=True) 
        #print(df.loc[16])
        df1 = df.loc[8:12,'y-7':'y0'] 
        df.at[13,'y-7':'y0'] = df1.apply('sum',axis=0)
        print(self.dic_incrate['y1'])
        print(df.loc[13]['y0'])
      
        df.at[13,'y1'] = df.loc[13]['y0']*(1+self.dic_incrate['y1'])
        print(df.loc[13])

if __name__ == '__main__':
    f = FCFF()
    f.build()
    

