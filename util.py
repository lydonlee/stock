import yagmail
import keyring
from io import StringIO
import pandas as pd
from module import module as md
from threading import Thread
from selenium import webdriver

#yagmail.register('904721093@qq.com', 'aaaaaaa')
def sendmail(mailcontent = 'this is content'):

    usrname = "904721093@qq.com"
    paswd = keyring.get_password("yagmail",usrname)
    yag = yagmail.SMTP(user = usrname,password = paswd,host = 'smtp.qq.com',port='587',smtp_ssl=False)
    yag.send(to = usrname, subject = 'from sendmail', contents = mailcontent)


def dftostring(df):
    df_string = StringIO(df.to_csv())
    return df_string.read()

def testdf():
    df = pd.DataFrame(np.random.randn(4,4),index = list('ABCD'),columns=list('OPKL'))
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame.from_dict(d)
    return df
#遍历所有ts_code，pFunc的参数必须是pcode
def thread_loop(by = 'ts_code',pFunc = None,p1=None,p2=None,p3=None,p4=None,p5=None):
    if pFunc == None:
        return
    if by == 'ts_code':
        thread_by_code(pFunc,p1,p2,p3,p4,p5)

def thread_by_code(pFunc = None,p1=None,p2=None,p3=None,p4=None,p5=None):
        if pFunc == None:
            return
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        l = len(ts_code_df)
        l0 = round(l/4)
        df1 = ts_code_df.iloc[0:l0]
        df2 = ts_code_df.iloc[l0:l0*2]
        df3 = ts_code_df.iloc[l0*2:l0*3]
        df4 = ts_code_df.iloc[l0*3:l]

        t1 = Thread(target=_thread_by_code, args=(pFunc,df1,p1,p2,p3,p4,p5))
        t2 = Thread(target=_thread_by_code, args=(pFunc,df2,p1,p2,p3,p4,p5))
        t3 = Thread(target=_thread_by_code, args=(pFunc,df3,p1,p2,p3,p4,p5))
        t4 = Thread(target=_thread_by_code, args=(pFunc,df4,p1,p2,p3,p4,p5))
        
        threads = []
        threads.append(t1)
        threads.append(t2)
        threads.append(t3)
        threads.append(t4)

        for t in threads:
            #t.setDaemon(True)
            t.start()
        for t in threads:
            t.join()

            

#pfunc的第一个参数必须是pcode,容许有5个参数，df 为ts_code
def _thread_by_code(pfunc=None,df=None,p1=None,p2=None,p3=None,p4=None,p5=None):
    print('start thread')
    if pfunc == None:
        return
    for i,code in df.iterrows():
        if p1 == None:
            pfunc(code['ts_code'])
        elif p2 == None:
            pfunc(code['ts_code'],p1)
        elif p3 == None:
            pfunc(code['ts_code'],p1,p2)
        elif p4 == None:
            pfunc(code['ts_code'],p1,p2,p3)
        elif p5 == None:
            pfunc(code['ts_code'],p1,p2,p3,p4)
        elif p5 != None:
            pfunc(code['ts_code'],p1,p2,p3,p4,p5)

if __name__ == '__main__':
    #geturl()
    urllib()

