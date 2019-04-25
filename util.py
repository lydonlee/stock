import yagmail
import keyring
from io import StringIO
import pandas as pd
from threading import Thread
from multiprocessing import Process
from selenium import webdriver
import module as md

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

def process_loop(by = 'ts_code',pFunc = None,p1=None,p2=None,p3=None,p4=None,p5=None):
    if pFunc == None:
        return
    if by == 'ts_code':
        process_by_code(pFunc,p1,p2,p3,p4,p5)

def process_by_code(pFunc = None,p1=None,p2=None,p3=None,p4=None,p5=None):
        if pFunc == None:
            return
        msql = md.datamodule()
        ts_code_df = msql.getts_code()
        l = len(ts_code_df)
        l0 = round(l/4)
        start1 = 0
        end1   = l0
        start2 = l0
        end2   = l0*2
        start3 = l0*2
        end3   = l0*3
        start4 = l0*3
        end4   = l

        t1 = Process(target=_process_by_code, args=(pFunc,start1,end1,p1,p2,p3,p4,p5))
        t2 = Process(target=_process_by_code, args=(pFunc,start2,end2,p1,p2,p3,p4,p5))
        t3 = Process(target=_process_by_code, args=(pFunc,start3,end3,p1,p2,p3,p4,p5))
        t4 = Process(target=_process_by_code, args=(pFunc,start4,end4,p1,p2,p3,p4,p5))
        
        processses = []
        processses.append(t1)
        processses.append(t2)
        processses.append(t3)
        processses.append(t4)

        for t in processses:
            #t.setDaemon(True)
            t.start()
        for t in processses:
            t.join()

#pfunc的第一个参数必须是pcode,容许有5个参数，df 为ts_code
def _process_by_code(pfunc=None,pstart=None,pend=None,p1=None,p2=None,p3=None,p4=None,p5=None):
    print('start process')
    if pfunc == None:
        return
    msql = md.datamodule()
    ts_code_df = msql.getts_code()
    df = ts_code_df.iloc[pstart:pend]

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

def grade(x):
    if x >= 5:
        return 5
    elif x<=-5:
        return -5
    return x

def toquter(date='20190401'):
    if date[4:] <='0630':
        end_date = date[:4]+'0630'
    else:
        end_date = date[:4]+'1231'
    return end_date

@md.redis_df_decorator()
def read_csv(path,index_col = None):
    df = pd.read_csv(path,index_col = index_col)
    return df

if __name__ == '__main__':
    #geturl()
    urllib()

