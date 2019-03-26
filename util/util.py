import yagmail
import keyring
from io import StringIO
import pandas as pd
#yagmail.register('mygmailusername', 'mygmailpassword')
def sendmail(mailcontent = 'this is content'):

    usrname = "904721093@qq.com"
    paswd = keyring.get_password("yagmail",usrname)
    yag = yagmail.SMTP(user = usrname,password = paswd,host = 'smtp.qq.com',port='587',smtp_ssl=False)
    yag.send(to = usrname, subject = 'from sendmail', contents = mailcontent)


def dftostring(df):
    df_string = StringIO(df.to_csv())
    return df_string.read()

def testdf():
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame.from_dict(d)
    return df
    
if __name__ == '__main__':
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame.from_dict(d)
    mail = dftostring(df)
    sendmail(mail)

