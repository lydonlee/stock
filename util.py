import yagmail
import keyring
from io import StringIO
import pandas as pd
from selenium import webdriver

def geturl(url = ''):
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.managed_default_content_settings.images": 2,
                 'profile.default_content_setting_values' :{'notifications' : 2}
        }
        chrome_options.add_experimental_option("prefs", prefs)

        #browser = webdriver.Chrome('/Users/liligong/anaconda/lib/python3.6/site-packages/chromedriver',
        #                           chrome_options=chrome_options)
        #browser = webdriver.Remote("http://localhost:4444/wd/hub", webdriver.DesiredCapabilities.HTMLUNITWITHJS)
        browser = webdriver.Chrome(chrome_options=chrome_options)
        browser.implicitly_wait(10)
        try:
            browser.get(url)
        except TimeoutException:
            print("TimeoutException")

        return browser
#yagmail.register('904721093@qq.com', 'pvlatlwgwebqbbbh')
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

def _pushfutureincome():
    db = 'futuren_income'
    concmd = self.mysqlcmd.format(db)
    yconnect = create_engine(concmd) 
    df = pd.DataFrame()
    url_formate = 'http://stockpage.10jqka.com.cn/{}/worth/#forecast'
    dft = self.getts_code()
    for index, code in dft.iterrows():
        ts_code = code[ts_code][:-3]
        url = url_formate.format(ts_code)

        driver = geturl(url)
        driver.implicitly_wait(10)

        y2019 = driver.find_element_by_xpath("//*[@id='forecast']/div[2]/div[2]/div[2]/table/tbody/tr[1]/th").text
        price2019 = driver.find_element_by_xpath("//*[@id='forecast']/div[2]/div[2]/div[2]/table/tbody/tr[1]/td[3]").text

        y2020 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[2]/th').text
        price2020 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[2]/td[3]').text

        y2021 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[3]/th').text
        price2021 = driver.find_element_by_xpath('//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[3]/td[3]').text

        d = {'ts_code': [ts_code,ts_code,ts_code], 'year': [y2019,y2020,y2021],'n_income':[price2019,price2020,price2021]}
        df1 = pd.DataFrame.from_dict(d)
        df = pd.concat([df,df1],ignore_index = False,sort=False)

    pd.io.sql.to_sql(df,db,con=yconnect, schema=db,if_exists='replace')
    

def buildflow():


 
if __name__ == '__main__':
    fg()

   
    #//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[1]/td[3]
    #//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[2]/td[3]
    #//*[@id="forecast"]/div[2]/div[2]/div[2]/table/tbody/tr[3]/td[3]
    

