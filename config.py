import os
import sys
import platform
# 将根目录路径添加到环境变量中
ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
sys.path.append(ROOT_PATH)

# 将功能模块的目录路径添加到环境变量中
# 若各目录下存在同名文件可能导致异常，请注意测试
MODULE_PATH = {}
MODULE_PATH['backtests'] = os.path.join(ROOT_PATH, 'backtests')
MODULE_PATH['module'] = os.path.join(ROOT_PATH, 'module')
MODULE_PATH['data'] = os.path.join(ROOT_PATH, 'data')
MODULE_PATH['train_FCFF'] = os.path.join(MODULE_PATH['data'], 'train_FCFF')
MODULE_PATH['DQN'] = os.path.join(MODULE_PATH['data'], 'DQN')
MODULE_PATH['detail'] = os.path.join(MODULE_PATH['data'], 'detail')

# 添加到环境变量中
for path in MODULE_PATH.values():
    if path not in sys.path:
        sys.path.append(path)

if 'Windows' in platform.uname() :
    db_update_day = ['index_daily','daily_basic','adj_factor','daily_basic_ts_code','margin_detail']
    #'trade_cal','stock_basic'要在最前面，因为里面包含了其他数据库更新需要的信息
    db_update_quter = ['trade_cal','stock_basic','fina_indicator','balancesheet','cashflow','dividend','income','future_income']
    #db_update_quter = ['income','future_income']
else:
    db_update_day = ['daily_basic_ts_code','dividend']
    db_update_quter =[]

    
configs = {
    'module': {
        'db_update_day' : db_update_day,
        'db_update_quter':db_update_quter,
        #'db_func_list'  : db_func_list,
        'mysqlrecord'   : os.path.join(MODULE_PATH['data'],'mysqlrecord1.csv'),
        'mysqlcmd'      : 'mysql+pymysql://root:152921@localhost:3306/{}?charset=utf8',
        'ah_stock_template':os.path.join(MODULE_PATH['data'],'ah_stock_template.csv'),
    },
    'blackswan': {
        'blackswan_csv' : os.path.join(MODULE_PATH['data'],'mblackswan.csv'),
        'blackswan_csv1': os.path.join(MODULE_PATH['data'],'mblackswan1.csv'),
        'moniter_csv'   : os.path.join(MODULE_PATH['data'],'moniter_blackswan.csv'),
    },
    'basicvalue':{
        'basic_csv'     : os.path.join(MODULE_PATH['data'],'mbasic.csv'),
        'monitor_basic' : os.path.join(MODULE_PATH['data'],'monitor_basic.csv'),
        'recommand_basic':os.path.join(MODULE_PATH['data'],'recommand_basic.csv'),
    },
    'dividend':{
        'dividend_csv'  : os.path.join(MODULE_PATH['data'],'dividend.csv'),
    },
    'FCFF':{
        'FCFF_csv'      : os.path.join(MODULE_PATH['data'],'FCFF.csv'),
        'FCFF_template' : os.path.join(MODULE_PATH['data'],'template.xls'),
        'monitor_FCFF_csv':os.path.join(MODULE_PATH['data'],'monitor_FCFF.csv'),
        'logfile_path'  :os.path.join(MODULE_PATH['data'],'log.txt'),
    },
    'Regression':{
        'Regression_csv'    :os.path.join(MODULE_PATH['data'],'Regression.csv'),
        'Regression_cfg'    :os.path.join(MODULE_PATH['data'],'Regression.cfg'),
    },
    'Backtest':{
        'account_csv'    :os.path.join(MODULE_PATH['data'],'account.csv'),
    },
}

# Simple dict but support access as x.y style.
class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

def toDict(d):
    D = Dict()
    for k, v in d.items():
        D[k] = toDict(v) if isinstance(v, dict) else v
    return D

configs = toDict(configs)

def detaildir(dirname,file):
    dirc = os.path.join(MODULE_PATH['detail'], dirname)
    if not os.path.exists(dirc):
        os.makedirs(dirc)

    return os.path.join(dirc,file)

def FCFFpath(filename):
        fpath = filename+'.csv'
        return os.path.join(MODULE_PATH['train_FCFF'], fpath)
def DQNpath(filename):
        return os.path.join(MODULE_PATH['DQN'], filename)