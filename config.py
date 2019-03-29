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

# 添加到环境变量中
for path in MODULE_PATH.values():
    if path not in sys.path:
        sys.path.append(path)

if 'Windows' in platform.uname() :
    db_func_list = ['daily_basic_ts_code','daily_basic','dividend','income']#margin_detail
else:
    db_func_list = ['daily_basic_ts_code','dividend']
    
configs = {
    'module': {
        'db_func_list'  : db_func_list,
        'mysqlrecord'   : os.path.join(MODULE_PATH['data'],'mysqlrecord1.csv'),
        'mysqlcmd'      : 'mysql+pymysql://root:152921@localhost:3306/{}?charset=utf8',
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
        'FCFF_csv'      : os.path.join(MODULE_PATH['data'],'valuation.csv'),
        'FCFF_template' : os.path.join(MODULE_PATH['data'],'template.xls'),

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