#!D:\ProgramData\Anaconda3\python
from operator import methodcaller
import module 
import regression 
import backtest 
import FCFF 
import util
helpinfor = '''
md =module.datamodule()
    md.updatedball(firsttime = 0,daily = True,quter=False)
    md.updatedbone(db1 = 'future_income',firsttime=1)
    md.fix_db(db = 'fina_indicator')
    md.fix_db_all()
reg=regression.Regression()
bt =backtest.Backtest()
    backtest.findbest()
fcff=FCFF.FCFF()
    FCFF.all_build_for_monitor()
    FCFF.train_FCFF()
    FCFF.train_FCFF_monitor()
'''
if __name__ == '__main__':
    run = True
    print(helpinfor)

    md   = module.datamodule()
    reg  = regression.Regression()
    bt   = backtest.Backtest()
    fcff  =  FCFF.FCFF()

    while(run):
        cmd = input('''输入函数名及参数：
''')
        eval(cmd)
    
  