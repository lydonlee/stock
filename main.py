from operator import methodcaller
from module import module as md
if __name__ == '__main__':
    run = True
    msql = md.datamodule()
    reg = Regression()
    while(run):
        cmd = input('''supported command: 
1 updatedball
2 exit
''')
        if   cmd == '2' or cmd == 'updatedball':
            msql.updatedball(daily = True,quter=False) 
        elif cmd == '1' or cmd == 'exit':
            run = False
        elif cmd == '3' or cmd == 'regression'
            reg.train()
        else:
            methodcaller('updatedball',daily = True,quter=False)(msql)
  