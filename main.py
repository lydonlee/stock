from operator import methodcaller
from module import module as md
if __name__ == '__main__':
    run = True
    msql = md.datamodule()
    while(run):
        cmd = input('''supported command: 
1 updatedball
2 exit
''')
        if   cmd == '1' or cmd == 'updatedball':
            msql.updatedball(daily = True,quter=False) 
        elif cmd == '2' or cmd == 'exit':
            run = False
        else:
            methodcaller('updatedball',daily = True,quter=False)(msql)
  