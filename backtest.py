import pandas as pd
def backtest(pstartdate,penddate):
    for date in range(pstartdate,penddate):
        onbar(date)
def onbar(pdate):
    dffcff = fcff.monitor(pdate=pdate)
    dfbuysell = logic(dffcff)
    doit(dfbuysell)