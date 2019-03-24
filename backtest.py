import pandas as pd
import os
import gpath
from backtests import engine as eg

DATA_FILENAME = os.path.join(gpath.MODULE_PATH['data'], 'data.csv')
BT_CONFIG = {
    'trainStart' :   0,
    'trainEnd'   :  200,
    'testStart'  :  201,
    'testEnd'    :  300,
    'features'   :  ["X1", "X2"],
}
def backtest():
    features  = BT_CONFIG['features']   # Financial indicators of choice
    trainStart = BT_CONFIG['trainStart']               # Start of training period
    trainEnd   = BT_CONFIG['trainEnd']             # End of training period
    testStart  = BT_CONFIG['testStart']             # Start of testing period
    testEnd    = BT_CONFIG['testEnd']            # End of testing period
    buyThreshold  = 0.65         # Confidence threshold for predicting buy (default = 0.65) 
    sellThreshold = 0.65         # Confidence threshold for predicting sell (default = 0.65)
    continuedTraining = False    # Continue training during testing period? (default = false)

    # Initialize backtester
    backtest = eg.Backtest(features, trainStart, trainEnd, testStart, testEnd, buyThreshold, sellThreshold, continuedTraining)

    # A little bit of pre-processing
    data = pd.read_csv(DATA_FILENAME, date_parser=['date'])
    data = data.round(3)                                    

    # Start backtesting and optionally modify SVC parameters
    # Available paramaters can be found at: http://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html
    backtest.start(data, kernel='rbf', C=1, gamma=10)
    backtest.conditions()
    backtest.statistics()  
    backtest.visualize('SBUX')

    simulation = eg.Simulation(features, trainStart, trainEnd, testStart, testEnd, buyThreshold, sellThreshold, continuedTraining)
    simulation.start(data, 1000, logic, kernel='rbf', C=1, gamma=10)
    simulation.statistics()
    simulation.chart('SBUX')

def logic(account, today, prediction, confidence):
    
    if prediction == 1:
        Risk         = 0.30
        EntryPrice   = today['close']
        EntryCapital = account.BuyingPower*Risk
        if EntryCapital >= 0:
            account.EnterPosition('Long', EntryCapital, EntryPrice)

    if prediction == -1:
        ExitPrice = today['close']
        for Position in account.Positions:  
            if Position.Type == 'Long':
                account.ClosePosition(Position, 1.0, ExitPrice)

if __name__ == '__main__':
    backtest()
    