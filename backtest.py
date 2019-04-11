import pandas as pd
#import os
#import gpath
from backtests import engine as eg
import regression as remod

def backtest(module):
    features  = module.CONFIG['features']   # Financial indicators of choice
    trainStart = module.CONFIG['trainStart']               # Start of training period
    trainEnd   = module.CONFIG['trainEnd']             # End of training period
    testStart  = module.CONFIG['testStart']             # Start of testing period
    testEnd    = module.CONFIG['testEnd']            # End of testing period
    buyThreshold  = 0.65         # Confidence threshold for predicting buy (default = 0.65) 
    sellThreshold = 0.65         # Confidence threshold for predicting sell (default = 0.65)
    continuedTraining = False    # Continue training during testing period? (default = false)
    DATA_FILENAME = module.CONFIG['DATA_FILENAME']
    # A little bit of pre-processing
    data = pd.read_csv(DATA_FILENAME) #date_parser=['date'])
    data = data.round(3)                                    

    # Start backtesting and optionally modify SVC parameters
    # Available paramaters can be found at: http://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html

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
    regression = remod.Regression()
    backtest(regression)
    