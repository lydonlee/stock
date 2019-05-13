import os
import datetime
import math
import random
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from collections import namedtuple
from itertools import count
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
import torchvision.transforms as T
from backtest import Account ,Trader,Backtest
from pytorch import savemodule,loadmoule
import config


# if gpu is to be used
device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
RETRACE_RATE = 0.6 #最大回撤，否则终止本轮训练

class Action(object):
    def __init__(self):
        self.n = 2 #[0,1] #1 买入,持有,0 卖出,不买入:random.randrange(n)

class Market(object):
    def __init__(self):
        self.account =Account(pwhichAccount=0)
        self.trader  = Trader(self.account)
        self.hisnetvalue = pd.DataFrame()
        self.commi       = pd.DataFrame()
        self.close_item       = 0
        self.close_plot       = pd.DataFrame()
        
        self.df          = pd.DataFrame()
        self.i           = 0
        self.action_space    = Action()
        self.n_states    = 3 #网络输入的维度，state number，也可以说是feature number
        self.yestoday_netvalue = self.account.buypower
        self.date        = None
        self.PLOT_SHARES = 0 #确定第一次可以买多少股票
    def reset(self,pts_code):
        self.__init__()
        srcpath = config.FCFFpath('monitor_'+pts_code)
        df = pd.read_csv(srcpath,index_col = 1)
        df = df.sort_values('trade_date',ascending = True)
        self.df=df.reset_index(drop=True)
        self.states = self.state()
        return self.states[0]

    def step(self,action):
        df = self.df
        i  = self.i
        if i + 1 >= len(df) :
            return None,None,True #done True
        row = df.iloc[i]
        date = row['trade_date']
        df1 = df.iloc[i:i+1,:]
        #df1 = df[df['trade_date']==date]
        df1 = df1.set_index(["ts_code"], inplace=False,drop = False)
        date = str(date)
        self.date = date
        current_sate = row['close']
        self.close_item   = row['close']
        next_state   = df.iloc[i+1]['close']

        if action == 1:
            dict_jn = {'ts_code':[row['ts_code']],'price':[row['close']]}
            jn = pd.DataFrame(dict_jn)
            buypower = self.account.buypower * (1- self.account.commission_rate)#留下交易佣金
            jn['Shares'] = buypower/100//jn['price']*100
            jn['buysell'] = 1
        else:
            jn = pd.DataFrame()
        
        self.trader.work(pdf=jn,pdate=date,pfcff=df1)
        self.i += 1

        #计算r值
        state = self.states[self.i]
        today_netvalue = self.account.netvalue(date)
        if today_netvalue == None:
            today_netvalue = self.yestoday_netvalue
        r     = today_netvalue - self.yestoday_netvalue
        self.yestoday_netvalue = today_netvalue

        #最大回撤
        if  today_netvalue < INIT_MONEY * RETRACE_RATE:
            done = True
        else:
            done = False
        return state,r,done #done False

    def state(self):
        df = pd.DataFrame()
        df['close'] = self.df['close']
        df['grade'] = self.df['grade']
        df['PEG'] = self.df['PEG']
        return df.values
    
    def plot_durations(self):
        plt.figure(2)
        plt.clf()
        plt.title('Training...')
        plt.xlabel('year')
        plt.ylabel('account')

        if self.i ==1: #确定第一次可以买多少股票
            self.PLOT_SHARES = self.yestoday_netvalue / self.close_item

        net = self.yestoday_netvalue
        commi = self.account.commi
        close = self.close_item * self.PLOT_SHARES
        d_date = datetime.datetime.strptime(self.date,"%Y%m%d")
        dic = {'netvalue':net,'date':d_date}
        #dic1 = {'commi':commi,'date':d_date}
        dic2 = {'close':close,'date':d_date}
        self.hisnetvalue = self.hisnetvalue.append(dic,ignore_index=True)
        self.close_plot       = self.close_plot.append(dic2,ignore_index=True)
        #self.commi       = self.commi.append(dic1,ignore_index=True)
   
        plt.plot_date(self.hisnetvalue['date'],self.hisnetvalue['netvalue'],ls='-',Markersize=0.1,color='darkgreen',label='account')
        plt.plot_date(self.close_plot['date'],self.close_plot['close'],ls='-',Markersize=0.1,color='darkorange',label='commition')
        # Take 100 episode averages and plot them too

        plt.pause(0.001)  # pause a bit so that plots are updated
    
     #上帝视角，预知第二天股价，低买高卖
    def besttrader(self,ts_code=None):
        df = pd.read_csv('F://stock//data//train_FCFF//monitor_000002.SZ.csv',index_col = 1)
        df = df.sort_values('trade_date',ascending = True)
        df=df.reset_index(drop=True)
        
        for i,row in df.iterrows():
            if i + 1 >= len(df):
                break

            date = row['trade_date']
            df1 = df[df['trade_date']==date]
            df1 = df1.set_index(["ts_code"], inplace=False,drop = False)
            date = str(date)
            current_sate = row['close']
            next_state   = df.iloc[i+1]['close']

            if next_state > current_sate:
                action = 1 #买入或持有
            else:
                action = 0 #卖出或不买入
            if action == 1:
                dict_jn = {'ts_code':[row['ts_code']],'price':[row['close']]}
                jn = pd.DataFrame(dict_jn)
                buypower = self.account.buypower * (1- self.account.commission_rate)#留下交易佣金
                jn['Shares'] = buypower/100//jn['price']*100
                jn['buysell'] = 1
            else:
                jn = pd.DataFrame()
            
            self.trader.work(pdf=jn,pdate=date,pfcff=df1)
        
            self.plot_durations()
            print(self.account.journalaccount)
        self.account.journalaccount.to_csv('F://stock//data//train_FCFF//journal_000002.SZ.csv',encoding='utf_8_sig',index = True)
        
env = Market()
# Hyper Parameters
BATCH_SIZE = 32
LR = 0.01                   # learning rate
EPSILON = 0.9               # greedy policy
GAMMA = 0.9                 # reward discount
TARGET_REPLACE_ITER = 100   # target update frequency
MEMORY_CAPACITY = 5000
N_ACTIONS = env.action_space.n
N_STATES = env.n_states
INIT_MONEY = env.account.buypower

Transition = namedtuple('Transition',
                        ('state', 'action', 'next_state', 'reward'))


class ReplayMemory(object):

    def __init__(self, capacity):
        self.capacity = capacity
        self.memory = []
        self.position = 0

    def push(self, *args):
        """Saves a transition."""
        if len(self.memory) < self.capacity:
            self.memory.append(None)
        self.memory[self.position] = Transition(*args)
        self.position = (self.position + 1) % self.capacity

    def sample(self, batch_size):
        return random.sample(self.memory, batch_size)

    def __len__(self):
        return len(self.memory)

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.fc1 = nn.Linear(N_STATES, 50)
        self.fc1.weight.data.normal_(0, 0.1)   # initialization
        self.out = nn.Linear(50, N_ACTIONS)
        self.out.weight.data.normal_(0, 0.1)   # initialization

    def forward(self, x):
        x = self.fc1(x)
        x = F.relu(x)
        actions_value = self.out(x)
        return actions_value

class DQN(object):
    def __init__(self):
        self.eval_net   = Net().to(device)
        self.target_net = Net().to(device)

        self.learn_step_counter = 0                                     # for target updating
        self.memory_counter = 0                                         # for storing memory
        #self.memory = np.zeros((MEMORY_CAPACITY, N_STATES * 2 + 2))     # initialize memory
        self.memeory = ReplayMemory(MEMORY_CAPACITY)
        self.optimizer = torch.optim.Adam(self.eval_net.parameters(), lr=LR)
        self.loss_func = nn.MSELoss()

    def choose_action(self, x):
        x = torch.unsqueeze(x, 0)
        # input only one sample
        if np.random.uniform() < EPSILON:   # greedy
            with torch.no_grad():
                action = self.eval_net(x).max(1)[1].view(1, 1)

        else:   # random
            action = torch.tensor([[random.randrange(N_ACTIONS)]],dtype=torch.long, device=device)
        return action

    def store_transition(self, s, a, r, s_):
        return self.memeory.push(s, a, s_, r)
    
    def learn(self):
        # target parameter update
        self.learn_step_counter += 1
        if self.learn_step_counter < BATCH_SIZE:
            return

        if self.learn_step_counter % TARGET_REPLACE_ITER == 0:
            self.target_net.load_state_dict(self.eval_net.state_dict())

        # sample batch transitions
        #sample_index = np.random.choice(MEMORY_CAPACITY, BATCH_SIZE)
        #b_memory = self.memory[sample_index, :]
        transitions= self.memeory.sample(BATCH_SIZE)

        batch = Transition(*zip(*transitions))
        state_batch = torch.cat(batch.state)
        action_batch = torch.cat(batch.action)
        reward_batch = torch.cat(batch.reward)
        next_state_batch = torch.cat(batch.next_state)

        state_batch = torch.reshape(state_batch, (BATCH_SIZE,N_STATES))
        next_state_batch = torch.reshape(next_state_batch,(BATCH_SIZE,N_STATES))

        # q_eval w.r.t the action in experience
        q_eval = self.eval_net(state_batch).gather(1, action_batch)  # shape (batch, 1)
        #print('q_eval:',q_eval)
        q_next = self.target_net(next_state_batch).detach()     # detach from graph, don't backpropagate
        q_target = reward_batch + GAMMA * q_next.max(1)[0].view(BATCH_SIZE, 1)   # shape (batch, 1)
        loss = self.loss_func(q_eval, q_target.unsqueeze(1))

        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()
def run():
    dqn = DQN()
    ts_code = '600519.SH'
    #ts_code = '000002.SZ'
    pmodulepath = config.DQNpath(ts_code+'_1')
    sucesstime = 0
    print('Collecting experience...')
    for i_episode in range(10000):
        if i_episode % 20 == 0:
            try:
                loadmoule(dqn.eval_net,pmodulepath)
                print('load module successed!')
            except Exception as e:
                print(e)
        s = env.reset(ts_code)
        s = torch.tensor(s,dtype=torch.float32,device=device)
        ep_r = 0
        ep_r = torch.tensor([ep_r],dtype=torch.float32, device=device)
        for t in count():
            a = dqn.choose_action(s)
            # take action
            s_, r, done = env.step(a.item())
            if done:
                    print('Ep: ', i_episode,
                        '| Ep_r: ', ep_r.item())
                    break
            s_ = torch.tensor(s_, dtype=torch.float32, device=device)
            r = torch.tensor([r],dtype=torch.float32, device=device)
            #print(a.item(), r.item())
            dqn.memeory.push(s, a, s_, r)
            ep_r += r
            #env.plot_durations()
            dqn.learn()
            s = s_
        if env.yestoday_netvalue > INIT_MONEY: #模型赚钱才保存 
            savemodule(dqn.target_net,pmodulepath)
            sucesstime +=1
        print('success time:',sucesstime)

if __name__ == '__main__':
    run()

    #gym_test()
    