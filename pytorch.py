import torch
from torch.utils.data import TensorDataset
import numpy as np
from torch import optim
from torch import nn
from torch.utils.data import DataLoader
import torch.nn.functional as F
import torch.nn as nn

import matplotlib.pyplot as plt

class LinearRegression(nn.Module):
    def __init__(self,din,dout):
        super().__init__()
        self.lin = nn.Linear(din, dout)

    def forward(self, xb):
        return self.lin(xb)

class Pytorch(object):
    def __init__(self,pnet='linearRegression',d_in=1,plr=0.01,pepochs = 100):
        self.net = pnet
        self.lr = plr  # learning rate
        self.model = LinearRegression(din=d_in,dout=1)
        self.optim = optim.SGD(self.model.parameters(), lr=self.lr)
        self.loss_func = F.smooth_l1_loss#F.mse_loss#F.smooth_l1_loss#F.cross_entropy
        self.bs = 3500
        self.epochs = pepochs  # how many epochs to train for
        self.episode_durations = []
        self.loss=[]

    def get_model(self):
        return self.model, self.optim

    def loss_batch(self,model, loss_func, xb, yb, opt):
        xb = xb.float()
        yb = yb.float()
        opt.zero_grad()
        loss = loss_func(model(xb), yb)

        loss.backward()
        opt.step()
        #opt.zero_grad()

        return loss.item()

    def poly_desc(self,W, b):
        """Creates a string description of a polynomial."""
        result = 'y = '
        for i, w in enumerate(W):
            result += '{:+.2f} x^{} '.format(w, len(W) - i)
        result += '{:+.2f}'.format(b[0])
        return result

    def plot_durations(self):
        plt.figure(2)
        plt.clf()
        durations_t = torch.tensor(self.episode_durations, dtype=torch.float)
        loss_t = torch.tensor(self.loss, dtype=torch.float)
        plt.title('Training...')
        plt.xlabel('Duration')
        plt.ylabel('loss')
        plt.plot(durations_t.numpy(),loss_t.numpy())

        plt.pause(0.001)  # pause a bit so that plots are updated
        

    def fit_detail(self,epochs, model, loss_func, opt, train_dl):#, valid_dl):
        for epoch in range(epochs):
            model.train()
            for xb, yb in train_dl:
                loss = self.loss_batch(model, loss_func, xb, yb, opt)

            self.episode_durations.append(epoch + 1)
            self.loss.append(loss) 
 
            self.plot_durations()

            '''
            model.eval()
            with torch.no_grad():
                losses, nums = zip(
                    *[loss_batch(model, loss_func, xb, yb) for xb, yb in valid_dl]
                )
            val_loss = np.sum(np.multiply(losses, nums)) / np.sum(nums)

            print(epoch, val_loss)
            '''
            
        print('Loss: {:.6f} after {} batches'.format(loss, epoch))
        print('==> Learned function:\t' + self.poly_desc(model.lin.weight.view(-1), model.lin.bias))
        plt.close()
    def fit(self,df_x,df_y):
        train_dl= self.df_dl(df_x,df_y)
        self.fit_detail(epochs=self.epochs, model=self.model, loss_func=self.loss_func, opt=self.optim, train_dl=train_dl)
      

    def _get_data(self,train_ds, bs):
        return (DataLoader(train_ds, batch_size=bs, shuffle=True))
            #,DataLoader(valid_ds, batch_size=bs * 2),)

    def df_dl(self,df1,df2):
        arrays1=df1.values
        arrays2=df2.values
      
        x_train,y_train= map(torch.tensor, (arrays1,arrays2))

        train_ds = TensorDataset(x_train, y_train)
        train_dl = self._get_data(train_ds,self.bs)
        return train_dl

def savemodule(model,pmodulepath):
    torch.save(model.state_dict(), pmodulepath)

def loadmoule(model,pmodulepath):
    model.load_state_dict(torch.load(pmodulepath),strict=False)
        #self.model.eval()
        #self.model.train()
    
    

if __name__ == '__main__':

    train_dl, valid_dl = get_data(train_ds, valid_ds, bs)
    model, opt = get_model()
    fit(epochs, model, loss_func, opt, train_dl, valid_dl)
    print(model.parameters())