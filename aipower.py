from __future__ import print_function
import torch
import torch.nn as nn 
from torch.autograd import Variable
import torch.optim as optim
import numpy as np
import datetime,time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

LAG = 1
INPUT_SIZE = 1
class Sequence(nn.Module):
    def __init__(self):
        super(Sequence, self).__init__()
        self.input_size = INPUT_SIZE
        self.lstm1 = nn.LSTMCell(1, 51)
        self.lstm3 = nn.LSTMCell(51, 1)

    def forward(self, input, future = 0):
        outputs = []
        h_t = Variable(torch.zeros(input.size(0), 51).double(), requires_grad=False)
        c_t = Variable(torch.zeros(input.size(0), 51).double(), requires_grad=False)

        # h_t2 = Variable(torch.zeros(input.size(0), 60).double(), requires_grad=False)
        # c_t2 = Variable(torch.zeros( input.size(0), 60).double(), requires_grad=False)

        h_t3 = Variable(torch.zeros(input.size(0), 1).double(), requires_grad=False)
        c_t3 = Variable(torch.zeros(input.size(0), 1).double(), requires_grad=False)

        # print( input.size(0) , input.size(1) )

        for i, input_t in enumerate(input.chunk(input.size(1)/self.input_size, dim=1)):
            # print ( i , input_t.data.size)
            h_t, c_t = self.lstm1(input_t, (h_t, c_t))

            # print (c_t.data.size() , h_t2.data.size() , c_t2.data.size() )
            # h_t2, c_t2 = self.lstm2(c_t, (h_t2, c_t2))
            h_t3, c_t3 = self.lstm3(c_t, (h_t3, c_t3))

            outputs += [c_t3]

        for i in range(future):# if we should predict the future
            h_t, c_t = self.lstm1(outputs[-LAG ], (h_t, c_t))
            # h_t2, c_t2 = self.lstm2(c_t, (h_t2, c_t2))
            h_t3, c_t3 = self.lstm3(c_t, (h_t3, c_t3))
            outputs += [c_t3]
        outputs = torch.stack(outputs, 1).squeeze(2)
        return outputs



if __name__ == '__main__':
    # set ramdom seed to 0
    np.random.seed(0)
    torch.manual_seed(0)
    # load data and make training set
    data = torch.load('traindata.pt')
    print (data.shape)
    # raise 'error'
    input = Variable(torch.from_numpy(data[:, :-LAG]), requires_grad=False)
    target = Variable(torch.from_numpy(data[:, LAG:]), requires_grad=False)

    row_n = input.size(0)

    # build the model
    seq = Sequence()
    seq.double()
    criterion = nn.MSELoss()
    # criterion = nn.L1Loss()
    # use LBFGS as optimizer since we can load the whole data to train
    optimizer = optim.LBFGS(seq.parameters())
    #begin to train
    for i in range(15):
        print('STEP: ', i)
        def closure():
            optimizer.zero_grad()
            out = seq(input)
            loss = criterion(out, target)
            print('loss:', loss.data.numpy()[0])
            loss.backward()
            return loss
        optimizer.step(closure)
        # begin to predict
        future = 30/INPUT_SIZE
        pred = seq(input[  row_n-1 : row_n], future = future)
        y = pred.data.numpy()
        # draw the result
        plt.figure(figsize=(30,10))
        plt.title('Predict future values for time sequences\n(Dashlines are predicted values)', fontsize=30)
        plt.xlabel('x', fontsize=20)
        plt.ylabel('y', fontsize=20)
        plt.xticks(fontsize=20)
        plt.yticks(fontsize=20)
        def draw(yi, color):
            print ( yi.shape)
            plt.plot(np.arange(input.size(1)), yi[:input.size(1)/INPUT_SIZE].flatten(), color, linewidth = 2.0)
            plt.plot(np.arange(input.size(1), input.size(1) + future*INPUT_SIZE), yi[ (input.size(1)/INPUT_SIZE):].flatten(), color + '^', linewidth = 2.0)
        draw(y[0], 'r')

        # print ('-' * 10)
        # print (y[0][input.size(1):])



        # draw(y[1], 'g')
        # # draw(y[2], 'b')
        # print (input.size(1))
        # print (len(target))
        # print( len(target[11:12][0]))

        # # print (target.data[:3][0])
        # draw(target.data[:3][0].numpy(), 'b')
        #

        from scipy.ndimage.interpolation import shift


        plt.plot(np.arange(input.size(1)),
                 shift(target.data[row_n-1:row_n][0].numpy()[:input.size(1)],  0) ,
                 'b',
                 linewidth = 2.0)

        plt.plot(np.arange(input.size(1)),
                 shift(target.data[row_n-1:row_n][0].numpy()[:input.size(1)],  1) ,
                 'y',
                 linewidth = 2.0)
        plt.savefig('predict%d.pdf'%i)
        plt.close()

    def build_date(day,cnt):
        ds = []
        d = datetime.datetime.strptime(day,'%Y%m%d')
        for i in range(cnt):
            c = d + datetime.timedelta(days=i)
            ds.append(  c.strftime('%Y%m%d'))
        return ds
    p =  y[0][input.size(1)/INPUT_SIZE:].flatten() * 4905574.
    p =  [ int(i) for i in p ]
    pd.DataFrame({'predict_date': build_date('20160901',30) , 'predict_power_consumption': p }).to_csv('Tianchi_power_predict_table.csv',index=False)
