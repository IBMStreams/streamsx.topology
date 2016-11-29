
#----------
# build the dataset
#----------

import numpy as np, math
import matplotlib.pyplot as plt
from pybrain.datasets import SupervisedDataSet
from pybrain.structure import SigmoidLayer, LinearLayer
from pybrain.tools.shortcuts import buildNetwork
from pybrain.supervised.trainers import BackpropTrainer

def get_nn_xvalues(xvalues, magnification):
    x_range = np.amax(xvalues) - np.amin(xvalues)
    density = len(xvalues)/(x_range)
    new_x_min = np.amin(xvalues) - magnification*x_range/2
    new_x_max = np.amax(xvalues) + magnification*x_range/2
    nn_xvalues = np.linspace(new_x_min, new_x_max, density * (new_x_max - new_x_min))
    return nn_xvalues

def set_range(xvalues, yvalues, magnification, ax):
    x_range = np.amax(xvalues) - np.amin(xvalues)
    y_range = np.amax(yvalues) - np.amin(yvalues)
    
    slope = y_range/x_range
    x_diff = magnification * x_range / 2
    y_diff = x_diff * slope
    
    ax.set_xlim(np.amin(xvalues)-x_diff, np.amax(xvalues)+x_diff)
    ax.set_ylim(np.amin(yvalues)-y_diff, np.amax(yvalues)+y_diff)
    
    
def train_nn(xvalues, yvalues, rate = 0.0001, batch = False, layers = [100, 100, 100], magnification = 0, iterations = 50):   
    ds = SupervisedDataSet(1, 1)
    for x, y in zip(xvalues, yvalues):
        ds.addSample((x,), (y,))
    
    #----------
    # build the network
    #----------
    
    net = buildNetwork(1,
                       *layers,
                       1,
                       bias = True,
                       hiddenclass = SigmoidLayer,
                       outclass = LinearLayer
                       )
    
    #----------
    # train
    #----------
    fig, ax = plt.subplots()
    plt.title("Engine Temp Vs. Probability of Failure")
    plt.ylabel("Probability of Failure")
    plt.xlabel("Engine Temp in Degrees Celcius")
    
    
    trainer = BackpropTrainer(net, ds, learningrate = rate, momentum=0, verbose = False, batchlearning=batch)
    #trainer.trainUntilConvergence(maxEpochs = 100)
    
    nn_xvalues = get_nn_xvalues(xvalues, magnification)
    
    ax.plot(nn_xvalues,
            [ net.activate([x]) for x in nn_xvalues ], linewidth = 2,
            color = 'blue', label = 'NN output')
    
    # target function
    ax.plot(xvalues,
            yvalues, "ro", linewidth = 2, color = 'red')
    
    set_range(xvalues, yvalues, magnification, ax)
    for i in range(iterations):
        trainer.train()
        # neural net approximation
        new_yvalues = [ net.activate([x]) for x in nn_xvalues ]
        ax.lines[0].set_ydata(new_yvalues)
        fig.canvas.draw()
    return net