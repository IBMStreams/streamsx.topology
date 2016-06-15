import subprocess 
import time
import numpy as np
import matplotlib.pyplot as plt
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def graph_every(view, time_step):
    fig, ax = plt.subplots()
    
    ydata = []
    xdata = []
    
    ax.plot(xdata,
            ydata, linewidth = 2, color = 'green', label = 'target')
    
    data_name = view.attributes[0]['name']
    
    while True:
        time.sleep(time_step)
        itms = view.get_view_items()
        
        del ydata[:]
        for item in itms:
            ydata.append(item.data[data_name])
        if(len(ydata) == 0):
            continue
        xdata = [x for x in range(len(ydata))]
        ax.lines[0].set_ydata(ydata)
        ax.lines[0].set_xdata(xdata)
        ax.set_xlim(0, len(ydata))
        ax.set_ylim(np.amin(ydata)-1.0, np.amax(ydata)+1.0)
        fig.canvas.draw()