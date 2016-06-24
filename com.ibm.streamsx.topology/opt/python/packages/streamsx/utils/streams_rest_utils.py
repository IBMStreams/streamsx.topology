import subprocess 
import time
import numpy as np
import matplotlib.pyplot as plt
import requests
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def get_view_obj(_view, rc):
    for domain in rc.get_domains():
        for instance in domain.get_instances():
            for view in instance.get_views():
                if view.name == _view.name:
                    return view
    return None


def multi_graph_every(views, key, time_step):
    colors = 'bgrcmykw'
    _views = []
    for view in views:
        v = get_view_obj(view,view.get_streams_context())
        if v is not None:
            _views.append(v)
    views = _views

    fig, ax = plt.subplots()

    ydata = []
    xdata = []
    ydatas = []
    for view, color in zip(views, colors):
        ax.plot(xdata,
                ydata, linewidth=2, color=color, label='target')

    data_name = view.attributes[0]['name']

    while True:
        time.sleep(time_step)
        count = 0
        for view, color in zip(views, colors):
            itms = view.get_view_items()

            ar = [json.loads(item.data[data_name])[key] for item in itms]

            if (len(ar) == 0):
                ydatas.insert(0, [0])
                continue
            else:
                ydatas.insert(0, ar)
            xdata = [x for x in range(len(ydatas[0]))]
            ax.lines[count].set_ydata(ydatas[0])
            ax.lines[count].set_xdata(xdata)
            ax.set_xlim(count, len(ydatas[0]))
            ax.set_ylim(np.amin(ydatas[0]) - 1.0, np.amax(ydatas[0]) + 1.0)
            count += 1
        fig.canvas.draw()
        ydatas = []

def graph_every(view, key, time_step):
    view = get_view_obj(view, view.get_streams_context())
    if view is None:
        return None
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
            ydata.append(json.loads(item.data[data_name])[key])
        if(len(ydata) == 0):
            continue
        xdata = [x for x in range(len(ydata))]
        ax.lines[0].set_ydata(ydata)
        ax.lines[0].set_xdata(xdata)
        ax.set_xlim(0, len(ydata))
        ax.set_ylim(np.amin(ydata)-1.0, np.amax(ydata)+1.0)
        fig.canvas.draw()
