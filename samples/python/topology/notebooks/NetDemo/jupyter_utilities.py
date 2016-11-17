import matplotlib.pyplot as plt
import sys
import numpy as np

def get_next_line(stdout):
    try:
        line = stdout.readline().strip().decode("utf=8")
        if line == '':
            return None
        return line
    except:
        sys.stderr.write("Error reading next line")
        raise
        
def graph_from_outstream(out, xvalues, xmin, xmax, ymin, ymax):
    # Plot data
    fig2, ax2 = plt.subplots()
    
    xdata = []
    ydata = []
    
    ax2.plot(xdata,
            ydata, "go", label = 'output')
    
    ax2.set_xlim(xmin, xmax)
    ax2.set_ylim(ymin, ymax)
    
    count = 0
    line = ""
    while line != None:
        line = get_next_line(out)
        xdata.append(xvalues[count])
        ydata.append(float(line))
        ax2.lines[0].set_xdata(xdata)
        ax2.lines[0].set_ydata(ydata)
        fig2.canvas.draw()
        count = count + 1

def read_output(process):
    line = ""
    while line != None:
        line = get_next_line(process)
        print(line)
        
def scatter_plot(xdata, ydata, xmin, xmax, ymin, ymax):
    # Plot target function
    fig0, ax0 = plt.subplots()
    ax0.plot(xdata,
            ydata, "ro", linewidth = 2, color = 'red')
    ax0.set_xlim(xmin, xmax)
    ax0.set_ylim(ymin, ymax)
'''    
def plot_data_append(out, ):  
    fig2, ax2 = plt.subplots()

    xdata = []
    ydata = []

    ax2.plot(xdata,
            ydata, "go", label = 'output')
    
    ax2.set_xlim(20, 100)
    ax2.set_ylim(0, 1)
    
    count = 0
    line = ""
    while line != None:
        line = get_next_line(out) 
        xdata.append(count)
        ydata.append(float(line))
        ax2.lines[0].set_xdata(xdata)
        ax2.lines[0].set_ydata(ydata)
        fig2.canvas.draw()
        count = count + 1
'''