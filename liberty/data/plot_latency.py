import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def graph():
    data = np.genfromtxt('out.csv', delimiter=',')
    x = [row[0] for row in data]
    y = [row[1] for row in data]
    fig = plt.figure()
    ax = fig.add_subplot(111, axisbg = 'w')
    ax.plot(x,y,'g',lw=1.3)
    plt.title("latency")
    plt.ylabel("qps")
    plt.xlabel("time")
    plt.show()

graph()
