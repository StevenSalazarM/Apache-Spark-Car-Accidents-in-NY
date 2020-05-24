# Query 1

## Import of libraries


```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
from ipywidgets import widgets, interactive
from io import StringIO

```

## Scan of the results


```python
df = pd.read_csv("files/first_query.csv", sep=",",names=["Year","Week","Count"])
```

## Function to create the plot


```python
# Make a dropdown to select the Year
year = widgets.Dropdown(
    options=list(df['Year'].unique()),
    value=2012,
    description='Year:',
)
def plot_first_query(year):
    df2 = df.copy()
    df2 = df2[df2.Year == year]
    week = range(1,54)
    count = list(df2['Count'])
    mycount = [0]*53
    i=0
    for w in list(df2['Week']):
        mycount[w-1] = count[i]
        i = i+1
    # Plot it (only if there's data to plot)
    if len(df2) > 0:
       	fig = plt.figure(figsize=(18,6))
        ax = fig.add_subplot(111)
        plt.xlabel('Week')
        ax.set_title("Results of the first query for year: "+str(year))
        plt.ylabel('N. Lethal Accidents')
        plt.xticks(week)
        plt.bar(week,mycount)
        plt.show();
    else:
        print("No data to show for current selection")
```


```python
interactive(plot_first_query,year=year)
```


![](https://github.com/StevenSalazarM/Apache-Spark-Car-Accidents-in-NY/blob/master/results/first_query.png)

