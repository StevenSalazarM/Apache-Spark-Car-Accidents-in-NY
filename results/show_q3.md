# Query 3

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
df = pd.read_csv("files/third_query.csv", sep=",",names=["Borough","Year","Week","Count","Average"])
```

## Function to create the plot


```python
# Make a dropdown to select the Year
borough = widgets.Dropdown(
    options=list(df['Borough'].unique()),
    value='BRONX',
    description='Borough:',
)
# Make a dropdown to select the Year
year = widgets.Dropdown(
    options=list(df['Year'].unique()),
    value=2012,
    description='Year:',
)
def plot_third_query(borough,year):
	df2 = df.copy()
	df2 = df2[(df2.Borough == borough) & (df2.Year == year)]
	week = range(1,54)
	count = list(df2['Count'])
	mycount = [0]*53
	i=0
	for w in list(df2['Week']):
		mycount[w-1] = count[i]
		i = i+1
	average = list(df2['Average'])
	fig = plt.figure(figsize=(24,12))
	ax = fig.add_subplot(111)
	plt.xlabel('Week')
	fig.subplots_adjust(left=0.04,right=0.99)
	ax.set_title("Results of the third query with Borough: "+borough+" and Year: "+str(year))
	plt.ylabel('N. Accidents')
	plt.xticks(week)
	bar_contrib = ax.bar(week,mycount)
	for rect, label in zip(bar_contrib, average):
		height = rect.get_height()
		ax.text(rect.get_x() + rect.get_width() / 2, height + 5, label,
		ha='center', va='bottom')
	
	plt.show();
```


```python
interactive(plot_third_query,borough=borough,year=year)
```


![](https://github.com/StevenSalazarM/Apache-Spark-Car-Accidents-in-NY/blob/master/results/third_query.png)

