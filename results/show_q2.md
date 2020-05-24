# Query 2

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
df = pd.read_csv("files/second_query.csv", sep=",",names=["C","N_Acc","Average"])
```

## Function to create the plot


```python

def plot_second_query():
	contrib = list(df['C'])
	n_accidents = list(df['N_Acc'])
	average = list(df['Average'])
	fig = plt.figure(figsize=(24,25))
	ax = fig.add_subplot(111)
	fig.subplots_adjust(bottom=0.42,left=0.07,right=0.98)
	plt.xlabel('Contributing Factor')
	ax.set_title("Results of the second query")
	plt.ylabel('N. Accidents')
	plt.xticks(range(len(contrib)), rotation='vertical')
	bar_contrib = ax.bar(contrib,n_accidents)
	for rect, label in zip(bar_contrib, average):
		height = rect.get_height()
		ax.text(rect.get_x() + rect.get_width() / 2, height + 5, label[:-1],
		ha='center', va='bottom')
	
	plt.show();
```


```python
interactive(plot_second_query)
```


![](https://github.com/StevenSalazarM/Apache-Spark-Car-Accidents-in-NY/blob/master/results/second_query.png)

