dbTable is a innovative library built on the Python SQLite Library that allows you to manipulate views on tables in Databases, select data from them, and modify them in many ways. This library abstracts out all SQL queries with native Python syntax, and has built-in lazy computation as well. Furthermore, this library provides an interface to load tables from SQLite into in-memory table structures like Pandas DataFrames or Numpy Array for further analysis.

Quickstart Guide:

1) Make sure you have numpy and tabulate installed 

```
pip install numpy
pip install tabulate
```

2) Load dbTable into your interpreter/project

```
import dbTable
```

3) Make a connection to a database

```
db = Database('test.db') # Replace with your database or use ":memory:" to load an in-memory database
```

4) Select a table:

```
table1 = db['table1'] 
````

Have fun!

Here's a sampling of modifications and filters you can do with dbTable

```
table2 = table1.select(['col1','col2']) # Selecting columns from database
table3 = table2.where(table2['col1']> 20) # Filters
table4 = table3.sort('col2',descending=False) # Sort data
table4['col2'] = table4['col1']*10 # Modify columns
table4['col3'] = table4['col1']+table4['col2']
table4['col3'] *= 2
