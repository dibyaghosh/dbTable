#### dbTable 

dbTable is a innovative library built on the Python SQLite Library that allows you to manipulate views on tables in Databases, select data from them, and modify them in many ways. This library abstracts out all SQL queries with native Python syntax, and has built-in lazy computation as well. Furthermore, this library provides an interface to load tables from SQLite into in-memory table structures like Pandas DataFrames or Numpy Array for further analysis.

Check out the full documentation at [DOCUMENTATION](http://dibya.xyz/dbTable/documentation.html)

Quickstart Guide:

1) Installation 

Make sure you have numpy and tabulate installed 

```
pip install numpy
pip install tabulate
```

Install dbTable through PyPI (This'll automatically install numpy and tabulate)
```
pip install dbTable
```

You can also install the built distribution in the dist/ folder, or for portable cases, just download the dbTable.py file, and place it in your project folder 


2) Load dbTable into your interpreter/project

```python
import dbTable
```

3) Make a connection to a database

```python
db = Database('test.db') # Replace with your database or use ":memory:" to load an in-memory database
```

4) Select a table:

```python
table1 = db['table1'] 
````

5) Exporting / Importing

To convert a dbTable to any other form, use:

```python
new_df = db['table1'].to_df() # Returns a Pandas DataFrame Object
new_table = db['table1'].to_table() # Returns a UCB Datascience Table 
new_ndarray = db['table1'].to_numpy_array() # Returns a Numpy Array
```

To save a DataFrame, UCB Table, or Numpy Array, use:

```python
db.store_table(new_df,'newtablename')
```
Here's a sampling of modifications and filters you can do with dbTable

```python
table2 = table1.select(['col1','col2']) # Selecting columns from database
table3 = table2.where(table2['col1']> 20) # Filters
table4 = table3.sort('col2',descending=False) # Sort data
grouped_table = table4.group(table4['col1'])
table4['col2'] = table4['col1']*10 # Modify columns
table4['col3'] = table4['col1']+table4['col2']
table4['col3'] *= 2
```
Have fun!

Check out the full documentation at [DOCUMENTATION](http://dibya.xyz/dbTable/documentation.html)

