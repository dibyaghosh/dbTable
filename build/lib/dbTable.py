import sqlite3 as sql
import numpy   as np
try:
    from tabulate import tabulate
    _tab = True
except:
    print("No Support for Pretty Printing dbTables")
    _tab = False
try:
    from datascience import Table
    _berktb = True
except ImportError:
    print("No support for UCB-Tables")
    _berktb = False
try:
    import pandas as pd
    _df = True
except ImportError:
    _df = False
    print("No support for Pandas DataFrames")

_sqltypes = {"REAL":float,"INTEGER":int,"TEXT":str,"INT":int,'':int}
_pytypes = {float:"REAL",int:"INTEGER",str:"TEXT"}
_numpytypes = {np.int_:"INTEGER",np.float_:"REAL",np.str_:"TEXT",np.bool_:"INTEGER",str:"TEXT"}

##########################################################
#
#       DATABASE -- An Abstract Cover on SQLite Databases
#
###########################################################

class Database:
    """
        Database is the interface in dbTable for connecting to databases.
        To initialize a connection,(replacing file.db with your filename)

            >>> db = Database('file.db') 

        To access tables in the database

            >>> table = db['tablename']

        To create a table from a NUMPY array and column names
        
            >>> db.create_table('newtable',array,cols)
                
                Where newtable is the name of the new table
                Array is a 2D matrix
                cols  is a list of column names

        To drop a table from the database

            >>> db.drop_table(db['tablename']) 

        To save all changes (automatically done when creating and dropping tables)
        
            >> db.commit()

        To close the connection to the database
        
            >>> db.close()

    """
    def __init__(self,db_name):
        """
        Creates a new Database Connection to a file in the current directory

            >>> db = Database('file.db') 
        """
        self.conn = sql.connect(db_name)
        self.c = self.conn.cursor()
        self.tables = self.__get_tables()
        self.name = db_name
    
    def store_table(self,table,name,columns=None):
        """
        Creates a new table in the database from a given data structure.
        Currently supports UC Berkeley DS Tables, Pandas DataFrames, and Numpy Arrays
        
        Parameters: 
            table -- the table to be stored as data   
            name -- name of the new table to be created   
            columns -- A list of column labels (if an ndarray is passed in)  

            >>> a = np.array([[1,2],[3,4]])
            >>> a
            1 2
            3 4
            >>> db.store_table(a,'numbers',['odds','evens'])

        """
        if _berktb and isinstance(table,Table):
            self.__create_table(name,table.rows,table.column_labels)
        if _df and isinstance(table,pd.DataFrame):
            self.__create_table_np(name,table.iterrows(),list(table))
        if isinstance(table,np.ndarray):
            if columns:
                self.__create_table(name,table,columns)
            else:
                raise TypeError("You need to pass in column names")
        self.commit()

    def __create_table(self,name,table,columns):
        
        if len(table[0]) != len(columns):
            print("Column Mismatch")
            return
        cols = []
        for v,c in zip(tuple(table[0]),columns):
            if type(v) in _numpytypes:
                cols.append("%s %s"%(c,_numpytypes[type(v)]))
        expr = "CREATE TABLE %s( %s )"%(name,", ".join(cols))
        self.c.execute(expr)
        insert_expr = "INSERT INTO %s VALUES( %s ) "%(name," ,".join(["?"]*len(cols)))
        for i in table:
            i = np.array(i).tolist()
            self.c.execute(insert_expr,i)
        self[name] = dbTable(self,name,self.__get_table_columns(name))

    def drop_table(self,table):
        """
        Deletes the table given by the table object passed insert_expr

        Parameters:
            table -- a dbTable object belonging to this database

        """
        if table.name not in self.tables:
            return False
        print("DROPPING TABLE ",table.name)
        expr = "DROP TABLE %s"%table.name
        self.c.execute(expr)
        del self.tables[table.name]
        return True


    def commit(self):
        """
        Commits all transactions done to the database: this is automatically done by some functions;
        Note: Commit won't make changes to any tables modified unless you call the tables table.save() function
        """
        self.conn.commit()
    
    def close(self):
        """
        Closes the connection to the database without saving any changes. To save, call commit() first
        """
        self.conn.close()
    
    def __get_tables(self):
        expr = "SELECT * from sqlite_master"
        self.c.execute(expr)
        table_names = [i[1] for i in self.c.fetchall() if i[0]=='table']
        return {name: dbTable(self,name,self.__get_table_columns(name)) for name in table_names}

    def __get_table_columns(self,table_name):
        expr = "PRAGMA table_info(%s)"%table_name
        self.c.execute(expr)
        return [(i[1],_sqltypes[i[2]]) for i in self.c.fetchall()]

    def __repr__(self):
        return "Database: %s Tables: %s" % (self.name, " ,".join(list(self.tables.keys())))

    def __getitem__(self,table):
        """
        Returns a dbTable object corresponding to the table name passed insert_expr

        >>> db = Database('sample.db')
        >>> table1 = db['col1']

        or alternatively

        >>> table1 = db.__getitem__('col1')
        """
        return self.tables[table]
    
    def __setitem__(self,tname,value):
        """
        Sets the label referring to a database to the object pointed to
        This method is used internally, and is prone to change.
        """
        if value.db == self:
            self.tables[tname] = value

    

##########################################################
#
#       DBTABLE -- table interface for Database
#
###########################################################
class dbTable:
    """
    The dbTable class provides a table data structure for the underlying SQL table.
    The full range of SQL statements are available through native Python constructs.

    To get a table object, simply use selection notation from the Database object

        >>> db = Database('test.db')
        >>> table = db['table1']

    To get columns, use selection notation

        >>> col1 = table['col1']

    dbTables can be manipulated in two manners:     
        * Performing Column Operations (check out the Column class)     
        * Performing filtering queries    

    To filter and sort the data, the dbTable api opens the following methods:

    table.select(): returns a new dbTable that has only a certain set of columns    
    table.where() : returns a new dbTable where a certain condition is True    
    table.group() : returns a new dbTable with rows grouped by a certain column
    table.sort()  : returns a new dbTable sorted by a certain column

    When performing data analysis, we can also randomly sample the data using the sample() method

    dbTables can easily be converted into other data structures, including NumPY arrays, Pandas DataFrames, and UCB DataScience Tables
 
         >>> table.to_numpy_array() # Creates a NumPY array
         >>> table.to_df()          # Creates a Pandas DataFrame
         >>> table.to_table()       # Creates a UC Berkeley DataScience Table

    """
    def __init__(self,db,name,columns,options=None):
        """
        Internal use only: do not use

        To get tables, use Database's getitem syntax
        """
        if options:
            self.options = options
        else:
            self.options = {} 
        self.name = name
        self.db   = db
        if type(columns) is dict:
            self.columns = columns
        else:
            self.options['columns'] = [[x[0] for x in columns]]
            self.columns = {c[0]:Column(self,c[0],c[1]) for c in columns}
        self.length = None

    ##################
    #
    #   Saving to DB
    ##################

    def save(self):
        """
            Saves the Database to memory with name - table.name

            Since SQLite doesn't support the removal of columns, if you've
            removed columns, you must use the save_as method to save the table

        """
        for i in self.columns:
            if i not in self.options['columns'][0]:
                print("Added an extra column: ",i)
                expr = "ALTER TABLE %s ADD %s %s" % (self.name,i,_pytypes[self[i].type])
                print(expr)
                self.db.c.execute(expr)
                expr = "UPDATE %s SET %s = %s" % (self.name,i,repr(self[i]))
                print(expr)
                self.db.c.execute(expr)
        columns = self.db.__get_table_columns(self.name)
        self.options['columns'][0] = [c[0] for c in columns]
        self.columns = {c[0]:Column(self,c[0],c[1]) for c in columns}
        self.db.commit()

    def save_as(self,name):
        """
            Saves a copy of the table to a new table with the given name

            Parameters:    
            name -- The name of the new database

                >>> db = Database('test.db')
                >>> table1 = db['table1']
                >>> table1.save_as('table_2')
                >>> table2 = db['table2'] 
        """

        expr = "CREATE TABLE %s as "%name + self._formulate()
        print(expr)
        self.db.c.execute(expr)
        self.db.commit()
        self.db[name] = dbTable(self.db,name,self.db.__get_table_columns(name))

    ##################
    #
    #   Array Notations
    ##################

    def __getitem__(self,column):
        """
            Returns a Column object referring to the column

                >>> table = db['table']
                >>> col1 = table['col1']
        """
        return self.columns[column]

    def __setitem__(self,column,newcol):
        """
            Makes a new column in the data table, 
            based on the column object passed in

                >>> table = db['table']
                >>> col1 = table['col1']
                >>> col2 = 2*col1
                >>> table['col2'] = col2

        """
        if not isinstance(newcol,Column):
            raise ValueError("Can't Set Column to NonColumn")
        if newcol.table !=self:
            raise ValueError("This Column doesn't belong to this table")
        self.columns[column] = newcol
        newcol.table = self

    def __delitem__ (self,b):
        """
            Deletes a column in the table

            >>> table = db['table']
            >>> del table['col1'] #Deleted col1
         """
        self.columns.__delitem__(b)

    ### SQL GENERATOR
    def _formulate(self, options={}):
        ### LIMIT
        if 'limit' in self.options:
            if 'limit' in options:
                limit = "LIMIT %d" % min(options['limit'],self.options['limit'])
            limit = "LIMIT %d" % self.options['limit']
        elif 'limit' in options:
            limit = "LIMIT %d" % options['limit']
        else:
            limit = ""
        ### WHERE
        if 'where' in self.options:
            if 'where' in options:
                where = "WHERE %s" % repr(self.options['where'] & options['where'])
            where = "WHERE %s" % self.options['where']
        elif 'where' in options:
            where = "WHERE %s" % options['where']
        else:
            where = ""
        ### SELECT COLUMNS
        if 'columns' in options:
            if type(options['columns']) is str:
                cols = options['columns']
            else:
                cols = ",".join([c.colexpr for c in options['columns']])
        else:
            cols = ",".join([c.colexpr for c in self.columns.values()]) 
        ### GROUP
        if 'group' in options:
            group = "GROUP BY %s "%options['group']
            if 'having' in options:
                group += " HAVING %s"%repr(options['having'])
        elif 'group' in self.options:
            group = "GROUP BY %s"%self.options['group']
            if 'having' in self.options:
                group += " HAVING %s"%repr(self.options['having'])
        else:
            group = ""
        ### SORT
        if 'sort' in options:
            sort = "ORDER BY %s %s" % options['sort']
        elif 'sort' in self.options:
            sort = "ORDER BY %s %s" % self.options['sort']
        else:
            sort = ""

        return "SELECT %s FROM %s %s %s %s %s" % (cols,self.name,where,group,sort, limit)
    
    ##################
    #
    #   SQL Notation
    ##################

    def select(self,columns):
        """
        Returns a new dbTable with only the columns specified. Similar to a SELECT statement in SQL

        Parameters:   
        columns: A list of column names to pick 

            >>> table = db['table']
            >>> table2 = table.select(['col1','col2'])

        """
        if not isinstance(columns,list):
            raise TypeError("SELECT expects a list")
        for col in columns:
            if col not in self.columns:
                raise KeyError(col+ " not in table")
        newcols = {col:self.columns[col] for col in columns}
        options = dict(self.options)
        return dbTable(self.db,self.name,newcols,options)

    def where(self,column):
        """
        Returns a new dbTable with the following conditions. Similar to a WHERE statement in SQL

        Use all boolean logic implemented with columns to define conditionals.

        Parameters:    
        column: Some condition on a column

            >>> table = db['table']
            >>> table2 = table.where(table['col1']>20)
            >>> table3 = table.where(table['col1']<20 & table['col1'] > 10)


        """
        if column.table != self:
            raise ValueError("Incorrect Column")
        options = dict(self.options)
        if 'where' in options:
            options['where'] = options['where'] & column
        else:
            options['where'] = column
        return dbTable(self.db,self.name,dict(self.columns),options)

    def sort(self,column,descending=False):
        """
        Returns a sorted version of the table (default ascending order)

        Parameters:    
        column: The table will be sorted based on the value of this column    
        descending: if True, this will be sorted in descending order, e     

            >>> table = db['table']
            >>> table2 = table.sort('col1') #Sorted in ascending order     
            >>> table3 = table.sort('col1',descending=True) # Sorted in descending order

        """
        options = dict(self.options)
        desc = "DESC" if descending else "ASC"
        if isinstance(column,str) and column in self.columns:
            column = self.columns[column]
        if isinstance(column,Column) and column.table == self:
            options['sort'] = (column,desc)
            return dbTable(self.db,self.name,dict(self.columns),options)
        raise ValueError('Incorrect Column')

    def group(self,column,having=None,collect=None):
        """
        Returns a new dbTable, where all similar elements in a column are grouped together

        Parameters:   
        column: The column which to group with     
        having: A condition on the group <Buggy - Use at own risk>     
        collect: An aggregator function for the other columns (Currently supports "SUM", "COUNT","MAX","MIN",and "AVG")

            >>> table = db['table']
            >>> table.group(table['col1'],collect="SUM")

        """
        if isinstance(column,str) and column in self.columns:
            column = self.columns[column]
        if isinstance(column,Column) and column.table == self:
            options = dict(self.options)
            options['group'] = column
            if having and isinstance(having,Column) and having.table==self:
                options['having'] = having
            columns = dict(self.columns)
            if collect:
                for i in columns:
                    columns[i] = columns[i].apply(collect)
            return dbTable(self.db,self.name,columns,options)
        raise ValueError('Incorrect Column')  


    ##################
    #
    #   PRINTING
    ##################
    def __repr__(self):
        return "Table %s from database %s: %d entries \n Columns: %s" % (self.name, self.db.name, len(self), ", ".join(list(self.columns.keys())))

    def __str__(self):
        """
        Prints important table information, and displays the first 10 entries of the table
        """
        st = "Table %s from database %s: Showing %d of %d entries \n" % (self.name, self.db.name,min(10,len(self)), len(self))
        return st+self._print()

    def _print(self,num_rows=10):
        expr = self._formulate({'limit':num_rows})
        data  = self.db.c.execute(expr).fetchall()
        data.insert(0, list(self.columns.keys()))
        if _tab:
            return tabulate(data)
        else:
            return "\n".join(["\t".join(map(str,row)) for row in data])
    ###############################
    #
    #   EXPORTING TO OTHER FORMATS
    ###############################

    def _get_column(self,column):
        if isinstance(column,str) and column in self.columns:
            column = self.columns[column]
        if isinstance(column,Column) and column.table ==self:
            expr = self._formulate({'columns':[column]})
            print(expr)
            return np.array(self.db.c.execute(expr).fetchall())
        raise ValueError('Incorrect Arguments')

    def to_table(self,data=None):
        """
        Loads the table into memory, and converts it into a UCB DataScience Table
        Check out http://data8.org for more information
        """
        if not _berktb:
            print("You don't have the Berkeley DataScience library installed")
            return
        if not data:
            data = self.db.c.execute(self._formulate()).fetchall()
        cols = list(self.columns.keys())
        return Table.from_rows(data,cols)


    def to_df(self,data=None):
        """
        Loads the table into memory, and converts it into a Pandas DataFrame
        """
        if not _df:
            print("You don't have the Pandas library installed")
            return
        if not data:
            data = self.db.c.execute(self._formulate()).fetchall()
        cols = list(self.columns.keys())
        return pd.DataFrame(data=data,columns=cols)
    
    def to_numpy_array(self,data=None):
        """
        Converts the table into a numpy array, and loads it into memory

        Note: Since numpy enforces universal types, you may lose data when converting to arrays
        It is recommended to use to_df() instead.
        """
        if not data:
            data = self.db.c.execute(self._formulate()).fetchall()
        return np.array(data)

    def sample(self,num_rows=100,output=np.array):
        """
        Selects a random sample from the table without replacement

        Parameters:    
        num_rows : The maximum number of rows to return (Default 100)
        output   : If specified, returns the data in that form: Currently supports np.array, datascience.Table, and pandas.DataFrame      
        
            >>> table = db['table']
            >>> sample = table.sample(10) #sampling 10 rows
            >>> sample = table.sample(output=pandas.DataFrame) #Returns a DataFrame
            >>> sample = table.sample(output=np.array) #Returns a numpy array
        """

        if len(self) <= num_rows:
            return self.to_table()
        n = len(self)//num_rows
        expr = "WITH temp as (%s) SELECT * from temp WHERE RANDOM() %% %d = 0 LIMIT %d"%(self._formulate(),n,num_rows)
        if output == Table:
            return self.to_table(data=self.db.c.execute(expr).fetchall())
        elif output == pd.DataFrame:
            return self.to_df(data=self.db.c.execute(expr).fetchall())
        return np.array(self.db.c.execute(expr).fetchall())

    #####USEFUL CLASS METHODS
    def __eq__ (self,other):
        """
        Checks whether two tables are just views into the same table.
        For example, a table and it's sorted version are views into the same
        structure, although they represent different transactions on the database

            >>> table1 = db['table1']
            >>> table1_sorted = table1.sort('col1')
            >>> table1 == table1_sorted
            True
            >>> table2 = db['table2']
            >>> table1 == table2
            False
        """
        if isinstance(other,dbTable):
            return other.name==self.name
        return False
    
    def __len__(self):
        """
        Number of rows in the table
        """
        if not self.length:
            if 'group' in self.options:
                expr = "WITH temp as (%s) SELECT COUNT(*) from temp" %(self._formulate())
            else:
                expr = self._formulate({"columns":"COUNT(*)"})
            self.length = int(self.db.c.execute(expr).fetchall()[0][0])
        return self.length 

##########################################################
#
#       COLUMN -- Columns of a Table
#
###########################################################

class Column:
    """
    Columns represent the columns in a dbTable. You can refer to columns
    in tables using the [] notation, and perform any number of transformations on them.

    To get a column:

        >>> col1 = table['col1']

    When one manipulates columns, just use typical python arithmetic notation
       
        >>> col1 = table['col1']
        >>> col2 = col1*2
        >>> table['col3'] = (col1+col2)/2

    You can only combine columns that have the same type (INTS and INTS) (FLOATS and FLOATS) (STRINGS and STRINGS)

    For making where statements, there's also an easy interface for conditionals:

        >>> table.where(table['col1']==2)
        >>> table.where(table['col1'] > 2 & table['col1'] <= 4)

    Note that the notation for AND and OR statements are with '&' and '|' respectively

    When manipulating columns, you can also use augmented arithmetic assignment

        >>> table['col1'] *= 2
        >>> table['col1'] -= 10
    """

    def __init__(self,table,colexpr,type=int):
        """
        Used Internally: do not call

        To get Column objects, use the getitem method from Table
        """
        self.colexpr = colexpr
        self.table      = table
        self.type    = type


    __add__  = lambda self,x: Column.operate(self,x,"(%s + %s)",True)   
    __radd__ = __add__
    __sub__  = lambda self,x: Column._operate(self,x,"(%s - %s)",True)
    __rsub__ = lambda self,x: Column._operate(x,self,"(%s - %s)",True)
    __mul__  = lambda self,x: Column._operate(self,x,"(%s*%s)"  ,True)
    __rmul__ = __mul__
    __div__  = lambda self,x: Column._operate(self,x,"(%s / %s)", True)
    __rdiv__ = lambda self,x: Column._operate(x,self,"(%s / %s)", True)
    __lt__   = lambda self,x: Column._operate(self,x,"(%s < %s)")
    __le__   = lambda self,x: Column._operate(self,x,"(%s <= %s)")
    __eq__   = lambda self,x: Column._operate(self,x,"(%s = %s)")
    __ne__   = lambda self,x: Column._operate(self,x,"(%s != %s)")
    __ge__   = lambda self,x: Column._operate(self,x,"(%s >= %s)")
    __gt__   = lambda self,x: Column._operate(self,x,"(%s > %s)")
    __and__  = lambda self,x: Column._operate(self,x,"(%s AND %s)",typecheck=False) 
    __or__   = lambda self,x: Column._operate(self,x,"(%s OR %s)",typecheck=False)
    __truediv__ = __div__
    def apply(self,function):
        """
            Returns a column where the function has been applied to the column.
            Currently, this only supports default SQL functions like "SUM", "AVG"
            "MIN", "MAX", and "COUNT". 

                >>> col1 = table['col1']
                >>> sum_col1 = col1.apply('SUM')
                >>> avg_col1 = col1.apply('AVG')

        """
        expr = "%s(%s)"%(function,self.colexpr)
        return Column(self.table,expr,self.type)

    def to_array(self):
        """
            Returns a numpy array representing this column
        """

        return self.table._get_column(self)

    def __repr__(self):
        return self.colexpr

    def _operate(c1,c2,expr,strcheck=False,typecheck=True):
        if isinstance(c1,Column) and isinstance(c2,Column) and (not typecheck or c1.type==c2.type) and c1.table==c2.table:
            expr = expr % (c1.colexpr,c2.colexpr)
            return Column(c1.table,expr,c1.type)
        if isinstance(c1,Column):
            try:
                c1.type(c2)
                if c1.type == str and strcheck:
                    expr = "(%s || %s)"% (c1.colexpr,repr(c1.type(c2)))
                    return Column(c1.table,expr,c1.type)
                expr = expr% (c1.colexpr,repr(c1.type(c2)))
                return Column(c1.table,expr,c1.type)
            except:
                raise ValueError("Can't operate")
        if isinstance(c2,Column):
            try:
                c2.type(c1)
                if c2.type == str and strcheck:
                    expr = "(%s || %s)"% (c1.colexpr,repr(c1.type(c2)))
                    return Column(c1.table,expr,c1.type)
                expr = expr%(repr(c2.type(c1)),c2.colexpr)
                return Column(c2.table,expr,c2.type)
                print("GOT HERE")

            except:
                raise ValueError("Can't operate")
