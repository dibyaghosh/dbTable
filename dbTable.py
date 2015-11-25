import sqlite3 as sql
import numpy   as np
from tabulate import tabulate
try:
    from datascience import Table
except ImportError:
    print("No support for UCB-Tables")
try:
    import pandas as pd
except ImportError:
    print("No support for Pandas DataFrames")

sqltypes = {"REAL":float,"INTEGER":int,"TEXT":str,"INT":int,'':int}
pytypes = {float:"REAL",int:"INTEGER",str:"TEXT"}
numpytypes = {np.int_:"INTEGER",np.float_:"REAL",str:"TEXT",np.bool_:"INTEGER"}

##########################################################
#
#       DATABASE -- An Abstract Cover on SQLite Databases
#
###########################################################

class Database:
    def __init__(self,db_name):
        self.conn = sql.connect(db_name)
        self.c = self.conn.cursor()
        self.tables = self.get_tables()
        self.name = db_name

    def get_tables(self):
        expr = "SELECT * from sqlite_master"
        self.c.execute(expr)
        table_names = [i[1] for i in self.c.fetchall() if i[0]=='table']
        return {name: dbTable(self,name,self.get_table_columns(name)) for name in table_names}

    def get_table_columns(self,table_name):
        expr = "PRAGMA table_info(%s)"%table_name
        self.c.execute(expr)
        return [(i[1],sqltypes[i[2]]) for i in self.c.fetchall()]

    def get_table_names(self):
        return [k for k in self.tables]

    def __repr__(self):
        return "Database: %s Tables: %s" % (self.name, " ,".join(list(self.tables.keys())))

    def __getitem__(self,table):
        return self.tables[table]
    
    def __setitem__(self,tname,value):
        if value.db == self:
            self.tables[tname] = value

    def create_table(self,name,table,columns):
        if len(table[0]) != len(columns):
            print("Column Mismatch")
            return
        cols = []
        for v,c in zip(table[0],columns):
            if type(v) in numpytypes:
                cols.append("%s %s"%(c,numpytypes[type(v)]))
        expr = "CREATE TABLE %s( %s )"%(name,", ".join(cols))
        print(expr)
        self.c.execute(expr)
        insert_expr = "INSERT INTO %s VALUES( %s ) "%(name," ,".join(["?"]*len(cols)))
        for i in table:
            self.c.execute(insert_expr,i)
        self[name] = dbTable(self,name,self.get_table_columns(name))

    def drop_table(self,table):
        if table.name not in self.tables:
            return False
        print("DROPPING TABLE ",table.name)
        expr = "DROP TABLE %s"%table.name
        self.c.execute(expr)
        del self.tables[table.name]
        return True


    def commit(self):
        self.conn.commit()
    def close(self):
        self.conn.close()

##########################################################
#
#       DBTABLE -- table interface for Database
#
###########################################################
class dbTable:
    def __init__(self,db,name,columns,options=None):
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
        for i in self.columns:
            if i not in self.options['columns'][0]:
                print("Added an extra column: ",i)
                expr = "ALTER TABLE %s ADD %s %s" % (self.name,i,pytypes[self[i].type])
                print(expr)
                self.db.c.execute(expr)
                expr = "UPDATE %s SET %s = %s" % (self.name,i,repr(self[i]))
                print(expr)
                self.db.c.execute(expr)
        columns = self.db.get_table_columns(self.name)
        self.options['columns'][0] = [c[0] for c in columns]
        self.columns = {c[0]:Column(self,c[0],c[1]) for c in columns}
        self.db.commit()

    def save_as(self,name):
        expr = "CREATE TABLE %s as "%name + self.formulate()
        print(expr)
        self.db.c.execute(expr)
        self.db.commit()
        self.db[name] = dbTable(self.db,name,self.db.get_table_columns(name))

    ##################
    #
    #   Array Notations
    ##################

    def __getitem__(self,column):
        return self.columns[column]

    def __setitem__(self,column,newcol):
        if not isinstance(newcol,Column):
            raise ValueError("Can't Set Column to NonColumn")
        if newcol.table !=self:
            raise ValueError("This Column doesn't belong to this table")
        self.columns[column] = newcol
        newcol.table = self

    def __delitem__ (self,b):
        self.columns.__delitem__(b)

    ### SQL GENERATOR
    def formulate(self, options={}):
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
        if not isinstance(columns,list):
            raise TypeError("SELECT expects a list")
        for col in columns:
            if col not in self.columns:
                raise KeyError(col+ " not in table")
        newcols = {col:self.columns[col] for col in columns}
        options = dict(self.options)
        return dbTable(self.db,self.name,newcols,options)

    def where(self,column):
        if column.table != self:
            raise ValueError("Incorrect Column")
        options = dict(self.options)
        if 'where' in options:
            options['where'] = options['where'] & column
        else:
            options['where'] = column
        return dbTable(self.db,self.name,dict(self.columns),options)

    def sort(self,column,descending=False):
        options = dict(self.options)
        desc = "DESC" if descending else "ASC"
        if isinstance(column,str) and column in self.columns:
            column = self.columns[column]
        if isinstance(column,Column) and column.table == self:
            options['sort'] = (column,desc)
            return dbTable(self.db,self.name,dict(self.columns),options)
        raise ValueError('Incorrect Column')

    def group(self,column,having=None,collect=None):
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
        st = "Table %s from database %s: Showing %d of %d entries \n" % (self.name, self.db.name,min(10,len(self)), len(self))
        return st+self.iprint()

    def iprint(self,num_rows=10):
        expr = self.formulate({'limit':num_rows})
        print(expr)
        data  = self.db.c.execute(expr).fetchall()
        data.insert(0, list(self.columns.keys()))
        return tabulate(data)
    ###############################
    #
    #   EXPORTING TO OTHER FORMATS
    ###############################

    def get_column(self,column):
        if isinstance(column,str) and column in self.columns:
            column = self.columns[column]
        if isinstance(column,Column) and column.table ==self:
            expr = self.formulate({'columns':[column]})
            print(expr)
            return np.array(self.db.c.execute(expr).fetchall())
        raise ValueError('Incorrect Arguments')

    def to_table(self,data=None):
        if not data:
            data = self.db.c.execute(self.formulate()).fetchall()
        cols = list(self.columns.keys())
        return Table.from_rows(data,cols)


    def to_df(self,data=None):
        if not data:
            data = self.db.c.execute(self.formulate()).fetchall()
        cols = list(self.columns.keys())
        return pd.DataFrame(data=data,columns=cols)
    
    def to_numpy_array(self,data=None):
        if not data:
            data = self.db.c.execute(self.formulate()).fetchall()
        return np.array(data)

    def sample(self,num_rows=100,output=None):
        if len(self) <= num_rows:
            return self.to_table()
        n = len(self)//num_rows
        expr = "WITH temp as (%s) SELECT * from temp WHERE RANDOM() %% %d = 0 LIMIT %d"%(self.formulate(),n,num_rows)
        if output == Table:
            return self.to_table(data=self.db.c.execute(expr).fetchall())
        elif output == pd.DataFrame:
            return self.to_df(data=self.db.c.execute(expr).fetchall())
        return np.array(self.db.c.execute(expr).fetchall())

    #####USEFUL CLASS METHODS
    def __eq__ (self,other):
        if isinstance(other,dbTable):
            return other.name==self.name
        return False
    
    def __len__(self):
        if not self.length:
            if 'group' in self.options:
                expr = "WITH temp as (%s) SELECT COUNT(*) from temp" %(self.formulate())
            else:
                expr = self.formulate({"columns":"COUNT(*)"})
            self.length = int(self.db.c.execute(expr).fetchall()[0][0])
        return self.length 

##########################################################
#
#       COLUMN -- Columns of a Table
#
###########################################################

class Column:

    def __init__(self,table,colexpr,type=int):
        self.colexpr = colexpr
        self.table      = table
        self.type    = type


    __add__  = lambda self,x: Column.operate(self,x,"(%s + %s)",True)   
    __radd__ = __add__
    __sub__  = lambda self,x: Column.operate(self,x,"(%s - %s)",True)
    __rsub__ = lambda self,x: Column.operate(x,self,"(%s - %s)",True)
    __mul__  = lambda self,x: Column.operate(self,x,"(%s*%s)"  ,True)
    __rmul__ = __mul__
    __div__  = lambda self,x: Column.operate(self,x,"(%s / %s)", True)
    __rdiv__ = lambda self,x: Column.operate(x,self,"(%s / %s)", True)
    __lt__   = lambda self,x: Column.operate(self,x,"(%s < %s)")
    __le__   = lambda self,x: Column.operate(self,x,"(%s <= %s)")
    __eq__   = lambda self,x: Column.operate(self,x,"(%s = %s)")
    __ne__   = lambda self,x: Column.operate(self,x,"(%s != %s)")
    __ge__   = lambda self,x: Column.operate(self,x,"(%s >= %s)")
    __gt__   = lambda self,x: Column.operate(self,x,"(%s > %s)")
    __and__  = lambda self,x: Column.operate(self,x,"(%s AND %s)") 
    __or__   = lambda self,x: Column.operate(self,x,"(%s OR %s)")
    __truediv__ = __div__
    def apply(self,function):
        expr = "%s(%s)"%(function,self.colexpr)
        return Column(self.table,expr,self.type)

    def to_array(self):
        return self.table.get_column(self)

    def __repr__(self):
        return self.colexpr

    def operate(c1,c2,expr,strcheck=False):
        if isinstance(c1,Column) and isinstance(c2,Column) and c1.type==c2.type and c1.table==c2.table:
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
