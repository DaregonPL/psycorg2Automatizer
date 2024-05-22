
import copy

import psycopg2


class Database():
    def __init__(self, connection: psycopg2.extensions.connection):
        if type(connection) is not psycopg2.extensions.connection:
            raise TypeError(f'In argument connection: {psycopg2.extensions.connection} expected, not {type(connection)}')
        self.con = connection
        print(f'Database initializated')

    def __str__(self):
        tbn = self.get_tables()
        if not tbn:
            return 'empty'
        mxl = max([len(x) for x in tbn]) + 4
        return '\n'.join(['-'+x.ljust(mxl).replace(' ','-')+'\n'+str(Table(self, x)) for x in tbn])

    def __iter__(self):
        return [Table(self, x) for x in self.get_tables()].__iter__()

    def create_table(self, name, tablespace='pg_default', schema='public'):
        command = [f'create table if not exists {schema}.{name}()',
                   f'tablespace {tablespace}']
        cur = self.con.cursor()
        cur.execute('\n'.join(command))
        cur.close()
        self.con.commit()
        return Table(self, f'{schema}.{name}')

    def get_table(self, name, schema='public'):
        return Table(self, f'{schema}.{name}')

    def del_table(self, name, schema='public'):
        command = f'DROP TABLE {schema}.{name}'
        cur = self.con.cursor()
        cur.execute(command)
        cur.close()

    def get_tables(self):
        command = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY table_schema, table_name;"
        cur = self.con.cursor()
        cur.execute(command)
        output = cur.fetchall()
        cur.close()
        return [f'{schema}.{table}' for schema, table in output]


class Table():
    def __init__(self, database: Database, name: str):
        if type(database) is not Database:
            raise TypeError(f'In argument connection: {Database} expected, not {type(database)}')
        self.db = database
        self.name = name

    def __str__(self):
        tb = [tuple(self.get_columns()), *self.get()]
        res = [[str(y) for y in x] for x in tb]
        ml = {}
        for r in res:
            for x in range(len(r)):
                ml[x] = len(r[x]) if ml.get(x, 0) < len(r[x]) else ml[x]
        return '\n'.join(['  '.join([r[x].ljust(ml[x]) for x in range(len(r))]) for r in res])

    def __iter__(self):
        self.iterrow = 0
        self.tablecopy = self.get()
        return self

    def __next__(self):
        if self.iterrow == len(self.tablecopy):
            raise StopIteration
        line = self.tablecopy[self.iterrow]
        self.iterrow += 1
        return line

    def save(self):
        self.db.con.commit()
        print('Table saved')

    def get(self):
        cur = self.db.con.cursor()
        cur.execute(f'SELECT * FROM {self.name}')
        output = cur.fetchall()
        cur.close()
        return output

    def get_columns(self):
        command = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.name.split('.')[1]}' AND table_schema = '{self.name.split('.')[0]}' ORDER BY ordinal_position"
        cur = self.db.con.cursor()
        cur.execute(command)
        output = cur.fetchall()
        cur.close()
        return [x[0] for x in output]

    def add_column(self, name: str, datatype: str, *args: str, **kw):
        deff = ' '.join([f'{arg} {value}' for arg, value in kw.items()]) + ' '.join(args)
        command = [f'ALTER TABLE {self.name}',
                   f'ADD {name} {datatype} {deff}']
        cur = self.db.con.cursor()
        cur.execute('\n'.join(command))
        cur.close()
        self.save()

    def edit_column(self, name: str, *args: str, **kw):
        deff = ' '.join([f'{arg} {value}' for arg, value in kw.items()]) + ' '.join(args)
        command = [f'ALTER TABLE {self.name}', f'ALTER COLUMN {name}', f'SET {deff}']
        cur = self.db.con.cursor()
        cur.execute('\n'.join(command))
        cur.close()
        self.save()

    def del_column(self, name: str):
        cur = self.db.con.cursor()
        cur.execute(f'ALTER TABLE {self.name} DROP COLUMN {name}')
        cur.close()
        self.save()

    def add_row(self, row_values: tuple, columns='all'):
        cols = f"({', '.join(columns)})" if type(columns) is tuple else ""
        command = f'INSERT INTO {self.name} {cols} VALUES {row_values}'
        cur = self.db.con.cursor()
        cur.execute(command)
        cur.close()
        self.save()


def pout(cursor: psycopg2.extensions.cursor):
    try:
        print(cursor.fetchall())
    except Exception as e:
        print(e)


con = psycopg2.connect(database="8V", user="postgres", password="admin", host="localhost", port="5432")
db = Database(con)
for x in db:
    print(x.name)