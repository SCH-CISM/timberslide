'''
Created on 02/01/2015

@author: asieira
'''

import psycopg2
from argparse import ArgumentTypeError
from re import compile, IGNORECASE

_identifier = compile('^[a-z][a-z0-9_]*$', IGNORECASE)

'''
Returns a new database connection to a PostgreSQL instance using psycopg2.
Assumes 'server' is in hostname[:port] format.
'''
def connect(server, database, user, password):
    args = {'user': user, 'password': password}
    
    server = server.split(':')
    args['host'] = server[0]
    if len(server) == 2:
        args['port'] = int(server[1])
        if args['port'] < 1 or args['port'] > 65535:
            raise ValueError('invalid server port value')
    elif len(server) != 1:
        raise ValueError('server must be in hostname[:port] format')
    
    if database is not None:
        args['database'] = database
    
    return psycopg2.connect(**args)

def validtable(name):
    if not _identifier.match(name):
        raise ArgumentTypeError('invalid table name \'{}\''.format(name))
    else:
        return name

def droptable(conn, name):
    conn.cursor().execute("DROP TABLE IF EXISTS " + name)

    