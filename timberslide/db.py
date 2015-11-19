'''
Created on 02/01/2015

@author: asieira
'''

import psycopg2
from argparse import ArgumentTypeError
from re import compile, IGNORECASE, sub
import logging

# regular expression to validate identifiers such as table and database names
# used by is_valid_id
_identifier = compile('^[a-z][a-z0-9_]*$', IGNORECASE)


'''
Returns a given text value in quoted and escaped format for use as a value in a libpq
connection string.

See http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING
'''


def escape(text):
    retval = '\''
    for c in text:
        if c == '\'':
            retval = retval + '\\\''
        elif c == '\\':
            retval = retval + '\\\\'
        else:
            retval = retval + c
    return retval + '\''


'''
Returns a new database connection string to a PostgreSQL instance using psycopg2.
Assumes 'server' is in hostname[:port] format.
'''


def connection_string(server, user, password, database=None, sslmode=None):
    connstr = 'user={0} password={1}'.format(escape(user), escape(password))

    server = server.split(':')
    connstr = connstr + ' host=' + escape(server[0])
    if len(server) == 2:
        port = int(server[1])
        if port < 1 or port > 65535:
            raise ValueError('invalid server port value')
        connstr = connstr + ' port=' + str(port)
    elif len(server) != 1:
        raise ValueError('server must be in hostname[:port] format')

    if database is not None:
        connstr = connstr + ' dbname=' + escape(database)

    if sslmode is not None:
        connstr = connstr + ' sslmode=' + escape(sslmode)

    return connstr


'''
Returns a new database connection to a PostgreSQL instance using psycopg2.
Assumes 'server' is in hostname[:port] format.
'''


def connect(server, user, password, database=None, sslmode=None):
    return psycopg2.connect(connection_string(server, user, password, database, sslmode))


'''
Function meant to be used as an argparse type that will validate a postgres
identifier such as a table or database name.
'''


def is_valid_id(name):
    if not _identifier.match(name):
        raise ArgumentTypeError('\'{0}\' is not a valid PostgreSQL identifier'.format(name))
    else:
        return name


# query to create table
_create_table_query = '''
    CREATE TABLE IF NOT EXISTS {0}
    (
        agg_count integer,
        agg_first varchar(5),
        agg_last varchar(5),
        net_blocked boolean,
        net_dst_ip inet,
        net_dst_ip_asname text,
        net_dst_ip_asnumber bigint,
        net_dst_ip_bgpPrefix cidr,
        net_dst_ip_datacenter_name text,
        net_dst_ip_datacenter_url text,
        net_dst_ip_mmgeo_areaCode text,
        net_dst_ip_mmgeo_city text,
        net_dst_ip_mmgeo_country varchar(3),
        net_dst_ip_mmgeo_latitude real,
        net_dst_ip_mmgeo_locationId integer,
        net_dst_ip_mmgeo_longitude real,
        net_dst_ip_mmgeo_metroCode text,
        net_dst_ip_mmgeo_postalCode text,
        net_dst_ip_mmgeo_region text,
        net_dst_ip_mmgeo_regionName text,
        net_dst_ip_rdomain text,
        net_dst_ip_rdomain_domain_0 text,
        net_dst_ip_rdomain_domain_1 text,
        net_dst_ip_rdomain_domain_2 text,
        net_dst_ip_torExitNode boolean,
        net_dst_port integer,
        net_l4proto text,
        net_src_ip inet,
        net_src_ip_asname text,
        net_src_ip_asnumber bigint,
        net_src_ip_bgpPrefix cidr,
        net_src_ip_datacenter_name text,
        net_src_ip_datacenter_url text,
        net_src_ip_mmgeo_areaCode text,
        net_src_ip_mmgeo_city text,
        net_src_ip_mmgeo_country varchar(3),
        net_src_ip_mmgeo_latitude real,
        net_src_ip_mmgeo_locationId integer,
        net_src_ip_mmgeo_longitude real,
        net_src_ip_mmgeo_metroCode text,
        net_src_ip_mmgeo_postalCode text,
        net_src_ip_mmgeo_region text,
        net_src_ip_mmgeo_regionName text,
        net_src_ip_rdomain text,
        net_src_ip_rdomain_domain_0 text,
        net_src_ip_rdomain_domain_1 text,
        net_src_ip_rdomain_domain_2 text,
        net_src_ip_torExitNode boolean,
        net_src_port integer,
        yyyymmddhh varchar(10)
    );
'''


'''
Drops a table of the given name, if it exists, using the given connection
'''


def droptable(conn, name):
    conn.cursor().execute("DROP TABLE IF EXISTS " + name + ";")


'''
Creates a table of the given name, optionally using a given query
'''


def createtable(conn, name, query=_create_table_query):
    conn.cursor().execute(query.format(name))


def _column_sub_repl(m):
    return '_' * len(m.group(0))


'''
Loops through a TSV Iterator and writes each entry as a new row in the given table

Inspired by this: http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
'''


def insert(conn, name, tsviter, chunksize=1024):
    cursor = conn.cursor()

    # build query strings based on column names found
    qmain = "INSERT INTO {0} ({1}) VALUES "
    qval = None
    try:
        row = tsviter.next()
        sqlcolnames = [is_valid_id(sub('[^0-9a-zA-Z_]+', _column_sub_repl, s))
                       for s in tsviter.colnames]
        qmain = qmain.format(name, ", ".join(sqlcolnames))
        qval = "(" + ", ".join(["%s"] * len(tsviter.colnames)) + ")"
    except StopIteration:
        logging.error("File is empty!")
        return

    # process 'chunksize' items at a time doing multi-value inserts
    count = 0
    chunk = [row]
    try:
        while True:
            for i in range(chunksize):
                chunk.append(tsviter.next())

            cursor.execute(qmain + ",".join([cursor.mogrify(qval, x) for x in chunk]) + ";")
            count = count + len(chunk)
            chunk = []
    except StopIteration:
        if len(chunk) > 0:
            cursor.execute(qmain + ",".join([cursor.mogrify(qval, x) for x in chunk]))
            count = count + len(chunk)
        cursor.close()
        conn.commit()
        return count
