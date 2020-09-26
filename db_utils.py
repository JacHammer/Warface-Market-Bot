import sqlite3
import psycopg2
import time
import datetime
import logging
import datetime
from functools import singledispatch


def handle_logs(WARNING_LEVEL: int, message: str):
    """ create a database connection to the SQLite database
        specified by db_file
    :param message: message you want to log
    :param WARNING_LEVEL: level of warnings.
    :return: None
    """

    warning_dict = {0: 'Info',
                    1: 'Warning',
                    2: 'Critical',
                    3: 'Fatal'}

    print("LOG {curr_time} {warning_level_string}: {log_message}"
          .format(curr_time=datetime.datetime.now(),
                  warning_level_string=warning_dict[WARNING_LEVEL],
                  log_message=message))


# TODO: hey you why not try functools.singledispatch here
# TODO: dispatch based on which db's connection
def create_connection(db_file: str):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)

    return connection


def create_connection_pg(dbname: str, user: str, password: str, host: str, port: str):
    """

    :param dbname: name of the PostgreSQL database
    :param user: db user
    :param password: password of the user
    :param host: the host where the db is hosted
    :param port: port opened by the database
    :return:
    """
    connection = None
    try:
        connection = psycopg2.connect(dbname=dbname,
                                      user=user,
                                      password=password,
                                      host=host,
                                      port=port)
    except psycopg2.Error as e:
        print(e)
    return connection


def create_db(region):
    conn = sqlite3.connect('./marketplace.db')
    c = conn.cursor()
    items_table_name = "items"
    if region == 'eu':
        items_table_name = "items"
    elif region == 'ru':
        items_table_name += '_ru'
    else:
        items_table_name += str('_' + region)

    c.execute('''CREATE TABLE IF NOT EXISTS {items_table_name} (
                entity_id integer PRIMARY KEY,
                item_id text NOT NULL,
                kind text NOT NULL,
                entity_type text NOT NULL,
                title_original text,
                title_en text,
                title_cn text
                );'''.format(items_table_name=items_table_name))
    conn.commit()
    conn.close()


def create_timeseries_table(connection, new_table_name, main_table_name):
    cur = connection.cursor()

    # SQL injection on @new_table_name
    # TODO: DON'T use format(); SQL Injection
    sql = '''
    CREATE TABLE IF NOT EXISTS {new_table_name} (
    id integer PRIMARY KEY AUTOINCREMENT,
    entity_id integer, 
    entity_timestamp integer, 
    min_price integer,
    entity_count integer,
    FOREIGN KEY (entity_id) REFERENCES {main_table_name}(entity_id)
    );
    '''.format(new_table_name=new_table_name, main_table_name=main_table_name)
    ex = cur.execute(sql)
    result = ex.fetchone()
    connection.commit()

    # create unique index on timeseries key
    index_name = new_table_name + '_id_uindex'

    sql2 = '''create unique index {idx_name} on {new_table_name} (id);''' \
        .format(idx_name=index_name, new_table_name=new_table_name)
    cur.execute(sql2)
    connection.commit()
    connection.close()


@singledispatch
def insert_item_to_table(cursor, item, item_table_name):
    raise NotImplementedError("{} is not a valid database cursor".format(type(cursor)))


@insert_item_to_table.register(psycopg2.extensions.cursor)
def _(cursor, item, item_table_name):
    sql = '''INSERT INTO {table_name}(entity_id, item_id, kind, entity_type) VALUES(%s, %s, %s, %s)''' \
        .format(table_name=item_table_name)
    cursor.execute(sql, item)
    return cursor.lastrowid


@insert_item_to_table.register(sqlite3.Cursor)
def _(cursor, item, item_table_name):
    sql = '''INSERT INTO {table_name}(entity_id, item_id, kind, entity_type) VALUES(?, ?, ?, ?)''' \
        .format(table_name=item_table_name)
    cursor.execute(sql, item)
    return cursor.lastrowid


@singledispatch
def insert_item_timestamp_to_table(cursor, item, timeseries_table_name):
    raise NotImplementedError("{} is not a valid database cursor".format(type(cursor)))


@insert_item_timestamp_to_table.register(psycopg2.extensions.cursor)
def _(cursor, item, timeseries_table_name):
    # TODO: DON'T use format(); SQL Injection
    sql = '''INSERT INTO {timeseries_table_name} (entity_id, entity_timestamp, min_price, entity_count)
             VALUES(%s, %s, %s, %s)
    '''.format(timeseries_table_name=timeseries_table_name)
    cursor.execute(sql, item)
    return cursor.lastrowid


@insert_item_timestamp_to_table.register(sqlite3.Cursor)
def _(cursor, item, timeseries_table_name):
    # TODO: DON'T use format(); SQL Injection
    sql = '''INSERT INTO {timeseries_table_name} (entity_id, entity_timestamp, min_price, entity_count)
             VALUES(?, ?, ?, ?)
    '''.format(timeseries_table_name=timeseries_table_name)
    cursor.execute(sql, item)
    return cursor.lastrowid

