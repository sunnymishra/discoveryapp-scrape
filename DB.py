#!/usr/bin/python

import psycopg2
from psycopg2 import pool, extras
from contextlib import contextmanager
import traceback

class DB():

    def create_global_connection(self, minconn, maxconn, _pgconnstr):
        global g_connection
        g_connection = psycopg2.pool.SimpleConnectionPool(minconn, maxconn, _pgconnstr)

    def __del__(self):
        global g_connection
        if not g_connection:
            print("Closing all Pooled Connections")
            g_connection.closeall()

    @contextmanager
    def getconn(self):
        global g_connection
        try:
            conn = g_connection.getconn()
            yield conn
        finally:
            g_connection.putconn(conn)

    @contextmanager
    def getcursor(self):
        global g_connection
        conn = g_connection.getconn()
        try:
            yield conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        finally:
            g_connection.putconn(conn)

    def find(self, tableName, params=None):
        r = None
        with self.getcursor() as cursor:
            try:
                #sql="select * from articles where author = %s and publish_date = %s;" , params=["Jon Russell","2017-12-13"]
                whereConditionSql =""
                if params is not None:
                    keys=params.keys()
                    params=list(params.values())
                    whereConditionSql = " where "+ 'and '.join(map(lambda key: str(key)+"=%s ", keys))
                sql="select * from " + tableName + whereConditionSql

                cursor.execute(sql, params)
                r=cursor.fetchone()
            except Exception as e:
                print('Error in db call.')
                traceback.print_exc()
        return r

    def findAll(self, tableName, params=None):
        r = None
        with self.getcursor() as cursor:
            try:
                #sql="select * from blogs where is_active = %s and blog_id = %s;" , params=[True,1]
                whereConditionSql =""
                if params is not None:
                    keys=params.keys()
                    params=list(params.values())
                    whereConditionSql = " where "+ 'and '.join(map(lambda key: str(key)+"=%s ", keys))
                sql="select * from " + tableName + whereConditionSql
                cursor.execute(sql, params)
                # Fetching data records from the cursor
                r=cursor.fetchall()
                # Fetching Column Name from Cursor before contextmanager closes the Cursor
                columnNamesList = [desc[0] for desc in cursor.description]
                # Adding Column names at Zeroeth index of the Data records list
                r.insert(0,columnNamesList)
                
            except Exception as e:
                print('Error in db call.')
                traceback.print_exc()
        return r

    def upsert(self, query, params=None):
        #with self.getcursor() as cursor:
        with self.getconn() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                try:
                    #print(cursor.mogrify(query, params))
                    cursor.execute(query, params)
                    id = cursor.fetchone()[0]
                    conn.commit()
                    print("*** New Id:{}".format(id))
                except Exception as e:
                    print('Error in db call. Rollbacking now.')
                    conn.rollback()
                    traceback.print_exc()
