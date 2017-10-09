# coding=utf-8
__author__ = 'fengoupeng'

import MySQLdb, traceback
import threading

from logs import LogHandler

log = LogHandler('pymysql')

class PyMySQL(object):

    FETCH_ONE = 0
    FETCH_MANY = 1
    FETCH_ALL = 2

    SELECT_SQL_FORMAT = "SELECT %s FROM `%s` WHERE %s"
    INSERT_SQL_FORMAT = "INSERT INTO `%s` (%s) VALUES (%s)"
    UPDATE_SQL_FORMAT = "UPDATE `%s` SET %s WHERE %s"
    DETELE_SQL_FORMAT = "DELETE FROM `%s` WHERE %s"

    def __init__(self, host, post, database, username, password):
        self.conn = None
        self.host = host
        self.post = post
        self.database = database
        self.username = username
        self.password = password
        self._connect()

    def __del__(self):
        self._close()

    def _connect(self):
        self.conn = MySQLdb.connect(host=self.host, port=self.post, user=self.username, passwd=self.password, db=self.database, charset="utf8")
        if self._isopen():
            self.conn.autocommit(True)
            self.conn.ping(True)
        else:
            raise Exception("connect failed!")

    def _close(self):
        if self._isopen():
            self.conn.close()

    def _isopen(self):
        if self.conn and self.conn.open:
            return True
        else:
            return False

    def reconnect(self):
        self._close()
        self._connect()

    def ping(self):
        try:
            self.conn.ping()
        except:
            self.reconnect()

    def execute(self, sql):
        with threading.Lock():
            if not sql:
                raise Exception("sql is empty")
            elif not self._isopen():
                self.reconnect()
            try:
                return self._execute(sql)
            except Exception, e:
                if str(e).find('Lost connection to MySQL server during query') > -1 or \
                   str(e).find('MySQL server has gone away') > -1:
                    try:
                        self.reconnect()
                        return self._execute(sql)
                    except Exception, e:
                        log.error("ERROR SQL : " + sql)
                        log.error(traceback.format_exc())
                        self.reconnect()
                        return self._execute(sql)
                else:
                    log.error("ERROR SQL : " + sql)
                    log.error(traceback.format_exc())
                    self.reconnect()
                    return self._execute(sql)

    def _execute(self, sql):
        with threading.Lock():
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql)
                return cursor.lastrowid
            except Exception, e:
                raise e
            finally:
                cursor.close()

    def fetch(self, sql, mode=FETCH_ONE, rows=1):
        with threading.Lock():
            if not sql:
                raise Exception("sql is empty")
            elif not self._isopen():
                self.reconnect()
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql)
                if cursor is None:
                    return None
                elif mode == self.FETCH_ONE:
                    return cursor.fetchone()
                elif mode == self.FETCH_MANY:
                    return cursor.fetchmany(rows)
                elif mode == self.FETCH_ALL:
                    return cursor.fetchall()
                else:
                    raise Exception("mode value is error")
            except Exception, e:
                log.error("ERROR SQL : " + sql)
                log.error(traceback.format_exc())
                self.reconnect()
            finally:
                cursor.close()

    def upsert(self, table_name, where, datas, isnone=False):
        if where is None or len(where) == 0:
            sql = self.get_insert_sql(table_name, where, datas, isnone)
        else:
            sql = self.get_select_sql(table_name, "*", where)
            data = self.fetch(sql, mode=self.FETCH_ONE)
            if data is not None:
                sql = self.get_update_sql(table_name, where, datas, isnone)
            else:
                sql = self.get_insert_sql(table_name, where, datas, isnone)
        res = self.execute(sql)
        return res

    def check_table(self, table_name):
        with threading.Lock():
            cursor = None
            try:
                if not self._isopen():
                    self.reconnect()
                cursor = self.conn.cursor()
                cursor.execute("describe %s" % table_name)
                return cursor.fetchall()
            except:
                return None
            finally:
                cursor.close()

    def get_select_sql(self, table_name, fields, where):
        if fields is None:
            fields = "*"
        elif isinstance(fields, list):
            fields = ",".join(fields)
        if where is None:
            where = "1=1"
        elif isinstance(where, list):
            where = ",".join(where)
        elif isinstance(where, dict):
            where = self._change_where_to_str(where)
        if not isinstance(fields, basestring) or not isinstance(table_name, basestring) or not isinstance(where, basestring):
            return None
        return self.SELECT_SQL_FORMAT % (fields, table_name, where)

    def get_insert_sql(self, table_name, fields, values, isnone):
        if isinstance(fields, list):
            fields = ",".join(fields)
        if isinstance(values, list):
            values = ",".join(values)
        if (fields is None or isinstance(fields, dict)) and isinstance(values, dict):
            fields, values = self._change_insert_to_string(fields, values, isnone)
        if not isinstance(table_name, basestring) or not isinstance(fields, basestring) or not isinstance(values, basestring):
            return None
        return self.INSERT_SQL_FORMAT % (table_name, fields, values)

    def get_update_sql(self, table_name, where, values, isnone=False):
        if isinstance(where, list):
            where = ",".join(where)
        elif isinstance(where, dict):
            where = self._change_where_to_str(where)
        if isinstance(values, list):
            values = ",".join(values)
        elif isinstance(values, dict):
            values = self._change_update_to_str(values, isnone)
        if not isinstance(table_name, basestring) or not isinstance(values, basestring) or not isinstance(where, basestring):
            return None
        return self.UPDATE_SQL_FORMAT % (table_name, values, where)

    def get_delete_sql(self, table_name, where):
        if isinstance(where, list):
            where = ",".join(where)
        elif isinstance(where, dict):
            where = self._change_where_to_str(where)
        if not isinstance(table_name, basestring) or not isinstance(where, basestring):
            return None
        return self.DETELE_SQL_FORMAT % (table_name, where)

    def get_create_sql(self, table_name, fields):
        fields_ = []
        pri_fields = []
        unique_fields = []
        if isinstance(fields, dict):
            for key, val in fields.items():
                if val['extra']:
                    field_str = "`%s` %s %s" % (val['field_title'], val['field_type'], val['extra'])
                else:
                    field_str = "`%s` %s" % (val['field_title'], val['field_type'])
                fields_.append(field_str)
                if val['key'] in ['PRI', 'pri']:
                    pri_fields.append("`%s`" % val['field_title'])
                if val['key'] in ['UNIQUE', 'unique']:
                    unique_fields.append("`%s`" % val['field_title'])
        elif isinstance(fields, list):
            for val in fields:
                fields_.append("`%s` %s" % (val['field_title'], val['field_type']))
                if val['key'] in ['PRI', 'pri']:
                    pri_fields.append("`%s`" % val['field_title'])
                if val['key'] in ['UNIQUE', 'unique']:
                    unique_fields.append("`%s`" % val['field_title'])
        else:
            self.logger.error('fields type error!')
            return
        if pri_fields:
            key_field_str = "PRIMARY KEY (%s)" % ",".join(pri_fields)
        if unique_fields:
            key_field_str += ", UNIQUE KEY (%s)" % ",".join(unique_fields)
        fields_.append(key_field_str)
        fields_str = ",".join(fields_)
        create_sql = "CREATE TABLE `%s`(%s)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci" % (table_name, fields_str)
        return create_sql

    def _process_mark(self, string):
        if isinstance(string, basestring):
            return string.replace("\"", "'").replace("\\", "\\\\")
        else:
            return string

    def _change_where_to_str(self, where):
        array = []
        for key in where:
            temp = None
            value = where[key]
            if value is None or value == "None":
                temp = "`" + key + "` is NULL"
            elif isinstance(value, basestring):
                temp = "`" + key + "`=\"" + self._process_mark(value) + "\""
            elif isinstance(value, int) or isinstance(value, long) or isinstance(value, float):
                temp = "`" + key + "`=" + str(where[key])
            elif isinstance(value, list):
                temp = "`" + key + "`=\"" + self._change_list_to_str(value) + "\""
            if temp is not None:
                array.append(temp)
        if len(array) > 0:
            return " AND ".join(array)
        else:
            return None

    def _change_update_to_str(self, values, isnone=False):
        array = []
        for key in values:
            temp = None
            value = values[key]
            if value is None or value == "None":
                if isnone:
                    temp = "`" + key + "`=NULL"
                else:
                    continue
            elif isinstance(value, basestring):
                temp = "`" + key + "`=\"" + self._process_mark(value) + "\""
            elif isinstance(value, int) or isinstance(value, long) or isinstance(value, float):
                temp = "`" + key + "`=" + str(value)
            elif isinstance(value, list):
                temp = "`" + key + "`=\"" + self._change_list_to_str(value) + "\""
            if temp is not None:
                array.append(temp)
        if len(array) > 0:
            return ",".join(array)
        else:
            return None

    def _change_insert_to_string(self, where, datas, isnone):
        field_array = []
        value_array = []
        if where is None:
            where = {}
        for dic in [where, datas]:
            for key in dic:
                val = dic[key]
                filed = "`" + key + "`"
                value = None
                if val is None or val == 'None':
                    if isnone:
                        value = "NULL"
                    else:
                        continue
                elif isinstance(val, basestring):
                    value = "\"" + self._process_mark(val) + "\""
                elif isinstance(val, int) or isinstance(val, long) or isinstance(val, float):
                    value = str(val)
                elif isinstance(val, list):
                    value = "\"" + self._change_list_to_str(val) + "\","
                if value is not None:
                    field_array.append(filed)
                    value_array.append(value)
        fields = ",".join(field_array)
        values = ",".join(value_array)
        return fields, values

    def _change_list_to_str(self, values):
        array = []
        for val in values:
            temp = None
            if val is None:
                temp = "NULL"
            elif isinstance(val, basestring):
                temp = "'" + self._process_mark(val).replace("'", "\\'") + "'"
            elif isinstance(val, int) or isinstance(val, long) or isinstance(val, float):
                temp = str(val)
            elif isinstance(val, list):
                temp = self._change_list_to_str(val)
            if temp is not None:
                array.append(temp)
        return "\"[%s]\"" % (",".join(array))
