#!/usr/bin/python3
# -*- coding: utf-8 -*-

__author__ = 'fane'

import asyncio, logging
import aiomysql

def log(sql, args=()):
    logging.info('SQL:%s' % sql)


#我们需要创建一个全局连接池，每个HTTP请求都可以从连接池中直接获取连接库连接。使用连接池的好处是不必频繁的打开和关闭数据库连接，而是能复用就尽量复用
#连接池由全局变量__pool存储，缺省情况下将编码设置为utf8,自动提交事务
async def create_pool(loop, **kw):
    loogging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
            host=kw.get('host', 'localhost'),
            port=kw.get('port', 3306),
            user=kw['user'],
            passwd=kw['password'],
            db=kw['db'],
            charset=kw.get('charset', 'utf8'),
            autocommit=kw.get('autocommit', True),
            maxsize=kw.get('maxsize', 10),
            minsize=kw.get('mindize', 1),
            loop=loop
        )


#要执行SELECT语句，我们用SELECT函数执行，需要传入SQL语句和SQL参数：
async def select(sql, arg, size=None):
    log(sql, args)
    gobal __pool
    async with __pool.get() as conn: #打开pool的方法，或with await __pool as conn
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
    await cur.close()
    logging.info('row return: %s' % len(rs))
    return rs


#要执行INSERT，UPDATE，DELETE语句，可以定义一个通用的execute()函数，因为这3种SQL的执行都需要相同的参数，以及返回一个整形表示影响的行数：
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
    return affected



def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)



#定义field以及field子类
class Field(object):
    
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__,self.column_type, self.name)


#映射varchar的StringField:
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)
        

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, defualt=None):
        super().__init__(name, 'text', False, default)




#元类
#编写metaclass:ModeMetaclass:
class ModeMetaclass(type):
    def __new__(cls, name, bases, attrs): #当前准备创建的类的对象，类的名字，类继承的父类集合，类的方法集合
        
        #排除Model类本身：
        if name='Model':
            return type.__new__(cls, name, bases, attrs)
        #获取table名称:
        tableName = attrs.get('__attrs__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        #获取所有的Field和主键名：
        mappings = dict()   #保存映射关系
        fields = []         #保存除主键外的属性
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:  #找到主键名
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field:%s' % k)
                    primaryKey = k     #此列设为列表的主键
                else:
                    fields.append(k)   #保存除主键外的属性
        if not primaryKey:
            raise StandardError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)  #从类属性中删除Field属性，否则容易造成运行时错误（实例的属性会掩盖类的同名属性）
        
        escaped_fields = list(map(lambda f: '%s' % f, field))  #转换为sql语法

        #创建供Model类使用的属性：
        attrs['__mappings__'] = mappings  #保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey   #主键名属性
        attrs['__fields__'] = fields             #除主键外的属性

        #构造默认的SELECT，INSERT，UPDATE，DELETE语句：
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) value (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mapping.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' %(tableName, primaryKey)
        
        return type.__new__(cls, name ,bases, attrs)



#定义所有ORM映射的基类Model:

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethon
    async def findAll(cls, where=None, args=None, **kw):
        sql = [cls.__select__]
        if where:
            sql.append('where')
            spl.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstace(limit, tuple) and len(limit)==2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value : %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]


    @classmethod
    async def findNumber(clsm selectField, where=None, args=None):
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']


    @classmethod
    async def find(cls, pk):
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % row)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)
