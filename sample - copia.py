#!/usr/bin/python
# -*- encoding: utf-8 -*-
# -*- encoding: ascii -*-

# coneccion a Oracle
import cx_Oracle
import pymongo
from pymongo import MongoClient

conn_str='hotel2013/hotel2013@127.0.0.1:1521/XE'
db_conn = cx_Oracle.connect(conn_str)
cursor = db_conn.cursor()
tablas = cursor.execute('SELECT * FROM tab')

conn = MongoClient('localhost', 27017)
db = conn['hotel']

# migracion de Oracle a MongoDB
for name_table in tablas.fetchall():
	tabla = cursor.execute('SELECT * FROM %s' % name_table[0])
	print '*' * 50
	names = [desc[0].lower() for desc in tabla.description]
	types = [desc[1].__name__ for desc in tabla.description]
	# print names
	collection = db[name_table[0].lower()]
	for row in tabla.fetchall():
		document = dict()
		for x in xrange(0,len(names)):
			if name_table[0] == 'PAIS' and x == 1:
				if row[x].__contains__('\xd1'):
					document[names[x]] = row[x].replace( '\xd1' ,'Ñ')
				else:
					document[names[x]] = row[x]
			else:
				document[names[x]] = row[x]
		collection.insert(document)

# renombrar el campo IDPAIS a PAIS
for x in db.cliente.find():
	db.cliente.update({'_id':x['_id']}, {'$rename':{'idpais':'pais'}})

# embeder tabla pais a tabla cliente
for x in db.cliente.find():
	y = db.pais.find_one({'idpais':x['pais']})
	db.cliente.update({'_id':x['_id']}, {'$set': {'pais':y['nombre']}})

		

# print '*' * 50

# s = u"\xd1"
# s1 = 'ma\xd1asd'.__contains__('\xd1')
# if 'ma\xd1asd'.__contains__('\xd1'):
# 	'ma\xd1asd'.replace( '\xd1' ,'ñ')
# print s1

# # coneccion a mongoDB
# import pymongo
# from pymongo import MongoClient
# conn = MongoClient('localhost', 27017)
# db = conn['sample']
# persona = db['persona']
# for x in persona.find():
# 	print x


# for x in tablas.fetchall():
# 	y = {'idcliente':x[0],'idhabitacion':x[1],'fechareserva':x[2]}
# 	print type(y)
# 	db.reserva.insert(y)
# 	# print x[0] +' --- '+ x[1].__name__