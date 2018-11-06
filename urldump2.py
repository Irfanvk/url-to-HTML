#!/usr/bin/env python
import pika
import pymongo

import urllib3
import uuid

from pymongo import MongoClient

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

client = MongoClient('mongodb://localhost:27017/')

db = client['imdb_']

collection = db['imdb_collection2']

channel.queue_declare(queue='names')

http = urllib3.PoolManager()
def callback(ch, method, properties, body):
    bdy = body.decode()
    bdy = bdy.strip('[]"')
    print(" [x] Received %r" % bdy)
    url = bdy
    new_name = uuid.uuid5(uuid.NAMESPACE_URL,url)
    # response = urllib3.urlopen(url)
    
    r = http.request('GET',url)
    print(r.data)
    # webContent = response.read()
    f = open('/home/user/mysys/jb6/dumps/%s.html' %new_name, 'w')
    f.write(str(r.data))
    f.close
    db.collection.insert({'url':url})

channel.basic_consume(callback,
                      queue='names',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()